"""
Parallel Pipeline Runner
========================
Runs all 3 cloud topography agents (aws, aks, do) in parallel.

Each cloud:
  1. Simulator sends ONE normalized load event
  2. TopoAgent processes it → computes redistribution actions
  3. Output is collected and printed

No Kafka required — uses direct in-process message passing via queues.
Each cloud folder's topo.py constants (alpha, beta, gamma, temperature) are
respected by instantiating each agent from its own module.

Folder structure expected:
  agents/aws/topo.py
  agents/aks/topo.py
  agents/do/topo.py
  mocks/normalized_load_simulator.py
"""

import sys
import time
import queue
import logging
import importlib.util
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass, field

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
)
logger = logging.getLogger("pipeline_runner")


# ---------------------------------------------------------------------------
# Lightweight stubs — replace kafka_core imports so topo.py loads without Kafka
# ---------------------------------------------------------------------------

class _StubConsumerTemplate:
    """Stub for KafkaConsumerTemplate — holds the message queue."""
    def __init__(self, topics=None, group_id=None):
        self.topics = topics or []
        self.group_id = group_id
        self._queue: queue.Queue = queue.Queue()

    def put(self, message: dict):
        self._queue.put(message)

    def poll(self, timeout=1.0) -> Optional[dict]:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def close(self):
        pass

    def start(self):
        pass


@dataclass
class _CloudOutput:
    """Captured output from one cloud's topo agent."""
    cloud: str
    actions: List[Dict] = field(default_factory=list)
    risk_level: str = "LOW"
    global_state: Dict = field(default_factory=dict)
    metadata: Dict = field(default_factory=dict)
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


class _CapturingProducer:
    """Fake Kafka producer — captures published decisions instead of sending."""

    def __init__(self, output: _CloudOutput):
        self._output = output

    def send(self, topic: str, message) -> bool:
        try:
            import json
            value = message.value if hasattr(message, "value") else message
            # Extract actions from decision JSON
            decision_str = getattr(value, "decision", "{}")
            parsed = json.loads(decision_str)
            self._output.actions = parsed.get("actions", [])
            self._output.risk_level = str(getattr(value, "risk_level", "LOW"))
            self._output.metadata = getattr(value, "metadata", {})
            logger.debug(f"[{self._output.cloud}] Captured {len(self._output.actions)} actions")
        except Exception as e:
            self._output.error = f"Producer capture error: {e}"
        return True

    def close(self):
        pass


# ---------------------------------------------------------------------------
# Dynamic loader — imports topo.py from a specific cloud folder
# ---------------------------------------------------------------------------

def _load_topo_module(cloud: str, base_path: Path):
    """
    Dynamically import agents/<cloud>/topo.py, injecting kafka_core stubs
    so the module loads without a real Kafka broker.
    """
    topo_path = base_path / "agents" / cloud / "topo.py"
    if not topo_path.exists():
        raise FileNotFoundError(f"Missing: {topo_path}")

    # Build a fake kafka_core package in sys.modules so imports resolve
    _inject_kafka_stubs(cloud)

    spec = importlib.util.spec_from_file_location(
        f"agents.{cloud}.topo", str(topo_path)
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _inject_kafka_stubs(cloud: str):
    """
    Patch sys.modules with lightweight stubs so `from kafka_core.xxx import YYY`
    resolves without installing kafka_core.
    """
    import types

    def _make_module(name):
        m = types.ModuleType(name)
        sys.modules[name] = m
        return m

    # kafka_core package
    kc = _make_module("kafka_core")

    # kafka_core.consumer_base
    cb = _make_module("kafka_core.consumer_base")
    cb.KafkaConsumerTemplate = _StubConsumerTemplate
    kc.consumer_base = cb

    # kafka_core.producer_base
    pb = _make_module("kafka_core.producer_base")

    class _StubProducerTemplate:
        def send(self, topic, msg): return True
        def close(self): pass

    pb.KafkaProducerTemplate = _StubProducerTemplate
    kc.producer_base = pb

    # kafka_core.schemas  (minimal dataclass-like stubs)
    sc = _make_module("kafka_core.schemas")

    class _Stub:
        def __init__(self, **kw):
            for k, v in kw.items():
                setattr(self, k, v)

    sc.PolicyDecision = _Stub
    sc.PolicyDecisionValue = _Stub
    sc.MetricsEvent = _Stub
    sc.MetricsEventValue = _Stub
    kc.schemas = sc

    # kafka_core.enums
    en = _make_module("kafka_core.enums")

    class _PolicyStatus:
        PENDING = "PENDING"

    class _RiskLevel:
        LOW = "LOW"
        MEDIUM = "MEDIUM"
        HIGH = "HIGH"

    class _CloudProvider:
        AWS = "aws"
        AZURE = "aks"
        DIGITALOCEAN = "do"

    en.PolicyStatus = _PolicyStatus
    en.RiskLevel = _RiskLevel
    en.CloudProvider = _CloudProvider
    kc.enums = en


# ---------------------------------------------------------------------------
# Simulator — generates one event per cloud (matching normalized_load_simulator.py logic)
# ---------------------------------------------------------------------------

def _load_simulator_module(base_path: Path):
    """Load the shared simulator from mocks/ to reuse its service config."""
    sim_path = base_path / "mocks" / "normalized_load_simulator.py"
    if not sim_path.exists():
        return None

    spec = importlib.util.spec_from_file_location(
        "mocks.normalized_load_simulator", str(sim_path)
    )
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)
        return mod
    except Exception:
        return None


def _build_event(service_id: str, L_i: float, L_opt_i: float, confidence: float) -> dict:
    """Build a MetricsEvent-shaped dict that topo.process_message() expects."""
    return {
        "value": {
            "service": service_id,
            "metrics": {
                "L_i": round(L_i, 3),
                "L_opt_i": round(L_opt_i, 3),
                "confidence": round(confidence, 3),
            }
        }
    }


# ---------------------------------------------------------------------------
# Per-cloud worker
# ---------------------------------------------------------------------------

def _run_cloud_agent(
    cloud: str,
    base_path: Path,
    results: Dict[str, _CloudOutput],
    done_event: threading.Event,
):
    """
    Worker function for one cloud:
      1. Load topo module with stubs
      2. Inject capturing producer
      3. Load simulator config to get realistic L_i / L_opt / confidence values
      4. Send one event per service into the agent
      5. Trigger compute_and_publish_actions once
      6. Store result
    """
    output = _CloudOutput(cloud=cloud)
    results[cloud] = output

    try:
        # ── Load topo module ──────────────────────────────────────────────
        _inject_kafka_stubs(cloud)
        topo_mod = _load_topo_module(cloud, base_path)
        AgentClass = topo_mod.TopographyAgent

        # ── Stub consumer & capturing producer ───────────────────────────
        stub_consumer = _StubConsumerTemplate(
            topics=["metrics.events"], group_id=f"topography-{cloud}"
        )
        capturing_producer = _CapturingProducer(output)

        agent = AgentClass(
            service_id=cloud,
            consumer=stub_consumer,
            producer=capturing_producer,
        )

        # Disable the time-gate so compute fires immediately on first event
        agent.computation_interval = 0.0

        # ── Load simulator config from shared mocks/ ──────────────────────
        services = _default_services(cloud)
        sim_mod = _load_simulator_module(base_path)  # single shared simulator
        if sim_mod and hasattr(sim_mod, "NormalizedLoadSimulator"):
            try:
                sim = sim_mod.NormalizedLoadSimulator.__new__(
                    sim_mod.NormalizedLoadSimulator
                )
                # Manually set the services list without calling __init__
                sim.producer = None
                sim.update_interval = 5.0
                sim.load_variation = 0.1
                sim.confidence_variation = 0.05
                # Re-use services from the class if they're defined at class level,
                # otherwise fall back to defaults
                if hasattr(sim_mod.NormalizedLoadSimulator, "_services"):
                    services = sim_mod.NormalizedLoadSimulator._services
            except Exception:
                pass  # fall back to defaults

        # ── Send one event per service into the agent ─────────────────────
        import random
        for svc in services:
            L_i = svc["baseline_L"] + random.gauss(0, 0.1)
            L_i = max(0.0, min(1.0, L_i))
            conf = svc["confidence"] + random.gauss(0, 0.05)
            conf = max(0.5, min(1.0, conf))

            event = _build_event(svc["id"], L_i, svc["L_opt"], conf)
            agent.process_message("metrics.events", event)

        # ── One explicit compute pass (no loop / no timer) ────────────────
        agent.compute_and_publish_actions()
        output.global_state = dict(agent.global_state)

        logger.info(f"[{cloud}] Done — {len(output.actions)} actions, risk={output.risk_level}")

    except Exception as e:
        output.error = str(e)
        logger.exception(f"[{cloud}] Agent failed: {e}")

    finally:
        done_event.set()


def _default_services(cloud: str) -> List[Dict]:
    """Fallback service configs if the simulator file isn't loadable."""
    defaults = {
        "aws": [
            {"id": "aws", "L_opt": 0.4, "baseline_L": 0.3, "confidence": 0.85},
            {"id": "aks", "L_opt": 0.35, "baseline_L": 0.5, "confidence": 0.78},
            {"id": "do",  "L_opt": 0.25, "baseline_L": 0.2, "confidence": 0.92},
        ],
        "aks": [
            {"id": "aws", "L_opt": 0.4, "baseline_L": 0.3, "confidence": 0.85},
            {"id": "aks", "L_opt": 0.35, "baseline_L": 0.5, "confidence": 0.78},
            {"id": "do",  "L_opt": 0.25, "baseline_L": 0.2, "confidence": 0.92},
        ],
        "do": [
            {"id": "aws", "L_opt": 0.4, "baseline_L": 0.3, "confidence": 0.85},
            {"id": "aks", "L_opt": 0.35, "baseline_L": 0.5, "confidence": 0.78},
            {"id": "do",  "L_opt": 0.25, "baseline_L": 0.2, "confidence": 0.92},
        ],
    }
    return defaults.get(cloud, defaults["aws"])


# ---------------------------------------------------------------------------
# Output formatter
# ---------------------------------------------------------------------------

def _print_results(results: Dict[str, _CloudOutput]):
    """Pretty-print captured outputs for all 3 clouds."""
    divider = "=" * 60
    print(f"\n{divider}")
    print("  PIPELINE RESULTS  —  one iteration per cloud")
    print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(divider)

    for cloud, out in results.items():
        print(f"\n{'─'*60}")
        print(f"  Cloud: {cloud.upper()}")
        print(f"  Timestamp: {out.timestamp}")

        if out.error:
            print(f"  ⚠  ERROR: {out.error}")
            continue

        print(f"\n  Global State observed by {cloud}:")
        for svc, state in out.global_state.items():
            pressure = state["L_opt_i"] - state["L_i"]
            tag = "OVER" if pressure < 0 else "UNDER" if pressure > 0 else "OK"
            print(f"    {svc:6s}  L_i={state['L_i']:.3f}  L_opt={state['L_opt_i']:.3f}"
                  f"  conf={state['confidence']:.3f}  pressure={pressure:+.3f}  [{tag}]")

        print(f"\n  Redistribution Actions ({len(out.actions)}):")
        if out.actions:
            for a in out.actions:
                print(f"    {a['from']:6s} → {a['to']:6s}  intensity={a['intensity']:.3f}")
        else:
            print("    (none — system is balanced)")

        print(f"\n  Risk Level : {out.risk_level}")

        if out.metadata:
            print(f"  Agent Meta : iter={out.metadata.get('iteration', 0)}"
                  f"  state_size={out.metadata.get('global_state_size', 0)}"
                  f"  temp={out.metadata.get('temperature', 1.0):.4f}")

    print(f"\n{divider}\n")


# ---------------------------------------------------------------------------
# Main — run all 3 clouds in parallel threads, collect results
# ---------------------------------------------------------------------------

def run_pipeline(base_path: str = "."):
    """
    Entry point. Runs aws, aks, do topo agents in parallel threads.

    Args:
        base_path: Root directory containing agents/ and mocks/ subfolders.
    """
    base = Path(base_path).resolve()
    clouds = ["aws", "aks", "do"]

    logger.info(f"Starting parallel pipeline for clouds: {clouds}")
    logger.info(f"Base path: {base}")

    results: Dict[str, _CloudOutput] = {}
    threads = []
    done_events = []

    # Launch one thread per cloud
    for cloud in clouds:
        done = threading.Event()
        done_events.append(done)
        t = threading.Thread(
            target=_run_cloud_agent,
            args=(cloud, base, results, done),
            name=f"topo-{cloud}",
            daemon=True,
        )
        threads.append(t)
        t.start()

    # Wait for all threads (max 30s per thread)
    timeout = 30.0
    for t, done in zip(threads, done_events):
        done.wait(timeout=timeout)
        t.join(timeout=1.0)

    # Print consolidated output
    _print_results(results)
    return results


if __name__ == "__main__":
    base_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    iterations = int(sys.argv[2]) if len(sys.argv) > 2 else 5
    
    logger.info(f"Running pipeline {iterations} times...")
    
    for i in range(1, iterations + 1):
        print(f"\n\n{'='*70}")
        print(f"  ITERATION {i}/{iterations}")
        print(f"{'='*70}\n")
        
        run_pipeline(base_dir)
        
        if i < iterations:
            logger.info(f"Waiting 2 seconds before next iteration...")
            time.sleep(2)
    
    logger.info(f"✓ All {iterations} iterations completed")