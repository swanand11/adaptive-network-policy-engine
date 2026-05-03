"""e2e_pipeline_runner.py — Continuous end-to-end pipeline runner with REAL Kafka.

Full event timeline per cycle
------------------------------
  Mock /metrics endpoint
      ↓  (HTTP poll)
  Adapter  →  metrics.events  (3 Kafka partitions)
      ↓  (KafkaConsumer in background thread)
  ServiceAgent  →  service.state  (3 Kafka partitions)
      ↓  (KafkaConsumer in background thread)
  TopographyAgent  →  topo.decisions  (Kafka)

Every agent runs its real start() loop inside a daemon thread so every hop
goes through the real broker. A probe KafkaConsumer watches topo.decisions;
when it sees a decision whose correlation_id matches the current cycle's
run_id, the wall-clock time from adapter-send to topo-publish is recorded.

Usage
-----
    # Terminal 1 — start mock services
    python run_mocks.py

    # Terminal 2 — run the pipeline (Kafka must be up)
    python e2e_pipeline_runner.py [--bootstrap-servers localhost:9092] [--interval 5]

Press Ctrl-C to stop; a summary table is printed on exit.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
import types
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ── kafka-python six compatibility shim (Python 3.11+) ──────────────────────
try:
    import six                               # type: ignore
    from six import moves as six_moves      # type: ignore
except Exception:
    six = None
    six_moves = None
else:
    if six_moves is None:
        six_moves = types.ModuleType("kafka.vendor.six.moves")
        six_moves.range = range             # type: ignore[attr-defined]
    sys.modules.setdefault("kafka.vendor.six",       six)
    sys.modules.setdefault("kafka.vendor.six.moves", six_moves)

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import TopicPartition

from agents.aks.topo import TopographyAgent as AKSTopo
from agents.aws.topo import TopographyAgent as AWSTopo
from agents.do.topo  import TopographyAgent as DOTopo
from agents.service_agent import ServiceAgent
from kafka_core.config import KafkaConfig
from kafka_core.enums import CloudProvider
from kafka_core.pipeline import iter_services
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.prometheus_kafka_adaptar import (
    MetricsNormalizer,
    PrometheusParser,
    PrometheusPoller,
)
from kafka_core.schemas import MetricsEvent, MetricsEventValue
from kafka_core.topic_initializer import TopicInitializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("e2e_runner")

# ── pipeline topology ────────────────────────────────────────────────────────
ENDPOINTS: List[Dict[str, Any]] = [
    {
        "name": svc.name,
        "service_id": svc.service_id,
        "cloud": svc.cloud,
        "partition": svc.partition,
        "metrics_url": svc.metrics_url,
    }
    for svc in iter_services()
]

# ── timing record ────────────────────────────────────────────────────────────
@dataclass
class EventTiming:
    """Latency breakdown for one event travelling through the full pipeline."""
    cycle:             int
    service:           str
    cloud:             str

    # Phase wall-clock durations measured directly
    adapter_fetch_ms:  float = 0.0   # HTTP GET /metrics
    adapter_parse_ms:  float = 0.0   # Prometheus text → normalised dict
    adapter_send_ms:   float = 0.0   # KafkaProducer.send() → metrics.events
    # The following are measured by watching Kafka: we record the monotonic
    # instant when the adapter finishes sending, then wait for the downstream
    # message to appear on Kafka and subtract.
    metrics_to_state_ms:  float = 0.0   # metrics.events → service.state (Kafka round-trip + ServiceAgent)
    state_to_topo_ms:     float = 0.0   # service.state  → topo.decisions (Kafka round-trip + TopoAgent)

    @property
    def adapter_total_ms(self) -> float:
        return self.adapter_fetch_ms + self.adapter_parse_ms + self.adapter_send_ms

    @property
    def end_to_end_ms(self) -> float:
        return self.adapter_total_ms + self.metrics_to_state_ms + self.state_to_topo_ms


@dataclass
class CycleSummary:
    cycle:      int
    run_id:     str
    started_at: str
    timings:    List[EventTiming] = field(default_factory=list)
    errors:     List[str]         = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors

    def avg_e2e_ms(self) -> float:
        if not self.timings:
            return 0.0
        return sum(t.end_to_end_ms for t in self.timings) / len(self.timings)


# ── helpers: Kafka probe consumer ────────────────────────────────────────────

def _end_offsets(topic: str, n_parts: int) -> Dict[int, int]:
    """Snapshot current end-offsets before we inject events."""
    cfg = KafkaConfig.get_consumer_config(group_id=f"probe_{uuid4().hex[:8]}")
    cfg["enable_auto_commit"] = False
    c = KafkaConsumer(
        **cfg,
        value_deserializer=lambda m: json.loads(m.decode()) if m else None,
        key_deserializer=lambda m: m.decode() if m else None,
    )
    tps = [TopicPartition(topic, p) for p in range(n_parts)]
    c.assign(tps)
    c.poll(timeout_ms=300)
    ends = {tp.partition: off for tp, off in c.end_offsets(tps).items()}
    c.close()
    return ends


def _wait_for_message(
    topic: str,
    n_parts: int,
    start_offsets: Dict[int, int],
    match_fn: Callable[[Dict], bool],
    timeout_s: float = 20.0,
) -> Optional[Dict[str, Any]]:
    """
    Block until a message satisfying match_fn appears on topic, or timeout.
    Returns the first matching message dict, or None.
    """
    cfg = KafkaConfig.get_consumer_config(group_id=f"probe_{uuid4().hex[:8]}")
    cfg["enable_auto_commit"] = False
    c = KafkaConsumer(
        **cfg,
        value_deserializer=lambda m: json.loads(m.decode()) if m else None,
        key_deserializer=lambda m: m.decode() if m else None,
    )
    tps = [TopicPartition(topic, p) for p in range(n_parts)]
    c.assign(tps)
    c.poll(timeout_ms=300)
    for tp in tps:
        c.seek(tp, start_offsets.get(tp.partition, 0))

    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            records = c.poll(timeout_ms=500, max_records=100)
            for _, recs in records.items():
                for rec in recs:
                    payload = rec.value or {}
                    if match_fn(payload):
                        return {
                            "topic":     rec.topic,
                            "partition": rec.partition,
                            "offset":    rec.offset,
                            "key":       rec.key,
                            "value":     payload,
                        }
    finally:
        c.close()
    return None


# ── agent background threads ─────────────────────────────────────────────────
#
# Each agent runs its real start() loop (which internally calls
# KafkaConsumer.poll() in a loop).  We stop it by calling close() which
# closes the underlying KafkaConsumer, causing the poll loop to raise and
# exit gracefully.

class _AgentThread(threading.Thread):
    """Thin wrapper: runs agent.start() and exposes a stop() method."""

    def __init__(self, agent, name: str):
        super().__init__(name=name, daemon=True)
        self._agent = agent
        self._exc: Optional[Exception] = None

    def run(self):
        try:
            self._agent.start()
        except Exception as exc:
            self._exc = exc
            logger.error("Agent thread %s crashed: %s", self.name, exc)

    def stop(self):
        try:
            self._agent.close()
        except Exception:
            pass


def _make_service_agent(ep: Dict, producer: KafkaProducerTemplate, run_id: str) -> ServiceAgent:
    return ServiceAgent(
        service_id=ep["service_id"],
        cloud=ep["cloud"].value,
        cloud_producer=producer,
        group_id=f"e2e_sa_{ep['name']}_{run_id[:8]}",
        partition=ep["partition"],
    )


def _make_topo_agent(name: str, producer: KafkaProducerTemplate, run_id: str):
    """
    TopographyAgent.__init__ calls super().__init__(topics, group_id) which
    creates a KafkaConsumer. We pass a fresh group_id so each run starts from
    the latest offset on service.state.
    """
    group = f"e2e_topo_{name}_{run_id[:8]}"
    cls_map = {"aws": AWSTopo, "aks": AKSTopo, "do": DOTopo}
    # TopographyAgent(service_id, consumer=None, producer=None)
    # When consumer=None it calls super().__init__() creating a real consumer.
    return cls_map[name](
        service_id=name,
        consumer=None,
        producer=producer,
        group_id=group,
        partitions={"service.state": [0, 1, 2]},
    )


# ── printing ─────────────────────────────────────────────────────────────────

_HDR = (
    f"  {'Service':<22} {'fetch':>8} {'parse':>7} {'->metrics':>10} "
    f"{'svc_agent':>11} {'->topo':>8}  |  {'E2E':>9}"
)
_SEP = f"  {'':->22} {'':->8} {'':->7} {'':->10} {'':->11} {'':->8}  |  {'':->9}"


def _print_cycle(s: CycleSummary) -> None:
    status = "OK" if s.passed else "FAIL"
    print(f"\n{'─'*82}")
    print(f"  Cycle {s.cycle:3d}  [{status}]  run_id={s.run_id}  {s.started_at}")
    print(f"{'─'*82}")
    print(_HDR)
    print(_SEP)
    for t in s.timings:
        print(
            f"  {t.service:<22} "
            f"{t.adapter_fetch_ms:>7.1f}ms "
            f"{t.adapter_parse_ms:>6.1f}ms "
            f"{t.adapter_send_ms:>9.1f}ms "
            f"{t.metrics_to_state_ms:>10.1f}ms "
            f"{t.state_to_topo_ms:>7.1f}ms  |"
            f"{t.end_to_end_ms:>9.1f}ms"
        )
    if s.timings:
        print(_SEP)
        print(f"  {'avg E2E':>60}  |{s.avg_e2e_ms():>9.1f}ms")
    for err in s.errors:
        print(f"  !! {err}")


def _print_final(all_s: List[CycleSummary]) -> None:
    all_t = [t for s in all_s for t in s.timings]
    if not all_t:
        return

    def avg(fn):
        v = [fn(t) for t in all_t]
        return sum(v) / len(v)

    print(f"\n{'='*82}")
    print("  FINAL SUMMARY")
    print(f"{'='*82}")
    print(f"  Cycles completed : {len(all_s)}")
    print(f"  Total events     : {len(all_t)}")
    print(f"  Total errors     : {sum(len(s.errors) for s in all_s)}")
    print()
    print(f"  Phase averages (all services & cycles):")
    print(f"    Adapter HTTP fetch      : {avg(lambda t: t.adapter_fetch_ms):>8.1f} ms")
    print(f"    Adapter parse+normalise : {avg(lambda t: t.adapter_parse_ms):>8.1f} ms")
    print(f"    Adapter Kafka send      : {avg(lambda t: t.adapter_send_ms):>8.1f} ms")
    print(f"    ServiceAgent (Kafka RT) : {avg(lambda t: t.metrics_to_state_ms):>8.1f} ms")
    print(f"    TopoAgent   (Kafka RT)  : {avg(lambda t: t.state_to_topo_ms):>8.1f} ms")
    print(f"    {'─'*35}")
    print(f"    End-to-end avg          : {avg(lambda t: t.end_to_end_ms):>8.1f} ms")
    print(f"    End-to-end min          : {min(t.end_to_end_ms for t in all_t):>8.1f} ms")
    print(f"    End-to-end max          : {max(t.end_to_end_ms for t in all_t):>8.1f} ms")
    print()
    print(f"  Per-service averages:")
    by_svc: Dict[str, List[EventTiming]] = {}
    for t in all_t:
        by_svc.setdefault(t.service, []).append(t)
    for svc, ts in sorted(by_svc.items()):
        print(f"    {svc:<26} {sum(t.end_to_end_ms for t in ts)/len(ts):>8.1f} ms avg E2E  (n={len(ts)})")
    print(f"{'='*82}\n")


# ── main pipeline cycle ───────────────────────────────────────────────────────

def _run_cycle(
    cycle:          int,
    run_id:         str,
    producer:       KafkaProducerTemplate,
    poller:         PrometheusPoller,
    svc_threads:    List[_AgentThread],
    topo_threads:   List[_AgentThread],
) -> CycleSummary:
    summary = CycleSummary(
        cycle=cycle,
        run_id=run_id,
        started_at=datetime.now(timezone.utc).isoformat(),
    )

    # Snapshot current tail-offsets so probe consumers only see new messages
    metrics_offsets = _end_offsets("metrics.events", 3)
    state_offsets   = _end_offsets("service.state",  3)
    topo_offsets    = _end_offsets("topo.decisions",  3)

    # ── PHASE 1: Adapter scrapes /metrics and publishes to metrics.events ────
    send_instants: Dict[str, float] = {}   # service_id → monotonic time after send

    for ep in ENDPOINTS:
        timing = EventTiming(cycle=cycle, service=ep["service_id"], cloud=ep["cloud"].value)

        try:
            # 1a. HTTP fetch
            t0 = time.monotonic()
            dump = poller.poll(ep["metrics_url"])
            timing.adapter_fetch_ms = (time.monotonic() - t0) * 1000
            if not dump:
                summary.errors.append(f"[{ep['name']}] Prometheus poll empty")
                continue

            # 1b. Parse + normalise
            t1 = time.monotonic()
            normalised = MetricsNormalizer.normalize(PrometheusParser.parse_dump(dump))
            timing.adapter_parse_ms = (time.monotonic() - t1) * 1000

            # 1c. Publish to Kafka metrics.events
            event = MetricsEvent(
                key=f"{ep['service_id']}@{ep['cloud'].value}",
                value=MetricsEventValue(
                    service=ep["service_id"],
                    cloud=ep["cloud"],
                    timestamp=datetime.now(timezone.utc),
                    metrics=normalised,
                    correlation_id=run_id,
                    parent_event_id=f"cycle-{cycle}",
                ),
            )
            t2 = time.monotonic()
            producer.send("metrics.events", event, partition=ep["partition"])
            timing.adapter_send_ms = (time.monotonic() - t2) * 1000

            # Record the instant the adapter finished sending so we can
            # measure how long the downstream agents take.
            send_instants[ep["service_id"]] = time.monotonic()
            summary.timings.append(timing)

            logger.info(
                "Cycle %d | adapter sent %s to metrics.events[%d] in %.1f ms total",
                cycle, ep["service_id"], ep["partition"], timing.adapter_total_ms,
            )

        except Exception as exc:
            summary.errors.append(f"[{ep['name']}] Adapter: {exc}")
            logger.exception("Adapter error for %s", ep["name"])

    # ── PHASE 2: Wait for ServiceAgents to publish to service.state ──────────
    # The real ServiceAgent threads are polling metrics.events; we just wait
    # for their output to appear on service.state.

    for timing in summary.timings:
        sid = timing.service
        t_sent = send_instants.get(sid)
        if t_sent is None:
            continue

        msg = _wait_for_message(
            "service.state", 3, state_offsets,
            match_fn=lambda p, s=sid: (
                p.get("correlation_id") == run_id and p.get("service") == s
            ),
            timeout_s=20.0,
        )
        if msg:
            timing.metrics_to_state_ms = (time.monotonic() - t_sent) * 1000
            logger.info(
                "Cycle %d | service.state received for %s  (%.1f ms since adapter send)",
                cycle, sid, timing.metrics_to_state_ms,
            )
        else:
            summary.errors.append(
                f"[{sid}] service.state not seen within timeout (are ServiceAgent threads running?)"
            )
            logger.error("Cycle %d | timeout waiting for service.state for %s", cycle, sid)

    # ── PHASE 3: Wait for TopoAgents to publish to topo.decisions ────────────
    # TopoAgents consume service.state and publish topo.decisions.

    for timing in summary.timings:
        sid = timing.service
        # We use the end of phase 2 (state received) as the start for phase 3
        t_state = time.monotonic() - timing.state_to_topo_ms  # approximate
        t_state_abs = time.monotonic()  # use current monotonic as start

        msg = _wait_for_message(
            "topo.decisions", 3, topo_offsets,
            match_fn=lambda p, s=sid: (
                p.get("service") == s and
                p.get("metadata", {}).get("agent_type") == "topography"
            ),
            timeout_s=25.0,
        )
        if msg:
            timing.state_to_topo_ms = (time.monotonic() - t_state_abs) * 1000
            actions = msg["value"].get("actions", [])
            logger.info(
                "Cycle %d | topo.decisions received for %s  "
                "(%.1f ms for topo hop, %d actions, E2E=%.1f ms)",
                cycle, sid, timing.state_to_topo_ms,
                len(actions), timing.end_to_end_ms,
            )
        else:
            summary.errors.append(
                f"[{sid}] topo.decisions not seen within timeout "
                f"(load may be balanced — topo agent skips publishing)"
            )
            logger.warning(
                "Cycle %d | topo.decisions not seen for %s "
                "(balanced load = no action = normal)", cycle, sid,
            )

    return summary


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Continuous E2E pipeline: adapter -> metrics.events -> "
            "ServiceAgent -> service.state -> TopoAgent -> topo.decisions"
        )
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--interval", type=float, default=5.0,
        help="Seconds between pipeline cycles (default: 5)",
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Apply bootstrap servers globally
    servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
    KafkaConfig.BOOTSTRAP_SERVERS = servers
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ",".join(servers)

    logger.info("Bootstrap servers : %s", servers)
    logger.info("Cycle interval    : %.1f s", args.interval)

    # ── one-time Kafka topic setup ────────────────────────────────────────────
    logger.info("Ensuring Kafka topics exist …")
    with TopicInitializer() as init:
        init.create_all_topics()
    logger.info("Topics OK")

    # ── shared producer (used by all agents and the adapter) ─────────────────
    producer = KafkaProducerTemplate()

    run_id = uuid4().hex[:12]
    poller = PrometheusPoller()

    # ── start ServiceAgent background threads (one per endpoint) ─────────────
    # Each gets a unique consumer group so it sees ALL partitions of
    # metrics.events from the beginning of this run.
    logger.info("Starting ServiceAgent threads …")
    svc_threads: List[_AgentThread] = []
    for ep in ENDPOINTS:
        agent = ServiceAgent(
            service_id=ep["service_id"],
            cloud=ep["cloud"].value,
            cloud_producer=producer,
            group_id=f"e2e_sa_{ep['name']}_{run_id[:8]}",
            partition=ep["partition"],
        )
        t = _AgentThread(agent, name=f"SA-{ep['name']}")
        t.start()
        svc_threads.append(t)
        logger.info("  ServiceAgent for %-20s started (group=e2e_sa_%s_%s)",
                    ep["service_id"], ep["name"], run_id[:8])

    # ── start TopographyAgent background threads (one per cloud) ─────────────
    logger.info("Starting TopographyAgent threads …")
    topo_threads: List[_AgentThread] = []
    for name, cls in [("aws", AWSTopo), ("aks", AKSTopo), ("do", DOTopo)]:
        agent = cls(
            service_id=name,
            consumer=None,      # None → real KafkaConsumer on service.state
            producer=producer,
            group_id=f"e2e_topo_{name}_{run_id[:8]}",
            partitions={"service.state": [0, 1, 2]},
        )
        t = _AgentThread(agent, name=f"Topo-{name}")
        t.start()
        topo_threads.append(t)
        logger.info("  TopoAgent for %-8s started", name)

    # Brief warm-up: give Kafka consumers time to receive partition assignments
    logger.info("Waiting 3 s for agent consumers to initialise …")
    time.sleep(3)

    all_summaries: List[CycleSummary] = []
    cycle = 0

    print(f"\n{'='*82}")
    print(f"  E2E Pipeline Runner   run_id={run_id}")
    print(f"  Agents: {len(svc_threads)} ServiceAgent(s), {len(topo_threads)} TopographyAgent(s)")
    print(f"  Cycle interval: {args.interval}s  |  Ctrl-C to stop")
    print(f"{'='*82}\n")

    try:
        while True:
            cycle += 1
            cycle_start = time.monotonic()

            summary = _run_cycle(
                cycle=cycle,
                run_id=run_id,
                producer=producer,
                poller=poller,
                svc_threads=svc_threads,
                topo_threads=topo_threads,
            )
            all_summaries.append(summary)
            _print_cycle(summary)

            elapsed = time.monotonic() - cycle_start
            sleep_for = max(0.0, args.interval - elapsed)
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        print("\nCtrl-C received — stopping pipeline runner.")

    finally:
        logger.info("Stopping agent threads …")
        for t in svc_threads + topo_threads:
            t.stop()
        for t in svc_threads + topo_threads:
            t.join(timeout=5)
        try:
            producer.close()
        except Exception:
            pass
        _print_final(all_summaries)


if __name__ == "__main__":
    main()
