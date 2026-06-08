#!/usr/bin/env python3
"""Complete UI System Starter - Full Pipeline + WebSocket + HITL API + Prometheus

Orchestrates starting the complete system with full UI support:
  1. Mocks (AWS, AKS, DigitalOcean) on localhost:8001-8003
  2. Prometheus adapter polling mocks and publishing to Kafka
  3. Service agents processing metrics
  4. Topology agents making decisions
  5. HITL API for approvals (Port 8080)
  6. Prometheus metrics server (Port 9090)
  7. WebSocket server for real-time UI updates (Port 8765)
  8. Pipeline visualizer (Port 8088)

USAGE:
  python runners/start_complete_ui_system.py

Then open:
  http://127.0.0.1:5173 (React UI - requires: cd ui && npm start)
  http://127.0.0.1:8088 (Pipeline Visualizer)
  ws://127.0.0.1:8765 (WebSocket)
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Set environment variables for local development BEFORE importing modules
os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
os.environ.setdefault("PROMETHEUS_POLL_INTERVAL", "5")
os.environ.setdefault("PROMETHEUS_TIMEOUT", "3")
os.environ.setdefault("PROMETHEUS_AWS_URL", "http://localhost:8001/metrics")
os.environ.setdefault("PROMETHEUS_AKS_URL", "http://localhost:8002/metrics")
os.environ.setdefault("PROMETHEUS_DO_URL", "http://localhost:8003/metrics")
os.environ.setdefault("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("complete_ui_system_starter")

from mocks.aws_simulator import AWSSimulator
from mocks.aks_simulator import AKSSimulator
from mocks.dodroplets_simulator import DigitalOceanSimulator
from kafka_core.config import KafkaConfig
from kafka_core.topic_initializer import TopicInitializer
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.pipeline import iter_services
from agents.aks.topo import TopographyAgent as AKSTopo
from agents.aws.topo import TopographyAgent as AWSTopo
from agents.do.topo import TopographyAgent as DOTopo
from agents.service_agent import ServiceAgent
from agents.governance_agent.governance_agent import GovernanceAgent
from consumers.nginx_weight_consumer import NGINXWeightConsumer

try:
    from launcher.websocket_server import WebSocketServer
    import asyncio
    import websockets
    WEBSOCKET_AVAILABLE = True
except ImportError:
    WEBSOCKET_AVAILABLE = False
    logger.warning("WebSocket server not available - UI will not receive real-time updates")


class ManagedThread(threading.Thread):
    def __init__(self, name: str, target, *args):
        super().__init__(name=name, daemon=True)
        self._target = target
        self._args = args
        self.exc: Exception = None

    def run(self) -> None:
        try:
            self._target(*self._args)
        except BaseException as exc:
            self.exc = exc
            logger.exception("%s crashed: %s", self.name, exc)


def start_mocks() -> list[threading.Thread]:
    """Start all three cloud simulators."""
    logger.info("=" * 70)
    logger.info("STARTING MOCK SERVICES")
    logger.info("=" * 70)
    
    simulators = [
        (AWSSimulator, 8001, "AWS"),
        (AKSSimulator, 8002, "AKS"),
        (DigitalOceanSimulator, 8003, "DigitalOcean"),
    ]
    
    threads = []
    for sim_class, port, name in simulators:
        sim = sim_class(service_port=port)
        thread = threading.Thread(
            target=sim.run,
            kwargs={"debug": False},
            name=f"{name}SimulatorThread",
            daemon=True,
        )
        thread.start()
        logger.info(f"✓ Started {name:15} simulator on localhost:{port}")
        threads.append(thread)
        time.sleep(0.5)
    
    logger.info("Waiting for mock services to be ready...")
    time.sleep(3)
    
    return threads


def initialize_kafka_topics() -> None:
    """Initialize Kafka topics."""
    logger.info("=" * 70)
    logger.info("INITIALIZING KAFKA TOPICS")
    logger.info("=" * 70)
    
    try:
        initializer = TopicInitializer()
        results = initializer.create_all_topics()
        initializer.close()
        
        if all(results.values()):
            logger.info("✓ Kafka topics initialized successfully")
        else:
            failed = [topic for topic, ok in results.items() if not ok]
            logger.warning(f"Some topics failed to initialize: {failed}")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka topics: {e}")
        raise


def start_prometheus_adapter() -> threading.Thread:
    """Start the Prometheus metrics adapter."""
    logger.info("=" * 70)
    logger.info("STARTING PROMETHEUS ADAPTER")
    logger.info("=" * 70)
    
    from kafka_core.prometheus_kafka_adaptar import PrometheusMetricsAdapter
    from kafka_core.prometheus_adapter_config import ENDPOINTS_CONFIG, POLL_INTERVAL
    
    logger.info(f"Polling interval: {POLL_INTERVAL}s")
    logger.info("Endpoints:")
    for name, config in ENDPOINTS_CONFIG.items():
        logger.info(f"  {name:10} → {config['url']}")
    
    try:
        producer = KafkaProducerTemplate()
        adapter = PrometheusMetricsAdapter(
            producer=producer,
            endpoints=ENDPOINTS_CONFIG,
            poll_interval=POLL_INTERVAL,
        )
        
        thread = threading.Thread(
            target=adapter.start,
            name="PrometheusAdapterThread",
            daemon=True,
        )
        thread.start()
        logger.info("✓ Prometheus adapter started")
        
        return thread
    except Exception as e:
        logger.error(f"Failed to start Prometheus adapter: {e}")
        raise


def start_service_agents(producer: KafkaProducerTemplate, run_id: str) -> list[threading.Thread]:
    """Start service agents for each cloud provider."""
    logger.info("=" * 70)
    logger.info("STARTING SERVICE AGENTS")
    logger.info("=" * 70)
    
    threads = []
    for svc in iter_services():
        agent = ServiceAgent(
            service_id=svc.service_id,
            cloud=svc.cloud.value,
            cloud_producer=producer,
            group_id=f"ui_service_agents_{run_id}",
            partition=svc.partition,
        )
        thread = threading.Thread(
            target=agent.start,
            name=f"ServiceAgent-{svc.name}",
            daemon=True,
        )
        thread.start()
        logger.info(f"✓ Started ServiceAgent for {svc.name}")
        threads.append(thread)
    
    return threads


def start_topology_agents(producer: KafkaProducerTemplate, run_id: str) -> list[threading.Thread]:
    """Start topology agents for each cloud provider."""
    logger.info("=" * 70)
    logger.info("STARTING TOPOLOGY AGENTS")
    logger.info("=" * 70)
    
    topo_classes = {"aws": AWSTopo, "aks": AKSTopo, "do": DOTopo}
    threads = []
    
    for svc in iter_services():
        agent = topo_classes[svc.name](
            service_id=svc.name,
            producer=producer,
            group_id=f"ui_topo_{svc.name}_{run_id}",
            partitions={"service.state": [0, 1, 2]},
        )
        thread = threading.Thread(
            target=agent.start,
            name=f"TopoAgent-{svc.name}",
            daemon=True,
        )
        thread.start()
        logger.info(f"✓ Started TopoAgent for {svc.name}")
        threads.append(thread)
    
    return threads


def start_governance_agent() -> threading.Thread:
    """Start the governance agent."""
    logger.info("=" * 70)
    logger.info("STARTING GOVERNANCE AGENT")
    logger.info("=" * 70)
    
    agent = GovernanceAgent(
        candidate_window_k=3,
        threshold=0.03,
        use_mongo=True,
    )
    thread = threading.Thread(
        target=agent.start,
        name="GovernanceAgentThread",
        daemon=True,
    )
    thread.start()
    logger.info("✓ Started GovernanceAgent")
    
    return thread


def start_execution_agent() -> threading.Thread:
    """Start the execution agent (NGINX Weight Consumer)."""
    logger.info("=" * 70)
    logger.info("STARTING EXECUTION AGENT")
    logger.info("=" * 70)
    
    # Using dummy redis settings for pure kafka mock execution if redis is unavailable
    # The consumer tries to connect to redis by default.
    consumer = NGINXWeightConsumer(
        redis_host="localhost",
        redis_port=6379,
        redis_db=0,
        group_id="ui_nginx_weight_consumer"
    )
    thread = threading.Thread(
        target=consumer.start,
        name="ExecutionAgentThread",
        daemon=True,
    )
    thread.start()
    logger.info("✓ Started ExecutionAgent (NGINXWeightConsumer)")
    
    return thread


def start_websocket_server() -> tuple[threading.Thread, Any]:
    """Start the WebSocket server for real-time updates and Kafka bridge."""
    if not WEBSOCKET_AVAILABLE:
        logger.warning("WebSocket server not available - skipping")
        return None, None
    
    logger.info("=" * 70)
    logger.info("STARTING WEBSOCKET SERVER & KAFKA BRIDGE")
    logger.info("=" * 70)
    
    try:
        ws_server = WebSocketServer(host="0.0.0.0", port=8765)
        
        async def run_websocket():
            # Create a wrapper that handles the new websockets API (v12+)
            async def handler(websocket):
                await ws_server.handle_client(websocket, "/")
            
            async with websockets.serve(handler, "0.0.0.0", 8765):
                logger.info("✓ WebSocket server started on ws://0.0.0.0:8765")
                await asyncio.Future()  # Run forever
        
        def websocket_thread():
            asyncio.run(run_websocket())
        
        thread = threading.Thread(
            target=websocket_thread,
            name="WebSocketServerThread",
            daemon=True,
        )
        thread.start()
        logger.info("✓ WebSocket server thread started")
        
        # Start Kafka to WebSocket Bridge
        from runners.kafka_ws_bridge import KafkaWebSocketBridge
        bridge = KafkaWebSocketBridge(ws_server)
        bridge.start()
        
        return thread, bridge
    except Exception as e:
        logger.error(f"Failed to start WebSocket server: {e}")
        return None, None


def start_hitl_api() -> threading.Thread:
    """Start the HITL API server for approvals."""
    logger.info("=" * 70)
    logger.info("STARTING HITL API SERVER")
    logger.info("=" * 70)
    
    try:
        from hitl.api import create_app
        import uvicorn
        from hitl.approval_engine import ApprovalEngine
        from hitl.api import HITLConsumer
        from kafka_core.producer_base import KafkaProducerTemplate
        
        approval_engine = ApprovalEngine()
        producer = KafkaProducerTemplate()
        hitl_consumer = HITLConsumer(approval_engine, producer)
        
        app = create_app(approval_engine, hitl_consumer)
        
        def run_api():
            # Start consumer in the background before the API server blocks
            import threading
            threading.Thread(target=hitl_consumer.start, daemon=True).start()
            uvicorn.run(app, host="127.0.0.1", port=8080)
        
        thread = threading.Thread(
            target=run_api,
            name="HITLAPIThread",
            daemon=True,
        )
        thread.start()
        logger.info("✓ HITL API server started on http://127.0.0.1:8080")
        
        return thread
    except Exception as e:
        logger.error(f"Failed to start HITL API: {e}")
        return None


def start_prometheus_server() -> threading.Thread:
    """Start the Prometheus metrics server."""
    logger.info("=" * 70)
    logger.info("STARTING PROMETHEUS METRICS SERVER")
    logger.info("=" * 70)
    
    try:
        from prometheus_client import start_http_server
        
        def run_prometheus():
            start_http_server(8000)
            logger.info("✓ Prometheus metrics server started on http://127.0.0.1:8000")
            # Keep running
            import time
            while True:
                time.sleep(1)
        
        thread = threading.Thread(
            target=run_prometheus,
            name="PrometheusServerThread",
            daemon=True,
        )
        thread.start()
        
        return thread
    except Exception as e:
        logger.error(f"Failed to start Prometheus server: {e}")
        return None


def main() -> None:
    """Main entry point."""
    logger.info("")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║" + " " * 68 + "║")
    logger.info("║" + "  COMPLETE UI SYSTEM STARTER (Full Pipeline + WebSocket)".center(68) + "║")
    logger.info("║" + " " * 68 + "║")
    logger.info("╚" + "=" * 68 + "╝")
    logger.info("")
    
    try:
        # Set Kafka config
        servers = ["localhost:9092"]
        KafkaConfig.BOOTSTRAP_SERVERS = servers
        os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ",".join(servers)
        
        # Start mocks
        mock_threads = start_mocks()
        
        # Initialize Kafka topics
        initialize_kafka_topics()
        
        # Start Prometheus adapter
        adapter_thread = start_prometheus_adapter()
        
        # Create producer for agents
        producer = KafkaProducerTemplate()
        run_id = uuid4().hex[:8]
        
        # Start service agents
        service_threads = start_service_agents(producer, run_id)
        
        # Start topology agents
        topo_threads = start_topology_agents(producer, run_id)
        
        # Start governance agent
        gov_thread = start_governance_agent()
        
        # Start execution agent
        exec_thread = start_execution_agent()
        
        # Start WebSocket server
        ws_thread, ws_bridge = start_websocket_server()
        
        # Start HITL API
        hitl_thread = start_hitl_api()
        
        # Start Prometheus metrics server
        prom_thread = start_prometheus_server()
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("COMPLETE UI SYSTEM RUNNING")
        logger.info("=" * 70)
        logger.info("")
        logger.info("Mock services:")
        logger.info("  AWS:          http://localhost:8001/metrics")
        logger.info("  AKS:          http://localhost:8002/metrics")
        logger.info("  DigitalOcean: http://localhost:8003/metrics")
        logger.info("")
        logger.info("Kafka topics:")
        logger.info("  metrics.events   (3 partitions) - Raw metrics from Prometheus")
        logger.info("  service.state    (3 partitions) - Service agent state")
        logger.info("  topo.decisions   (3 partitions) - Topology decisions")
        logger.info("  policy.decisions (3 partitions) - Governance policy decisions")
        logger.info("  policy.approved  (3 partitions) - Approved policy decisions")
        logger.info("")
        logger.info("Backend services:")
        logger.info("  HITL API:       http://localhost:8080 (approvals/pending, stats)")
        logger.info("  Prometheus:     http://localhost:9090 (metrics queries)")
        logger.info("  WebSocket:      ws://localhost:8765 (real-time updates)")
        logger.info("")
        logger.info("Frontend:")
        logger.info("  React UI:       http://localhost:5173 (requires: cd ui && npm start)")
        logger.info("")
        logger.info("Optional:")
        logger.info("  Pipeline Viz:   http://localhost:8088 (run in another terminal)")
        logger.info("                  python -m runners.pipeline_live_runner")
        logger.info("")
        logger.info("Press Ctrl-C to stop all services")
        logger.info("=" * 70)
        logger.info("")
        
        # Keep main thread alive
        while True:
            # Check if any threads died
            all_threads = (
                mock_threads + 
                [adapter_thread, gov_thread, exec_thread] + 
                service_threads + 
                topo_threads
            )
            if ws_thread:
                all_threads.append(ws_thread)
            if hitl_thread:
                all_threads.append(hitl_thread)
            if prom_thread:
                all_threads.append(prom_thread)
            
            dead_threads = [t for t in all_threads if not t.is_alive()]
            
            if dead_threads:
                logger.error(f"Thread(s) died: {[t.name for t in dead_threads]}")
                sys.exit(1)
            
            time.sleep(5)
    
    except KeyboardInterrupt:
        logger.info("")
        logger.info("Ctrl-C received — shutting down")
        logger.info("=" * 70)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
