#!/usr/bin/env python3
"""Local Development Pipeline Starter

Orchestrates starting the complete Kafka/Prometheus integration locally:
  1. Mocks (AWS, AKS, DigitalOcean) on localhost:8001-8003
  2. Prometheus adapter polling mocks and publishing to Kafka
  3. Service agents consuming metrics and producing state
  4. Topology agents consuming state and producing decisions

USAGE:
  # Terminal 1: Start the full pipeline
  python runners/start_local_pipeline.py

  # Terminal 2: Monitor Kafka topics
  python -m runners.kafka_monitor

  # Terminal 3: View live pipeline visualization
  open http://127.0.0.1:8088

ENVIRONMENT VARIABLES:
  KAFKA_BOOTSTRAP_SERVERS: Kafka broker (default: localhost:9092)
  PROMETHEUS_POLL_INTERVAL: Polling interval in seconds (default: 5)
  LOG_LEVEL: Logging level (default: INFO)
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from pathlib import Path

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
logger = logging.getLogger("pipeline_starter")

from mocks.aws_simulator import AWSSimulator
from mocks.aks_simulator import AKSSimulator
from mocks.dodroplets_simulator import DigitalOceanSimulator
from kafka_core.prometheus_kafka_adaptar import PrometheusMetricsAdapter
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.topic_initializer import TopicInitializer
from kafka_core.prometheus_adapter_config import ENDPOINTS_CONFIG, POLL_INTERVAL


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
        time.sleep(0.5)  # Stagger startup
    
    # Wait for mocks to be ready
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


def main() -> None:
    """Main entry point."""
    logger.info("")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║" + " " * 68 + "║")
    logger.info("║" + "  LOCAL KAFKA/PROMETHEUS PIPELINE STARTER".center(68) + "║")
    logger.info("║" + " " * 68 + "║")
    logger.info("╚" + "=" * 68 + "╝")
    logger.info("")
    
    try:
        # Start mocks
        mock_threads = start_mocks()
        
        # Initialize Kafka topics
        initialize_kafka_topics()
        
        # Start Prometheus adapter
        adapter_thread = start_prometheus_adapter()
        
        logger.info("")
        logger.info("=" * 70)
        logger.info("PIPELINE RUNNING")
        logger.info("=" * 70)
        logger.info("")
        logger.info("Mock services:")
        logger.info("  AWS:          http://localhost:8001/metrics")
        logger.info("  AKS:          http://localhost:8002/metrics")
        logger.info("  DigitalOcean: http://localhost:8003/metrics")
        logger.info("")
        logger.info("Kafka topics:")
        logger.info("  metrics.events  (3 partitions) - Raw metrics from Prometheus")
        logger.info("  service.state   (3 partitions) - Service agent state")
        logger.info("  topo.decisions  (3 partitions) - Topology decisions")
        logger.info("")
        logger.info("To monitor Kafka topics in another terminal:")
        logger.info("  python -m runners.kafka_monitor")
        logger.info("")
        logger.info("Press Ctrl-C to stop all services")
        logger.info("=" * 70)
        logger.info("")
        
        # Keep main thread alive
        while True:
            # Check if any threads died
            all_threads = mock_threads + [adapter_thread]
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
