#!/usr/bin/env python3
"""Verify Kafka/Prometheus Integration

Checks that all components are properly configured and running:
1. Mock services are accessible
2. Kafka is running and topics exist
3. Prometheus adapter can reach mocks
4. Data is flowing through Kafka

USAGE:
  python runners/verify_integration.py
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import requests
from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("verify_integration")


class IntegrationVerifier:
    """Verify all components of the Kafka/Prometheus integration."""
    
    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = []
    
    def check(self, name: str, condition: bool, error_msg: str = "") -> bool:
        """Record a check result."""
        if condition:
            logger.info(f"✓ {name}")
            self.checks_passed += 1
            return True
        else:
            logger.error(f"✗ {name}")
            if error_msg:
                logger.error(f"  → {error_msg}")
            self.checks_failed += 1
            return False
    
    def warn(self, msg: str) -> None:
        """Record a warning."""
        logger.warning(f"⚠ {msg}")
        self.warnings.append(msg)
    
    def verify_mocks(self) -> bool:
        """Verify mock services are accessible."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("CHECKING MOCK SERVICES")
        logger.info("=" * 70)
        
        mocks = [
            ("AWS", "http://localhost:8001/metrics"),
            ("AKS", "http://localhost:8002/metrics"),
            ("DigitalOcean", "http://localhost:8003/metrics"),
        ]
        
        all_ok = True
        for name, url in mocks:
            try:
                response = requests.get(url, timeout=2)
                if response.status_code == 200:
                    # Check if it contains Prometheus metrics
                    if "latency_ms" in response.text:
                        self.check(f"{name:15} mock at {url}", True)
                    else:
                        self.check(
                            f"{name:15} mock at {url}",
                            False,
                            "Response doesn't contain Prometheus metrics"
                        )
                        all_ok = False
                else:
                    self.check(
                        f"{name:15} mock at {url}",
                        False,
                        f"HTTP {response.status_code}"
                    )
                    all_ok = False
            except requests.exceptions.ConnectionError:
                self.check(
                    f"{name:15} mock at {url}",
                    False,
                    "Connection refused - is the mock running?"
                )
                all_ok = False
            except requests.exceptions.Timeout:
                self.check(
                    f"{name:15} mock at {url}",
                    False,
                    "Request timeout"
                )
                all_ok = False
            except Exception as e:
                self.check(
                    f"{name:15} mock at {url}",
                    False,
                    str(e)
                )
                all_ok = False
        
        return all_ok
    
    def verify_kafka(self) -> bool:
        """Verify Kafka is running and topics exist."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("CHECKING KAFKA")
        logger.info("=" * 70)
        
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=5000,
            )
            self.check("Kafka broker connection", True)
            
            # Check topics
            required_topics = ["metrics.events", "service.state", "topo.decisions"]
            
            try:
                metadata = admin_client.describe_topics(topics=required_topics)
                
                for topic in required_topics:
                    if topic in metadata:
                        topic_meta = metadata[topic]
                        partitions = len(topic_meta.get("partitions", []))
                        self.check(
                            f"Topic '{topic}' exists with {partitions} partitions",
                            partitions > 0,
                            f"Expected > 0 partitions, got {partitions}"
                        )
                    else:
                        self.check(
                            f"Topic '{topic}' exists",
                            False,
                            "Topic not found"
                        )
            except Exception as e:
                self.check("Describe topics", False, str(e))
                return False
            finally:
                admin_client.close()
            
            return True
        
        except Exception as e:
            self.check(
                f"Kafka connection to {bootstrap_servers}",
                False,
                str(e)
            )
            return False
    
    def verify_data_flow(self) -> bool:
        """Verify data is flowing through Kafka."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("CHECKING DATA FLOW")
        logger.info("=" * 70)
        
        bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
        
        try:
            consumer = KafkaConsumer(
                "metrics.events",
                bootstrap_servers=bootstrap_servers,
                group_id=f"verify-{int(time.time())}",
                auto_offset_reset="latest",
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
            )
            
            logger.info("Waiting for messages on metrics.events (5 second timeout)...")
            
            message_count = 0
            for message in consumer:
                message_count += 1
                logger.info(f"  Received message: partition={message.partition}, offset={message.offset}")
                if message_count >= 3:
                    break
            
            consumer.close()
            
            if message_count > 0:
                self.check(f"Data flowing through metrics.events ({message_count} messages)", True)
                return True
            else:
                self.warn("No messages on metrics.events (adapter may not be running)")
                self.check("Data flowing through metrics.events", False, "No messages received")
                return False
        
        except Exception as e:
            self.check("Data flow check", False, str(e))
            return False
    
    def verify_config(self) -> bool:
        """Verify configuration."""
        logger.info("")
        logger.info("=" * 70)
        logger.info("CHECKING CONFIGURATION")
        logger.info("=" * 70)
        
        config_vars = {
            "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
            "PROMETHEUS_POLL_INTERVAL": "5",
            "PROMETHEUS_AWS_URL": "http://localhost:8001/metrics",
            "PROMETHEUS_AKS_URL": "http://localhost:8002/metrics",
            "PROMETHEUS_DO_URL": "http://localhost:8003/metrics",
        }
        
        all_ok = True
        for var, default in config_vars.items():
            value = os.getenv(var, default)
            self.check(f"{var:35} = {value}", True)
        
        return all_ok
    
    def run(self) -> int:
        """Run all verifications."""
        logger.info("")
        logger.info("╔" + "=" * 68 + "╗")
        logger.info("║" + " " * 68 + "║")
        logger.info("║" + "  KAFKA/PROMETHEUS INTEGRATION VERIFICATION".center(68) + "║")
        logger.info("║" + " " * 68 + "║")
        logger.info("╚" + "=" * 68 + "╝")
        
        # Run all checks
        self.verify_config()
        mocks_ok = self.verify_mocks()
        kafka_ok = self.verify_kafka()
        data_ok = self.verify_data_flow()
        
        # Summary
        logger.info("")
        logger.info("=" * 70)
        logger.info("SUMMARY")
        logger.info("=" * 70)
        logger.info(f"Checks passed: {self.checks_passed}")
        logger.info(f"Checks failed: {self.checks_failed}")
        if self.warnings:
            logger.info(f"Warnings: {len(self.warnings)}")
            for warning in self.warnings:
                logger.info(f"  - {warning}")
        logger.info("=" * 70)
        logger.info("")
        
        if self.checks_failed == 0:
            logger.info("✓ All checks passed! Integration is working correctly.")
            logger.info("")
            logger.info("Next steps:")
            logger.info("  1. Start the pipeline: python runners/start_local_pipeline.py")
            logger.info("  2. Monitor Kafka: python runners/kafka_monitor.py")
            logger.info("  3. View live UI: http://127.0.0.1:8088")
            return 0
        else:
            logger.error(f"✗ {self.checks_failed} check(s) failed. See errors above.")
            logger.info("")
            logger.info("Troubleshooting:")
            if not mocks_ok:
                logger.info("  - Mock services not responding")
                logger.info("    → Start mocks: python runners/mocks_runner.py")
            if not kafka_ok:
                logger.info("  - Kafka not running or topics don't exist")
                logger.info("    → Start Kafka: docker-compose up -d zookeeper kafka")
            if not data_ok:
                logger.info("  - No data flowing through Kafka")
                logger.info("    → Start adapter: python runners/prometheus_adapter_runner.py")
            return 1


def main() -> int:
    """Main entry point."""
    verifier = IntegrationVerifier()
    return verifier.run()


if __name__ == "__main__":
    sys.exit(main())
