#!/usr/bin/env python3
"""Kafka Topic Monitor

Real-time monitoring of Kafka topics to verify data flow through the pipeline.

USAGE:
  python runners/kafka_monitor.py [--bootstrap-servers localhost:9092] [--topics metrics.events,service.state,topo.decisions]

EXAMPLES:
  # Monitor all pipeline topics
  python runners/kafka_monitor.py

  # Monitor specific topic
  python runners/kafka_monitor.py --topics metrics.events

  # Monitor with custom Kafka broker
  python runners/kafka_monitor.py --bootstrap-servers kafka.example.com:9092
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("kafka_monitor")


class TopicMonitor:
    """Monitor a single Kafka topic."""
    
    def __init__(self, topic: str, bootstrap_servers: list[str]):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.message_count = 0
        self.last_message_time = None
        self.running = False
        self.consumer = None
    
    def start(self) -> None:
        """Start monitoring the topic."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=f"monitor-{self.topic}-{int(time.time())}",
                auto_offset_reset="latest",
                enable_auto_commit=False,
                value_deserializer=lambda m: m.decode("utf-8") if m else None,
                consumer_timeout_ms=1000,
            )
            
            self.running = True
            logger.info(f"Started monitoring topic: {self.topic}")
            
            while self.running:
                try:
                    for message in self.consumer:
                        self.message_count += 1
                        self.last_message_time = datetime.now()
                        
                        # Try to parse as JSON for pretty printing
                        try:
                            value = json.loads(message.value) if message.value else {}
                            value_str = json.dumps(value, indent=2)[:200]
                        except (json.JSONDecodeError, TypeError):
                            value_str = str(message.value)[:200]
                        
                        logger.info(
                            f"[{self.topic}] "
                            f"partition={message.partition} "
                            f"offset={message.offset} "
                            f"key={message.key} "
                            f"value={value_str}"
                        )
                except Exception as e:
                    if self.running:
                        logger.error(f"Error consuming from {self.topic}: {e}")
                    time.sleep(1)
        
        except Exception as e:
            logger.error(f"Failed to start monitoring {self.topic}: {e}")
            self.running = False
    
    def stop(self) -> None:
        """Stop monitoring."""
        self.running = False
        if self.consumer:
            self.consumer.close()
    
    def get_status(self) -> dict:
        """Get current monitoring status."""
        return {
            "topic": self.topic,
            "message_count": self.message_count,
            "last_message": self.last_message_time.isoformat() if self.last_message_time else None,
        }


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Monitor Kafka topics for data flow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor all pipeline topics
  python runners/kafka_monitor.py

  # Monitor specific topic
  python runners/kafka_monitor.py --topics metrics.events

  # Monitor with custom Kafka broker
  python runners/kafka_monitor.py --bootstrap-servers kafka.example.com:9092
        """,
    )
    
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--topics",
        default="metrics.events,service.state,topo.decisions",
        help="Comma-separated topics to monitor (default: metrics.events,service.state,topo.decisions)",
    )
    
    args = parser.parse_args()
    
    bootstrap_servers = args.bootstrap_servers.split(",")
    topics = [t.strip() for t in args.topics.split(",")]
    
    logger.info("")
    logger.info("╔" + "=" * 68 + "╗")
    logger.info("║" + " " * 68 + "║")
    logger.info("║" + "  KAFKA TOPIC MONITOR".center(68) + "║")
    logger.info("║" + " " * 68 + "║")
    logger.info("╚" + "=" * 68 + "╝")
    logger.info("")
    logger.info(f"Bootstrap servers: {bootstrap_servers}")
    logger.info(f"Topics: {topics}")
    logger.info("")
    logger.info("Monitoring for incoming messages...")
    logger.info("Press Ctrl-C to stop")
    logger.info("=" * 70)
    logger.info("")
    
    monitors = []
    threads = []
    
    try:
        # Start a monitor thread for each topic
        for topic in topics:
            monitor = TopicMonitor(topic, bootstrap_servers)
            monitors.append(monitor)
            
            thread = threading.Thread(
                target=monitor.start,
                name=f"Monitor-{topic}",
                daemon=True,
            )
            thread.start()
            threads.append(thread)
            time.sleep(0.5)
        
        # Status reporter thread
        def report_status():
            while True:
                time.sleep(10)
                logger.info("")
                logger.info("=" * 70)
                logger.info("STATUS REPORT")
                logger.info("=" * 70)
                for monitor in monitors:
                    status = monitor.get_status()
                    logger.info(
                        f"{status['topic']:20} messages={status['message_count']:5} "
                        f"last_msg={status['last_message'] or 'none'}"
                    )
                logger.info("=" * 70)
                logger.info("")
        
        status_thread = threading.Thread(target=report_status, daemon=True)
        status_thread.start()
        
        # Keep main thread alive
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("")
        logger.info("Ctrl-C received — stopping monitors")
        for monitor in monitors:
            monitor.stop()
        logger.info("=" * 70)
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
