"""Kafka Configuration Module

PURPOSE:
  Central configuration for Kafka broker, producer, consumer, and topic definitions.
  All Kafka-related settings are managed here with ENV variable overrides.

KEY CLASSES:
  - KafkaConfig: Static class with broker settings, producer/consumer configs
  - TopicConfig: Dataclass defining partition count, retention, partition key, schema

TOPICS CONFIGURED:
  - metrics.events (3 partitions): Raw metrics from mock/real services
  - system.audit.log (1 partition): Audit trail for all actions and decisions
  - policy.decisions (2 partitions): Agent decisions pending approval/execution
  - policy.executions (2 partitions): Execution results of approved policies

USAGE:
  from kafka.config import KafkaConfig
  
  # Get producer config for initialization
  producer_cfg = KafkaConfig.get_producer_config()
  
  # Get consumer config for a specific consumer group
  consumer_cfg = KafkaConfig.get_consumer_config(group_id="service_agent")
  
  # Get topic config (partitions, retention, partition key)
  topic_cfg = KafkaConfig.get_topic_config("metrics.events")
  print(f"Partitions: {topic_cfg.partitions}, Key: {topic_cfg.partition_key}")

ENVIRONMENT VARIABLES:
  - KAFKA_BOOTSTRAP_SERVERS: Broker address(es), default=localhost:9092
  - KAFKA_PRODUCER_ACKS: Producer ack mode (all/1/0), default=all
  - KAFKA_PRODUCER_RETRIES: Retries on failure, default=3
  - KAFKA_CONSUMER_AUTO_OFFSET_RESET: earliest/latest, default=earliest
  - KAFKA_CONSUMER_ENABLE_AUTO_COMMIT: true/false, default=false (manual commit)

NOTES:
  - All settings use ENV vars for flexibility (dev vs prod vs docker)
  - Consumer auto-commit is DISABLED for manual offset management (ordered events)
  - Topic configs include retention (7 days), partition key strategy
"""

import os
from typing import Dict, Any
from dataclasses import dataclass, field


@dataclass
class TopicConfig:
    """Configuration for a Kafka topic."""
    name: str
    partitions: int
    retention_ms: int = 7 * 24 * 60 * 60 * 1000  # 7 days default
    replication_factor: int = 1  # Single broker for dev
    partition_key_field: str = "key"  # Default partition key field

    def to_kafka_config(self) -> Dict[str, Any]:
        """Convert to kafka-python AdminClient topic config."""
        return {
            "num_partitions": self.partitions,
            "replication_factor": self.replication_factor,
            "config": {
                "retention.ms": str(self.retention_ms),
            }
        }


class KafkaConfig:
    """Global Kafka configuration."""

    # Broker settings
    BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

    # Producer settings
    PRODUCER_ACKS = os.getenv("KAFKA_PRODUCER_ACKS", "all")  # all, 1, 0
    PRODUCER_RETRIES = int(os.getenv("KAFKA_PRODUCER_RETRIES", "3"))

    # Consumer settings
    CONSUMER_AUTO_OFFSET_RESET = os.getenv("KAFKA_CONSUMER_AUTO_OFFSET_RESET", "earliest")
    CONSUMER_ENABLE_AUTO_COMMIT = os.getenv("KAFKA_CONSUMER_ENABLE_AUTO_COMMIT", "false").lower() == "true"

    # Message encoding
    MESSAGE_ENCODING = "utf-8"

    # Topics Configuration
    TOPICS: Dict[str, TopicConfig] = {
        "metrics.events": TopicConfig(
            name="metrics.events",
            partitions=3,
            retention_ms=7 * 24 * 60 * 60 * 1000,
            partition_key_field="service_id",
        ),
        "service.state": TopicConfig(
            name="service.state",
            partitions=2,
            retention_ms=7 * 24 * 60 * 60 * 1000,
            partition_key_field="service_id",
        ),
        "system.audit.log": TopicConfig(
            name="system.audit.log",
            partitions=1,
            retention_ms=7 * 24 * 60 * 60 * 1000,
            partition_key_field="event_id",
        ),
        "policy.decisions": TopicConfig(
            name="policy.decisions",
            partitions=2,
            retention_ms=7 * 24 * 60 * 60 * 1000,
            partition_key_field="decision_id",
        ),
        "policy.executions": TopicConfig(
            name="policy.executions",
            partitions=2,
            retention_ms=7 * 24 * 60 * 60 * 1000,
            partition_key_field="execution_id",
        ),
    }

    @classmethod
    def get_topic_config(cls, topic_name: str) -> TopicConfig:
        """Get configuration for a specific topic."""
        if topic_name not in cls.TOPICS:
            raise ValueError(f"Topic '{topic_name}' not found in configuration")
        return cls.TOPICS[topic_name]

    @classmethod
    def get_producer_config(cls) -> Dict[str, Any]:
        """Get producer configuration."""
        return {
            "bootstrap_servers": cls.BOOTSTRAP_SERVERS,
            "acks": cls.PRODUCER_ACKS,
            "retries": cls.PRODUCER_RETRIES,
        }

    @classmethod
    def get_consumer_config(cls, group_id: str) -> Dict[str, Any]:
        """Get consumer configuration."""
        return {
            "bootstrap_servers": cls.BOOTSTRAP_SERVERS,
            "group_id": group_id,
            "auto_offset_reset": cls.CONSUMER_AUTO_OFFSET_RESET,
            "enable_auto_commit": cls.CONSUMER_ENABLE_AUTO_COMMIT,
        }
