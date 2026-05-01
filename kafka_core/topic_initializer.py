"""Kafka Topic Initializer Module.

Creates configured Kafka topics at startup and validates partitioning.
"""

import logging
from typing import Dict, List, Optional

from kafka.admin import KafkaAdminClient, NewPartitions, NewTopic
from kafka.errors import TopicAlreadyExistsError

from .config import KafkaConfig
from .exceptions import TopicInitializationError


logger = logging.getLogger(__name__)


class TopicInitializer:
    """Initialize Kafka topics automatically."""

    def __init__(self, bootstrap_servers: Optional[List[str]] = None):
        self.bootstrap_servers = bootstrap_servers or KafkaConfig.BOOTSTRAP_SERVERS
        self.admin_client = None
        self._initialize_admin_client()

    def _initialize_admin_client(self) -> None:
        """Initialize Kafka admin client."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=10000,
            )
            logger.info("Kafka Admin Client initialized with brokers: %s", self.bootstrap_servers)
        except Exception as e:
            logger.error("Failed to initialize Kafka Admin Client: %s", e)
            raise TopicInitializationError(f"Admin client init failed: {e}")

    def _get_topic_partition_count(self, topic_name: str) -> Optional[int]:
        """Return current partition count for topic, or None when unavailable."""
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        try:
            metadata = self.admin_client.describe_topics(topics=[topic_name])
            if isinstance(metadata, list) and metadata:
                return len(metadata[0].get("partitions", []))
            if isinstance(metadata, dict):
                topic_meta = metadata.get(topic_name)
                if topic_meta:
                    return len(topic_meta.get("partitions", []))
            return None
        except Exception as e:
            logger.error("Failed to inspect partitions for topic '%s': %s", topic_name, e)
            return None

    def _ensure_min_partitions(self, topic_name: str, min_partitions: int) -> bool:
        """Ensure topic has at least min_partitions by increasing when needed."""
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        current = self._get_topic_partition_count(topic_name)
        if current is None:
            return False

        if current >= min_partitions:
            logger.info(
                "Topic '%s' partition config OK: current=%s required=%s",
                topic_name,
                current,
                min_partitions,
            )
            return True

        try:
            response = self.admin_client.create_partitions(
                {topic_name: NewPartitions(total_count=min_partitions)},
                validate_only=False,
            )

            # kafka-python versions differ here:
            # - newer: dict(topic -> future)
            # - older: CreatePartitionsResponse
            if hasattr(response, "items"):
                for topic, future in response.items():
                    future.result(timeout=10)
                    logger.info(
                        "Increased partitions for topic '%s' from %s to %s",
                        topic,
                        current,
                        min_partitions,
                    )
            else:
                updated = self._get_topic_partition_count(topic_name)
                if updated is None or updated < min_partitions:
                    logger.error(
                        "Partition increase response received but topic '%s' still has %s partitions",
                        topic_name,
                        updated,
                    )
                    return False
                logger.info(
                    "Increased partitions for topic '%s' from %s to %s",
                    topic_name,
                    current,
                    updated,
                )

            return True
        except Exception as e:
            logger.error("Failed to increase partitions for topic '%s': %s", topic_name, e)
            return False

    def create_topic(
        self,
        topic_name: str,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
    ) -> bool:
        """Create topic if missing; validate/adjust existing topic when needed."""
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        if topic_name not in KafkaConfig.TOPICS:
            logger.warning("Topic '%s' not found in configuration", topic_name)
            return False

        topic_config = KafkaConfig.TOPICS[topic_name]
        num_partitions = num_partitions or topic_config.partitions
        replication_factor = replication_factor or topic_config.replication_factor

        try:
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=topic_config.to_kafka_config()["config"],
            )
            futures = self.admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            for topic, future in futures.items():
                try:
                    future.result(timeout=10)
                    logger.info(
                        "Topic created: '%s' (partitions=%s, replication_factor=%s)",
                        topic,
                        num_partitions,
                        replication_factor,
                    )
                except TopicAlreadyExistsError:
                    logger.info("Topic already exists: '%s'", topic)
        except TopicAlreadyExistsError:
            logger.info("Topic already exists: '%s'", topic_name)
        except Exception as e:
            logger.error("Error creating topic '%s': %s", topic_name, e)
            return False

        # Enforce metrics.events partition requirement.
        if topic_name == "metrics.events":
            return self._ensure_min_partitions(topic_name, 3)
        return True

    def create_all_topics(self) -> Dict[str, bool]:
        """Create all configured topics and log startup partition state."""
        results: Dict[str, bool] = {}
        logger.info("Creating %s topics...", len(KafkaConfig.TOPICS))

        for topic_name in KafkaConfig.TOPICS:
            results[topic_name] = self.create_topic(topic_name)

        success_count = sum(1 for ok in results.values() if ok)
        logger.info("Topic creation summary: %s/%s successful", success_count, len(results))

        metrics_partitions = self._get_topic_partition_count("metrics.events")
        if metrics_partitions is not None:
            logger.info(
                "Partition configuration at startup: topic='metrics.events', partitions=%s, required=%s",
                metrics_partitions,
                3,
            )
        else:
            logger.warning("Could not verify startup partition configuration for 'metrics.events'")

        return results

    def delete_topic(self, topic_name: str) -> bool:
        """Delete a topic."""
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        try:
            futures = self.admin_client.delete_topics(topics=[topic_name])
            for topic, future in futures.items():
                future.result(timeout=10)
                logger.info("Topic deleted: '%s'", topic)
                return True
        except Exception as e:
            logger.error("Error deleting topic '%s': %s", topic_name, e)
            return False

    def delete_all_topics(self) -> Dict[str, bool]:
        """Delete all configured topics."""
        results: Dict[str, bool] = {}
        logger.warning("Deleting all configured topics...")

        for topic_name in KafkaConfig.TOPICS:
            results[topic_name] = self.delete_topic(topic_name)

        success_count = sum(1 for ok in results.values() if ok)
        logger.warning("Topic deletion summary: %s/%s successful", success_count, len(results))
        return results

    def list_topics(self) -> Dict[str, Dict]:
        """List configured topics on broker."""
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        try:
            return self.admin_client.describe_topics(topics=list(KafkaConfig.TOPICS.keys()))
        except Exception as e:
            logger.error("Error listing topics: %s", e)
            return {}

    def get_topic_info(self, topic_name: str) -> Optional[Dict]:
        """Get detailed metadata for one topic."""
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        try:
            metadata = self.admin_client.describe_topics(topics=[topic_name])
            if isinstance(metadata, list) and metadata:
                return metadata[0]
            if isinstance(metadata, dict):
                return metadata.get(topic_name)
            return None
        except Exception as e:
            logger.error("Error getting info for topic '%s': %s", topic_name, e)
            return None

    def close(self) -> None:
        """Close admin client and release resources."""
        if self.admin_client:
            self.admin_client.close()
            logger.info("Kafka Admin Client closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def initialize_kafka_topics() -> bool:
    """Initialize all Kafka topics."""
    try:
        initializer = TopicInitializer()
        results = initializer.create_all_topics()
        initializer.close()
        return all(results.values())
    except TopicInitializationError as e:
        logger.error("Topic initialization failed: %s", e)
        return False
