"""Kafka Topic Initializer Module

PURPOSE:
  Automatically creates Kafka topics on application startup.
  Idempotent (safe to run multiple times, doesn't error if topics exist).
  Single entry point for all topic initialization.

KEY CLASS:
  - TopicInitializer: Manages topic creation
    - create_all_topics(): Create all 4 topics (idempotent)
    - create_topic(topic_name): Create single topic
    - delete_topic(topic_name): Delete topic (testing only)
    - list_topics(): List all topics in cluster

FUNCTION:
  - initialize_kafka_topics(): Convenience function, calls create_all_topics()

USAGE:
  from kafka.topic_initializer import initialize_kafka_topics
  
  # On application startup (main or app factory)
  success = initialize_kafka_topics()
  if success:
      print("Topics initialized successfully")
  else:
      print("Topic initialization failed (check logs)")
  
  # Or use TopicInitializer class directly
  from kafka.topic_initializer import TopicInitializer
  
  initializer = TopicInitializer()
  initializer.create_all_topics()  # Create all 4 topics
  topics = initializer.list_topics()  # List existing topics
  print(f"Topics: {topics}")

TOPICS CREATED:
  - metrics.events (3 partitions): Raw metrics from services
  - service.state (2 partitions): Local service state estimates
  - system.audit.log (1 partition): Audit trail
  - policy.decisions (2 partitions): Agent decisions
  - policy.executions (2 partitions): Execution results

IDEMPOTENCY:
  - Topics are only created if they don't exist
  - Existing topics are skipped
  - Safe to call on every app startup

CALLED FROM:
  - Application initialization (main entry point)
  - Docker entrypoint script
  - Test setup (pytest fixtures)

ERROR HANDLING:
  - TopicAlreadyExistsError: Caught and ignored (idempotent)
  - Other KafkaErrors: Logged and re-raised
  - Returns False if topics already exist (expected), True if created

CONFIGURATION:
  - Topic configs come from kafka.config.KafkaConfig.TOPICS
  - Partitions, retention, and partition key defined there

DEBUGGING:
  - Enable DEBUG logging to see topic creation details
  - list_topics() shows current topics in cluster

DEPENDENCIES:
  - kafka.admin (KafkaAdminClient, NewTopic)
  - kafka.config (KafkaConfig, TopicConfig)
  - kafka.exceptions (TopicInitializationError)
"""

import logging
from typing import List, Dict, Optional

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

from .config import KafkaConfig, TopicConfig
from .exceptions import TopicInitializationError


logger = logging.getLogger(__name__)


class TopicInitializer:
    """
    Initialize Kafka topics automatically.
    
    Usage:
        initializer = TopicInitializer()
        initializer.create_all_topics()
    """

    def __init__(self, bootstrap_servers: Optional[List[str]] = None):
        """
        Initialize TopicInitializer.
        
        Args:
            bootstrap_servers: Kafka broker addresses
        """
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
            logger.info(f"Kafka Admin Client initialized with brokers: {self.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Admin Client: {e}")
            raise TopicInitializationError(f"Admin client init failed: {e}")

    def create_topic(
        self,
        topic_name: str,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
    ) -> bool:
        """
        Create a single topic.
        
        Args:
            topic_name: Name of topic
            num_partitions: Number of partitions (uses config if not provided)
            replication_factor: Replication factor (uses config if not provided)
            
        Returns:
            True if topic created or already exists, False on error
        """
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        # Get topic config
        if topic_name not in KafkaConfig.TOPICS:
            logger.warning(f"Topic '{topic_name}' not found in configuration")
            return False

        topic_config = KafkaConfig.TOPICS[topic_name]
        num_partitions = num_partitions or topic_config.partitions
        replication_factor = replication_factor or topic_config.replication_factor

        try:
            # Create NewTopic object
            new_topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                topic_configs=topic_config.to_kafka_config()["config"],
            )

            # Create topic
            fs = self.admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            
            # Wait for creation
            for topic, future in fs.items():
                try:
                    future.result(timeout_sec=10)
                    logger.info(
                        f"Topic created: '{topic}' "
                        f"(partitions={num_partitions}, replication_factor={replication_factor})"
                    )
                    return True
                except TopicAlreadyExistsError:
                    logger.info(f"Topic already exists: '{topic}'")
                    return True

        except Exception as e:
            logger.error(f"Error creating topic '{topic_name}': {e}")
            return False

    def create_all_topics(self) -> Dict[str, bool]:
        """
        Create all topics from configuration.
        
        Returns:
            Dict mapping topic_name -> success (bool)
        """
        results = {}
        
        logger.info(f"Creating {len(KafkaConfig.TOPICS)} topics...")

        for topic_name in KafkaConfig.TOPICS:
            results[topic_name] = self.create_topic(topic_name)

        # Log summary
        success_count = sum(1 for v in results.values() if v)
        logger.info(f"Topic creation summary: {success_count}/{len(results)} successful")

        return results

    def delete_topic(self, topic_name: str) -> bool:
        """
        Delete a topic.
        
        Args:
            topic_name: Name of topic to delete
            
        Returns:
            True if deleted, False if error
        """
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        try:
            fs = self.admin_client.delete_topics(topics=[topic_name])
            for topic, future in fs.items():
                future.result(timeout_sec=10)
                logger.info(f"Topic deleted: '{topic}'")
                return True
        except Exception as e:
            logger.error(f"Error deleting topic '{topic_name}': {e}")
            return False

    def delete_all_topics(self) -> Dict[str, bool]:
        """
        Delete all configured topics.
        
        Returns:
            Dict mapping topic_name -> success (bool)
        """
        results = {}
        logger.warning("Deleting all configured topics...")

        for topic_name in KafkaConfig.TOPICS:
            results[topic_name] = self.delete_topic(topic_name)

        success_count = sum(1 for v in results.values() if v)
        logger.warning(f"Topic deletion summary: {success_count}/{len(results)} successful")

        return results

    def list_topics(self) -> Dict[str, Dict]:
        """
        List all topics on broker.
        
        Returns:
            Dict of topic_name -> topic_metadata
        """
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        try:
            metadata = self.admin_client.describe_topics(topics=list(KafkaConfig.TOPICS.keys()))
            return metadata
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return {}

    def get_topic_info(self, topic_name: str) -> Optional[Dict]:
        """
        Get detailed info about a topic.
        
        Args:
            topic_name: Name of topic
            
        Returns:
            Topic metadata dict or None
        """
        if not self.admin_client:
            raise TopicInitializationError("Admin client not initialized")

        try:
            metadata = self.admin_client.describe_topics(topics=[topic_name])
            return metadata.get(topic_name)
        except Exception as e:
            logger.error(f"Error getting info for topic '{topic_name}': {e}")
            return None

    def close(self) -> None:
        """Close admin client and release resources."""
        if self.admin_client:
            self.admin_client.close()
            logger.info("Kafka Admin Client closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Convenience function for initialization
def initialize_kafka_topics() -> bool:
    """
    Initialize all Kafka topics.
    
    Returns:
        True if all topics created successfully
    """
    try:
        initializer = TopicInitializer()
        results = initializer.create_all_topics()
        initializer.close()
        return all(results.values())
    except TopicInitializationError as e:
        logger.error(f"Topic initialization failed: {e}")
        return False
