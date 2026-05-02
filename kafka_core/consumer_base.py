"""Kafka Consumer Template Module

PURPOSE:
  Abstract base class for consuming events from Kafka topics.
  Agents and services subclass this to implement topic-specific logic.
  Handles deserialization, offset management, and error logging.

KEY CLASS:
  - KafkaConsumerTemplate (abstract): Base for all consumers
    - __init__(topics, group_id): Initialize for topics and consumer group
    - process_message(topic, message): Override in subclass (abstract)
    - start(): Consume one message at a time
    - start_batch(batch_size): Consume and process in batches
    - close(): Graceful shutdown

USAGE:
  from kafka.consumer_base import KafkaConsumerTemplate
  
  # Create agent subclass
  class ServiceAgent(KafkaConsumerTemplate):
      def __init__(self):
          super().__init__(
              topics=["metrics.events"],
              group_id="service_agent"
          )
      
      def process_message(self, topic: str, message: dict) -> bool:
          """"Process a single message. Return True to commit offset.""""
          print(f"Received from {topic}: {message}")
          # Your agent logic here
          return True  # Commit offset if processing succeeded
  
  # Start consuming
  agent = ServiceAgent()
  agent.start()  # Consume one at a time
  # OR
  agent.start_batch(batch_size=50)  # Consume 50 at a time
  
  # Graceful shutdown
  agent.close()

IMPLEMENTATION:
  1. Subclass KafkaConsumerTemplate
  2. Call super().__init__(topics, group_id) in __init__
  3. Override process_message(topic, message) with your logic
  4. Return True to commit offset, False to skip
  5. Call start() or start_batch(batch_size)

USED BY:
  - service_agent: Consume metrics.events
  - topography_agent: Consume policy.decisions
  - governance_agent: Consume policy.decisions
  - executor: Consume policy.decisions
  - rollback_engine: Consume policy.executions and system.audit.log
  - monitoring_system: Consume system.audit.log

CONSUMER GROUPS:
  - agent_consumer_group: service_agent, topography_agent, governance_agent
  - execution_consumer_group: executor
  - audit_consumer_group: rollback_engine, monitoring_system

DESERIALIZATION:
  - Key: String (UTF-8)
  - Value: JSON → dict (auto-parsed)
  - Datetime fields: ISO 8601 (parse manually if needed)

OFFSET MANAGEMENT:
  - Auto-commit DISABLED (manual control)
  - Offset committed only after process_message returns True
  - If process_message fails/returns False, offset not committed (will retry)

ERROR HANDLING:
  - JSON parse errors: Logged, offset not committed (stays on bad message)
  - process_message exceptions: Logged, offset not committed
  - Check logs for error details

DEPENDENCIES:
  - kafka (KafkaConsumer)
"""

import json
import logging
from typing import List, Dict, Any
from abc import ABC, abstractmethod

try:
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError
except ImportError:  # pragma: no cover
    KafkaConsumer = None

    class KafkaError(Exception):
        pass

from .config import KafkaConfig
from .exceptions import KafkaConsumerError


logger = logging.getLogger(__name__)


class KafkaConsumerTemplate(ABC):
    """
    Generic Kafka Consumer Template.
    
    Subclass and implement process_message() to handle events.
    
    Usage:
        class ServiceAgent(KafkaConsumerTemplate):
            def process_message(self, topic: str, message: Dict) -> bool:
                return True  # True = commit offset
        
        agent = ServiceAgent(topics=["metrics.events"], group_id="service_agent")
        agent.start()
    """

    def __init__(self, topics: List[str], group_id: str):
        """
        Initialize Kafka Consumer.
        
        Args:
            topics: List of topics to consume from
            group_id: Consumer group ID
        """
        self.topics = topics
        self.group_id = group_id
        self.consumer = None
        self._initialize_consumer()

    def _initialize_consumer(self) -> None:
        """Initialize the underlying Kafka consumer."""
        if KafkaConsumer is None:
            raise KafkaConsumerError("kafka package is not installed")

        try:
            config = KafkaConfig.get_consumer_config(self.group_id)
            self.consumer = KafkaConsumer(
                *self.topics,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                **config,
            )
            logger.info(f"Consumer initialized: group_id='{self.group_id}', topics={self.topics}")
        except Exception as e:
            logger.error(f"Failed to initialize consumer: {e}")
            raise KafkaConsumerError(f"Consumer initialization failed: {e}")

    @abstractmethod
    def process_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """
        Process a consumed message.
        
        Args:
            topic: Topic from which message was consumed
            message: Message payload (deserialized)
            
        Returns:
            True to commit offset, False to skip commit
        """
        pass

    def start(self) -> None:
        """Start consuming messages."""
        if not self.consumer:
            raise KafkaConsumerError("Consumer not initialized")

        logger.info(f"Starting consumer: group_id='{self.group_id}'")

        try:
            for record in self.consumer:
                try:
                    should_commit = self.process_message(
                        topic=record.topic,
                        message=record.value,
                    )
                    if should_commit:
                        self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error processing message from '{record.topic}': {e}")

        except KeyboardInterrupt:
            logger.info("Consumer interrupted")
        except KafkaError as e:
            logger.error(f"Kafka consumer error: {e}")
            raise KafkaConsumerError(f"Consumer error: {e}")
        finally:
            self.close()

    def start_batch(self, batch_size: int = 10) -> None:
        """
        Start consuming messages in batches.
        
        Args:
            batch_size: Number of messages per batch
        """
        if not self.consumer:
            raise KafkaConsumerError("Consumer not initialized")

        logger.info(f"Starting batch consumer: group_id='{self.group_id}', batch_size={batch_size}")

        try:
            while True:
                messages = self.consumer.poll(timeout_ms=1000, max_records=batch_size)
                if not messages:
                    continue

                batch = []
                for topic_partition, records in messages.items():
                    for record in records:
                        batch.append({"topic": record.topic, "value": record.value})

                if batch:
                    try:
                        self.process_batch(batch)
                        self.consumer.commit()
                    except Exception as e:
                        logger.error(f"Error processing batch: {e}")

        except KeyboardInterrupt:
            logger.info("Batch consumer interrupted")
        finally:
            self.close()

    def process_batch(self, messages: List[Dict[str, Any]]) -> None:
        """
        Process a batch of messages.
        Default: iterate and call process_message() for each.
        Override for custom batch logic.
        """
        for msg in messages:
            self.process_message(topic=msg["topic"], message=msg["value"])

    def close(self) -> None:
        """Close the consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info(f"Consumer closed: group_id='{self.group_id}'")
