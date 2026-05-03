"""Kafka Producer Template Module

PURPOSE:
  Generic producer template for sending events to any Kafka topic.
  Single producer handles all 4 topics (metrics.events, policy.decisions, etc.).
  Handles serialization, error logging, and graceful shutdown.

KEY CLASS:
  - KafkaProducerTemplate: Generic producer for all topics
    - send(topic, event): Send event synchronously
    - flush(): Wait for pending messages
    - close(): Graceful shutdown

USAGE:
  from kafka import KafkaProducerTemplate
  from kafka.schemas import MetricsEvent, MetricsEventValue
  from kafka.enums import CloudProvider
  from datetime import datetime
  
  # Initialize producer (once)
  producer = KafkaProducerTemplate()
  
  # Send metric event
  event = MetricsEvent(
      key="service-api@aws",
      value=MetricsEventValue(
          service="service-api",
          cloud=CloudProvider.AWS,
          timestamp=datetime.now(),
          metrics={"latency_ms": 100}
      )
  )
  success = producer.send("metrics.events", event)
  
  if success:
      print("Event sent successfully")
  else:
      print("Failed to send event (check logs)")
  
  # Graceful shutdown
  producer.flush()
  producer.close()

USED BY:
  - service_adapters: Publish metrics.events
  - service_agent: Publish audit logs and policy decisions
  - topography_agent: Publish policy decisions
  - governance_agent: Publish policy decisions and audit logs
  - executor: Publish audit logs and execution results
  - rollback_engine: Publish audit logs
  - human_in_the_loop: Publish decision approvals/rejections

ERROR HANDLING:
  - Exceptions are caught and logged (not raised)
  - Failed sends return False
  - Check logs for details

SERIALIZATION:
  - Key: String (UTF-8)
  - Value: JSON (Pydantic model → dict → JSON)
  - Datetime fields: ISO 8601 format

DEPENDENCIES:
  - kafka (KafkaProducer)
  - pydantic (schema validation)
"""

import json
import logging
from typing import Any
from datetime import datetime
from enum import Enum
from json import JSONEncoder

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError:  # pragma: no cover
    KafkaProducer = None

    class KafkaError(Exception):
        pass

try:
    from pydantic import BaseModel
except ImportError:  # pragma: no cover
    class BaseModel:
        pass

from .config import KafkaConfig
from .exceptions import KafkaProducerError


logger = logging.getLogger(__name__)


class DateTimeEncoder(JSONEncoder):
    """JSON encoder for datetime objects."""
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Enum):
            return obj.value
        return super().default(obj)


class KafkaProducerTemplate:
    """
    Generic Kafka Producer Template.
    
    Usage:
        producer = KafkaProducerTemplate()
        event = MetricsEvent(key="service_id", value=MetricsEventValue(...))
        producer.send("metrics.events", event)
    """

    def __init__(self):
        """Initialize Kafka Producer."""
        self.producer = None
        self._initialize_producer()

    def _initialize_producer(self) -> None:
        """Initialize the underlying Kafka producer."""
        if KafkaProducer is None:
            raise KafkaProducerError("kafka package is not installed")

        try:
            config = KafkaConfig.get_producer_config()
            self.producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(
                    v, cls=DateTimeEncoder, default=str
                ).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                **config,
            )
            logger.info("Kafka Producer initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka Producer: {e}")
            raise KafkaProducerError(f"Producer initialization failed: {e}")

    def send(
        self,
        topic: str,
        event: BaseModel,
        key: str = None,
        partition: int = None,
    ) -> str:
        """
        Send event to Kafka topic (blocking).
        
        Args:
            topic: Kafka topic name
            event: Pydantic model with 'key' and 'value' fields
            
        Returns:
            Record metadata string (topic@partition:offset)
            
        Raises:
            KafkaProducerError: If send fails
        """
        if not self.producer:
            raise KafkaProducerError("Producer not initialized")

        try:
            # Extract key and value from event
            key = (
                key
                if key is not None
                else event.key if hasattr(event, "key") else None
            )
            value = event.value if hasattr(event, "value") else event

            # Send to Kafka
            future = self.producer.send(
                topic,
                key=key,
                value=value.dict() if isinstance(value, BaseModel) else value,
                partition=partition,
            )

            # Block until send completes
            record_metadata = future.get(timeout=10)

            logger.info(
                f"Message sent to topic='{topic}', partition={record_metadata.partition}, offset={record_metadata.offset}"
            )

            return f"{record_metadata.topic}@{record_metadata.partition}:{record_metadata.offset}"

        except KafkaError as e:
            error_msg = f"Kafka error sending to topic '{topic}': {e}"
            logger.error(error_msg)
            raise KafkaProducerError(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error sending to topic '{topic}': {e}"
            logger.error(error_msg)
            raise KafkaProducerError(error_msg)

    def close(self) -> None:
        """Close the producer and release resources."""
        if self.producer:
            self.producer.close()
            logger.info("Kafka Producer closed")

    def flush(self) -> None:
        """Flush pending records to Kafka."""
        if self.producer:
            self.producer.flush()
