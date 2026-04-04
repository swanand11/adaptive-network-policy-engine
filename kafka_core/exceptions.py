"""Kafka Exceptions Module

PURPOSE:
  Custom exception classes for Kafka operations.
  Allows fine-grained error handling in producers/consumers.

EXCEPTIONS DEFINED:
  - KafkaConfigError: Configuration is invalid (missing ENV vars, bad values)
  - KafkaProducerError: Producer failed to send event
  - KafkaConsumerError: Consumer failed to initialize or process
  - SchemaValidationError: Pydantic schema validation failed
  - TopicInitializationError: Topic creation/initialization failed
  - MessageDeserializationError: Failed to deserialize message from Kafka

USAGE:
  from kafka.exceptions import (
      KafkaProducerError,
      KafkaConsumerError,
      SchemaValidationError
  )
  from kafka import KafkaProducerTemplate
  
  try:
      producer = KafkaProducerTemplate()
      producer.send("metrics.events", event)
  except KafkaProducerError as e:
      print(f"Producer error: {e}")
  except SchemaValidationError as e:
      print(f"Schema validation failed: {e}")
  
  try:
      agent = ServiceAgent()
      agent.start()
  except KafkaConsumerError as e:
      print(f"Consumer error: {e}")

ERROR HANDLING STRATEGY:
  - Producer/Consumer catch exceptions and log (not raise to caller)
  - process_message errors are logged, offset not committed (retry)
  - Schema validation errors are logged, event skipped
  - Configuration errors should fail fast on startup

CURRENT IMPLEMENTATION:
  - Producer sends return True/False (don't raise)
  - Consumer logs errors but continues processing
  - Topic initializer raises TopicInitializationError on failure

FUTURE ENHANCEMENT:
  - Dead letter queue for malformed events (Phase 3+)
  - Custom retry logic with backoff
  - Metrics for error rates per producer/consumer
"""


class KafkaConfigError(Exception):
    """Raised when Kafka configuration is invalid."""
    pass


class KafkaProducerError(Exception):
    """Raised when producer encounters an error."""
    pass


class KafkaConsumerError(Exception):
    """Raised when consumer encounters an error."""
    pass


class SchemaValidationError(Exception):
    """Raised when event schema validation fails."""
    pass


class TopicInitializationError(Exception):
    """Raised when topic initialization fails."""
    pass


class MessageDeserializationError(Exception):
    """Raised when message deserialization fails."""
    pass
