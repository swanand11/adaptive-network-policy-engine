"""Kafka Module - Event-Driven Architecture

=============================================================================
OVERVIEW:
  Complete Kafka abstraction for multi-cloud traffic management system.
  Handles metrics ingestion, policy decisions, execution results, and audit logs.
  Generic producer/consumer templates for all topics.

=============================================================================
TOPICS (4 total):
  1. metrics.events (3 partitions)
     - Raw metrics from mock/real cloud services
     - Key: service_id@cloud_provider
     - Producers: service_adapters
     - Consumers: service_agent, topography_agent, governance_agent

  2. system.audit.log (1 partition)
     - Audit trail for all actions and decisions
     - Key: event_id
     - Producers: all agents, executor, rollback_engine
     - Consumers: rollback_engine, monitoring_system

  3. policy.decisions (2 partitions)
     - Agent decisions (pending/approved/rejected)
     - Key: decision_id
     - Producers: governance_agent, human_in_the_loop
     - Consumers: human_in_the_loop, executor

  4. policy.executions (2 partitions)
     - Execution results of approved policies
     - Key: execution_id
     - Producers: executor
     - Consumers: rollback_engine, final_status_aggregator

=============================================================================
QUICK START:

  1. Initialize topics on app startup:
       from kafka import initialize_kafka_topics
       initialize_kafka_topics()

  2. Send events (all producers use same template):
       from kafka import KafkaProducerTemplate, MetricsEvent, MetricsEventValue
       from kafka.enums import CloudProvider
       from datetime import datetime
       
       producer = KafkaProducerTemplate()
       event = MetricsEvent(
           key=\"service-api@aws\",
           value=MetricsEventValue(
               service=\"service-api\",
               cloud=CloudProvider.AWS,
               timestamp=datetime.now(),
               metrics={\"latency_ms\": 100}
           )
       )
       producer.send(\"metrics.events\", event)
       producer.close()

  3. Consume events (agents subclass consumer):
       from kafka import KafkaConsumerTemplate
       
       class ServiceAgent(KafkaConsumerTemplate):
           def __init__(self):
               super().__init__(
                   topics=[\"metrics.events\"],
                   group_id=\"service_agent\"
               )
           
           def process_message(self, topic: str, message: dict) -> bool:
               print(f\"Received: {message}\")
               return True  # Commit offset
       
       agent = ServiceAgent()
       agent.start()  # or agent.start_batch(batch_size=50)

=============================================================================
EXPORTED CLASSES & FUNCTIONS:

  Producers/Consumers:
    - KafkaProducerTemplate: Send events to any topic
    - KafkaConsumerTemplate: Consume events from topics (subclass it)

  Configuration:
    - KafkaConfig: Broker settings, topic configs (read from ENV vars)
    - TopicInitializer: Create topics on startup

  Enums (type-safe constants):
    - CloudProvider: aws, azure, gcp
    - PolicyStatus: pending, approved, rejected
    - ExecutionStatus: success, failed, in_progress
    - RiskLevel: low, medium, high, critical

  Schemas (Pydantic models for all topics):
    - MetricsEvent, MetricsEventValue
    - AuditLogEvent, AuditLogEventValue
    - PolicyDecision, PolicyDecisionValue
    - PolicyExecution, PolicyExecutionValue

=============================================================================
COMMON USAGE PATTERNS:

  PATTERN 1: Initialize and send metrics
    from kafka import KafkaProducerTemplate, MetricsEvent, MetricsEventValue
    from kafka.enums import CloudProvider
    from datetime import datetime

    producer = KafkaProducerTemplate()
    event = MetricsEvent(
        key=f\"service-id@{cloud_provider}\",
        value=MetricsEventValue(
            service=\"service-name\",
            cloud=CloudProvider.AWS,
            timestamp=datetime.now(),
            metrics={...}
        )
    )
    producer.send(\"metrics.events\", event)

  PATTERN 2: Build an agent consumer
    from kafka import KafkaConsumerTemplate

    class MyAgent(KafkaConsumerTemplate):
        def process_message(self, topic: str, message: dict):
            # Your agent logic here
            return True  # Commit offset if successful

    agent = MyAgent(topics=[\"metrics.events\"], group_id=\"my_agent\")
    agent.start_batch(batch_size=50)

  PATTERN 3: Publish audit logs
    from kafka import KafkaProducerTemplate, AuditLogEvent, AuditLogEventValue

    producer = KafkaProducerTemplate()
    event = AuditLogEvent(
        key=f\"event-{uuid.uuid4()}\",
        value=AuditLogEventValue(
            actor=\"service_agent\",
            action=\"decision_generated\",
            status=\"success\",
            timestamp=datetime.now(),
            metadata={\"decision_id\": \"...\"}
        )
    )
    producer.send(\"system.audit.log\", event)

=============================================================================
CONFIGURATION:

  All Kafka settings come from environment variables (see kafka/config.py):
    - KAFKA_BOOTSTRAP_SERVERS (default: localhost:9092)
    - KAFKA_PRODUCER_ACKS (default: all)
    - KAFKA_PRODUCER_RETRIES (default: 3)
    - KAFKA_CONSUMER_AUTO_OFFSET_RESET (default: earliest)
    - KAFKA_CONSUMER_ENABLE_AUTO_COMMIT (default: false)

  For docker-compose containers, set:
    - KAFKA_BOOTSTRAP_SERVERS=kafka:9092

  For local development, uses localhost:9092 (default)

=============================================================================
FILE DOCUMENTATION:

  kafka/config.py
    - Topic definitions (partitions, retention, partition keys)
    - Broker settings (ENV-driven)
    - get_producer_config(), get_consumer_config(), get_topic_config()

  kafka/enums.py
    - CloudProvider, PolicyStatus, ExecutionStatus, RiskLevel
    - Type-safe constants

  kafka/schemas.py
    - Pydantic models for all 4 topic schemas
    - Validation on serialize/deserialize

  kafka/producer_base.py
    - KafkaProducerTemplate: Send events to any topic
    - Handles serialization, error logging

  kafka/consumer_base.py
    - KafkaConsumerTemplate (abstract): Subclass to build consumers
    - Handles deserialization, offset management

  kafka/topic_initializer.py
    - TopicInitializer: Create topics on startup (idempotent)
    - initialize_kafka_topics() convenience function

  kafka/exceptions.py
    - Custom exception classes for error handling

=============================================================================
ERRORS & TROUBLESHOOTING:

  \"Connection refused\" error:
    - Kafka broker not running
    - Check docker-compose: docker-compose ps
    - Start Kafka: docker-compose up -d kafka zookeeper

  \"Topic does not exist\" error:
    - Call initialize_kafka_topics() on app startup
    - Or let TopicInitializer.create_all_topics() handle it

  \"offset out of range\" error:
    - Offset was committed but messages were purged (7-day retention)
    - Consumer will auto-reset to earliest (configurable)

  Messages not being consumed:
    - Check consumer group has matching topics
    - Verify consumer_group_id is correct
    - Check logs for deserialization errors

=============================================================================
NEXT STEPS:

  Phase 1-2 (current):
    ✓ Producer/consumer templates ready
    ✓ Topic initialization ready
    ✓ Schemas for all topics ready
    → Next: Build mock services (Flask apps exposing /metrics)
    → Then: Build service adapters (pull from Prometheus → Kafka)
    → Then: Build agents (consume metrics → produce decisions)

  Phase 3+:
    - Add real cloud service adapters (EC2, AKS, DigitalOcean SDKs)
    - Enhance simulation/decision logic in agents
    - Build NGINX controller (consume decisions → update load balancing)
    - Implement rollback engine
    - Build React dashboard + human-in-the-loop approval UI

=============================================================================
"""

from .producer_base import KafkaProducerTemplate
from .consumer_base import KafkaConsumerTemplate
from .topic_initializer import TopicInitializer, initialize_kafka_topics
from .config import KafkaConfig
from .enums import CloudProvider, PolicyStatus, ExecutionStatus, RiskLevel
from .schemas import (
    MetricsEvent,
    MetricsEventValue,
    AuditLogEvent,
    AuditLogEventValue,
    PolicyDecision,
    PolicyDecisionValue,
    PolicyExecution,
    PolicyExecutionValue,
)

__all__ = [
    "KafkaProducerTemplate",
    "KafkaConsumerTemplate",
    "TopicInitializer",
    "initialize_kafka_topics",
    "KafkaConfig",
    "CloudProvider",
    "PolicyStatus",
    "ExecutionStatus",
    "RiskLevel",
    "MetricsEvent",
    "MetricsEventValue",
    "AuditLogEvent",
    "AuditLogEventValue",
    "PolicyDecision",
    "PolicyDecisionValue",
    "PolicyExecution",
    "PolicyExecutionValue",
]
