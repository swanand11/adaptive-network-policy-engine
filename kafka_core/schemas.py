"""Kafka Event Schemas Module

PURPOSE:
  Pydantic models for validating and serializing Kafka events.
  Ensures all events conform to their topic schema before sending/after receiving.

SCHEMAS DEFINED:
  - MetricsEvent: Raw metrics from services (metrics.events topic)
  - AuditLogEvent: Audit trail for all actions (system.audit.log topic)
  - PolicyDecision: Agent decisions (policy.decisions topic)
  - PolicyExecution: Execution results (policy.executions topic)

KEY FIELDS (all events):
  - key: Partition key (service_id, event_id, decision_id, execution_id)
  - value: Event payload (Pydantic model with validation)
  - timestamp: Event creation time

USAGE:
  from kafka.schemas import MetricsEvent, MetricsEventValue, PolicyDecision
  from kafka.enums import CloudProvider
  from datetime import datetime
  
  # Create and validate an event
  event = MetricsEvent(
      key="service-api@aws",  # Partition key
      value=MetricsEventValue(
          service="service-api",
          cloud=CloudProvider.AWS,
          timestamp=datetime.now(),
          metrics={"latency_ms": 100, "error_rate": 0.01}
      )
  )
  
  # Validation happens automatically
  # Pass to producer: producer.send("metrics.events", event)
  
  # Deserialize received event
  received_dict = {"key": "...", "value": {...}}
  event = MetricsEvent(**received_dict)  # Auto-validates

VALIDATION:
  - Pydantic enforces type checking
  - Required fields must be present
  - Enum fields must be valid values
  - Raises ValidationError if schema doesn't match

EXTENSION:
  Add new schema models for new topics.
  Update producer/consumer to handle new schemas.

DEPENDENCIES:
  - kafka.enums (CloudProvider, PolicyStatus, ExecutionStatus, RiskLevel)
  - pydantic (validation)

"""

from typing import Any, Dict, Optional
from datetime import datetime

try:
    from pydantic import BaseModel, Field
except ImportError:  # pragma: no cover
    class BaseModel:
        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)

        def dict(self):
            result = {}
            for key, value in self.__dict__.items():
                result[key] = value.dict() if hasattr(value, "dict") else value
            return result

    def Field(default=None, **kwargs):
        return default

from .enums import CloudProvider, PolicyStatus, ExecutionStatus, RiskLevel


# ============================================================
# Metrics Events Topic Schema
# ============================================================
class MetricsEventValue(BaseModel):
    """Value schema for metrics.events topic."""
    service: str = Field(..., description="Service identifier")
    cloud: CloudProvider = Field(..., description="Cloud provider")
    timestamp: datetime = Field(..., description="Event timestamp")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Metric data (CPU, latency, etc.)")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")
    parent_event_id: Optional[str] = Field(None, description="Parent event ID for chaining")

    class Config:
        use_enum_values = False


class MetricsEvent(BaseModel):
    """Complete metrics event with key and value."""
    key: str = Field(..., description="Partition key (service_id)")
    value: MetricsEventValue


# ============================================================
# System Audit Log Topic Schema
# ============================================================
class AuditLogEventValue(BaseModel):
    """Value schema for system.audit.log topic."""
    actor: str = Field(..., description="Component/actor performing action")
    action: str = Field(..., description="Action performed (e.g., 'decision_generated', 'policy_approved')")
    status: str = Field(..., description="Status of action (e.g., 'success', 'failed')")
    timestamp: datetime = Field(..., description="Event timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")
    decision_id: Optional[str] = Field(None, description="Link to decision if applicable")

    class Config:
        use_enum_values = False


class AuditLogEvent(BaseModel):
    """Complete audit log event with key and value."""
    key: str = Field(..., description="Partition key (event_id)")
    value: AuditLogEventValue


# ============================================================
# Policy Decisions Topic Schema
# ============================================================
class PolicyDecisionValue(BaseModel):
    """Value schema for policy.decisions topic."""
    service: str = Field(..., description="Service identifier")
    decision: str = Field(..., description="Decision details (e.g., 'shift 20% traffic from AWS to Azure')")
    risk_level: RiskLevel = Field(..., description="Risk classification")
    status: PolicyStatus = Field(..., description="Decision status")
    timestamp: datetime = Field(..., description="Event timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")
    parent_event_id: Optional[str] = Field(None, description="Parent event ID for chaining")

    class Config:
        use_enum_values = False


class PolicyDecision(BaseModel):
    """Complete policy decision with key and value."""
    key: str = Field(..., description="Partition key (decision_id)")
    value: PolicyDecisionValue


# ============================================================
# Policy Executions Topic Schema
# ============================================================
class PolicyExecutionValue(BaseModel):
    """Value schema for policy.executions topic."""
    decision_id: str = Field(..., description="Reference to decision_id")
    service: str = Field(..., description="Service identifier")
    action_taken: str = Field(..., description="Action executed (e.g., 'updated NGINX weights')")
    status: ExecutionStatus = Field(..., description="Execution result status")
    timestamp: datetime = Field(..., description="Event timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata (error details, etc.)")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")

    class Config:
        use_enum_values = False


class PolicyExecution(BaseModel):
    """Complete policy execution with key and value."""
    key: str = Field(..., description="Partition key (execution_id)")
    value: PolicyExecutionValue
