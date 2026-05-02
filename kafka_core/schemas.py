"""Kafka Event Schemas Module

PURPOSE:
  Pydantic models for validating and serializing Kafka events.

NOTES:
  - The canonical input format for the `metrics.events` topic is the
    flat adapter payload represented by `MetricsEventValue`. Producers
    (the Prometheus adapter) SHOULD send the flat JSON as the message
    value. Consumers SHOULD validate incoming message values against
    `MetricsEventValue`.
  - A legacy wrapped envelope model `MetricsEvent(key, value)` is kept
    for backward compatibility but is considered deprecated.

DEPENDENCIES:
  - kafka.enums (CloudProvider, PolicyStatus, ExecutionStatus, RiskLevel)
  - pydantic (validation)
"""

from typing import Any, Dict, List, Optional
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
# Metrics Events Topic Schema (canonical flat payload)
# ============================================================
class MetricsEventValue(BaseModel):
    """Value schema for `metrics.events` topic (flat adapter payload).

    Example payload produced by the Prometheus adapter should match this
    model (service, cloud, timestamp, metrics, correlation_id, parent_event_id).
    """
    service: str = Field(..., description="Service identifier")
    cloud: CloudProvider = Field(..., description="Cloud provider")
    timestamp: datetime = Field(..., description="Event timestamp")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Metric data (CPU, latency, etc.)")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")
    parent_event_id: Optional[str] = Field(None, description="Parent event ID for chaining")

    class Config:
        use_enum_values = False


class MetricsEvent(BaseModel):
    """(Deprecated) Wrapped metrics event with key and value.

    This model exists for backward compatibility with earlier code that
    used a `{key, value}` envelope. New code should prefer `MetricsEventValue`
    as the canonical payload for `metrics.events`.
    """
    key: Optional[str] = Field(None, description="Partition key (service_id)")
    value: Optional[MetricsEventValue] = None


# ============================================================
# Service State Topic Schemas
# ============================================================
class ServiceStateBelief(BaseModel):
    """Local belief model for service state."""
    latency_ewma: float = Field(..., description="EWMA-smoothed latency in ms")
    trend: str = Field(..., description="Latency trend over the last window")
    confidence: float = Field(..., description="Confidence in local state estimate")
    status: str = Field(..., description="Service health status: healthy/stressed/overloaded")


class ServiceStateIntent(BaseModel):
    """Local intent model representing traffic share estimates."""
    current_load: float = Field(..., description="Estimated current traffic share [0,1]")
    optimal_load: float = Field(..., description="Optimal traffic share [0,1] derived from EWMA latency")


class ServiceStateValue(BaseModel):
    """Value schema for `service.state` topic."""
    service: str = Field(..., description="Service identifier")
    cloud: CloudProvider = Field(..., description="Cloud provider")
    timestamp: datetime = Field(..., description="Event timestamp")
    belief: ServiceStateBelief = Field(..., description="Local belief about service health")
    intent: ServiceStateIntent = Field(..., description="Local traffic intent")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")
    parent_event_id: Optional[str] = Field(None, description="Parent event ID for chaining")

    class Config:
        use_enum_values = False


class ServiceState(BaseModel):
    """Complete service state event with key and value."""
    key: str = Field(..., description="Partition key (service_id)")
    value: ServiceStateValue


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

# ============================================================
# Topology Decisions Topic Schema
# ============================================================

class TopoAction(BaseModel):
    """Single redistribution action."""
    source: str = Field(..., description="Overloaded service (from)")
    target: str = Field(..., description="Underloaded service (to)")
    intensity: float = Field(..., ge=0.0, description="Flow intensity (0 → 1)")

    class Config:
        use_enum_values = False


class TopoDecisionValue(BaseModel):
    """Value schema for topo.decisions topic."""
    service: str = Field(..., description="Agent/service emitting decision")
    actions: List[TopoAction] = Field(
        ..., description="List of redistribution actions"
    )
    risk_level: RiskLevel = Field(..., description="Risk classification")
    status: PolicyStatus = Field(..., description="Decision status")
    timestamp: datetime = Field(..., description="Event timestamp")

    # Observability / traceability
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Solver metadata, parameters, diagnostics"
    )
    correlation_id: Optional[str] = Field(
        None, description="Trace correlation ID"
    )
    parent_event_id: Optional[str] = Field(
        None, description="Upstream event reference"
    )

    class Config:
        use_enum_values = False


class TopoDecision(BaseModel):
    """Complete topology decision event."""
    key: str = Field(..., description="Partition key (topo_decision_id)")
    value: TopoDecisionValue