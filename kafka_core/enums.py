"""Kafka Enums Module

PURPOSE:
  Provides type-safe enumerations for all Kafka event fields.
  Ensures consistent values across producers and consumers.

ENUMS DEFINED:
  - CloudProvider: aws, azure, gcp (cloud sources for metrics)
  - PolicyStatus: pending, approved, rejected (decision status)
  - ExecutionStatus: success, failed, in_progress (execution result)
  - RiskLevel: low, medium, high, critical (decision risk classification)

USAGE:
  from kafka.enums import CloudProvider, PolicyStatus, ExecutionStatus, RiskLevel
  
  # In schemas/events
  cloud = CloudProvider.AWS
  status = PolicyStatus.PENDING
  exec_status = ExecutionStatus.SUCCESS
  risk = RiskLevel.HIGH
  
  # Type hints
  def process_metric(cloud: CloudProvider):
      if cloud == CloudProvider.AWS:
          print("Processing AWS metrics")

BENEFITS:
  - No string typos (enum validation)
  - IDE autocomplete
  - Type checking with mypy
  - Clear constant values across codebase

EXTENSION:
  Add new enums here if you need to classify new event fields.
"""

from enum import Enum


class CloudProvider(str, Enum):
    """Cloud provider enumeration."""
    AWS = "aws"
    AZURE = "azure"
    DIGITALOCEAN = "digitalocean"


class PolicyStatus(str, Enum):
    """Policy decision status."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ExecutionStatus(str, Enum):
    """Execution status."""
    SUCCESS = "success"
    FAILED = "failed"
    IN_PROGRESS = "in_progress"


class RiskLevel(str, Enum):
    """Risk level classification."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
