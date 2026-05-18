"""HITL Rules - Risk assessment and approval rules."""

from enum import Enum
from typing import Dict, Any
from dataclasses import dataclass


class ApprovalAction(Enum):
    """Approval actions."""
    AUTO_APPROVE = "auto_approve"
    REQUIRE_APPROVAL = "require_approval"
    AUTO_REJECT = "auto_reject"


@dataclass
class RiskThresholds:
    """Risk thresholds for approval decisions."""
    auto_approve_threshold: float = 0.03  # KL divergence < 0.03 → auto approve
    require_approval_threshold: float = 0.10  # KL divergence >= 0.10 → require approval
    # Between 0.03 and 0.10 → require approval for safety


class ApprovalRules:
    """Rules engine for determining approval requirements."""

    def __init__(self, thresholds: RiskThresholds = None):
        self.thresholds = thresholds or RiskThresholds()

    def evaluate(self, decision: Dict[str, Any]) -> ApprovalAction:
        """Evaluate a policy decision and determine approval action.
        
        Args:
            decision: Policy decision dict with metadata
            
        Returns:
            ApprovalAction indicating what action to take
        """
        metadata = decision.get("metadata", {})
        kl_divergence = metadata.get("kl_divergence", 0.0)
        risk_level = decision.get("risk_level", "unknown")
        weights = metadata.get("weights", {})
        
        # Rule 1: Very low KL divergence → auto approve
        if kl_divergence < self.thresholds.auto_approve_threshold:
            return ApprovalAction.AUTO_APPROVE
        
        # Rule 2: High KL divergence → require approval
        if kl_divergence >= self.thresholds.require_approval_threshold:
            return ApprovalAction.REQUIRE_APPROVAL
        
        # Rule 3: Check for extreme weight changes
        if self._has_extreme_weight_change(weights):
            return ApprovalAction.REQUIRE_APPROVAL
        
        # Rule 4: Check for zero-weight CSPs (complete removal)
        if self._has_zero_weights(weights):
            return ApprovalAction.REQUIRE_APPROVAL
        
        # Default: Medium risk → require approval for safety
        return ApprovalAction.REQUIRE_APPROVAL

    def _has_extreme_weight_change(self, weights: Dict[str, int]) -> bool:
        """Check if any CSP has extreme weight (>80% or <5% but not 0)."""
        for csp, weight in weights.items():
            if weight > 80:
                return True
            if 0 < weight < 5:
                return True
        return False

    def _has_zero_weights(self, weights: Dict[str, int]) -> bool:
        """Check if any CSP has zero weight (complete removal)."""
        return any(weight == 0 for weight in weights.values())

    def get_risk_score(self, decision: Dict[str, Any]) -> float:
        """Calculate risk score (0-1) for a decision.
        
        Args:
            decision: Policy decision dict
            
        Returns:
            Risk score between 0 (low risk) and 1 (high risk)
        """
        metadata = decision.get("metadata", {})
        kl_divergence = metadata.get("kl_divergence", 0.0)
        
        # Normalize KL divergence to 0-1 scale
        # KL > 0.2 is considered very high risk
        risk_score = min(1.0, kl_divergence / 0.2)
        
        return risk_score
