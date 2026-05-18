"""Approval Engine - Manage approval workflow and state."""

import logging
import uuid
from typing import Dict, List, Optional, Any
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field, asdict

from .rules import ApprovalRules, ApprovalAction

logger = logging.getLogger(__name__)


class ApprovalStatus(Enum):
    """Approval status states."""
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"
    EXPIRED = "expired"
    AUTO_APPROVED = "auto_approved"


@dataclass
class ApprovalRequest:
    """Approval request data structure."""
    id: str
    decision_id: str
    decision: Dict[str, Any]
    risk_score: float
    status: ApprovalStatus = ApprovalStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    approved_by: Optional[str] = None
    approval_note: Optional[str] = None
    expires_at: Optional[datetime] = None

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        data = asdict(self)
        data['status'] = self.status.value
        data['created_at'] = self.created_at.isoformat()
        data['updated_at'] = self.updated_at.isoformat()
        if self.expires_at:
            data['expires_at'] = self.expires_at.isoformat()
        return data


class ApprovalEngine:
    """Manage approval workflow and pending requests."""

    def __init__(self):
        self.rules = ApprovalRules()
        self.pending_requests: Dict[str, ApprovalRequest] = {}
        self.completed_requests: Dict[str, ApprovalRequest] = {}
        logger.info("ApprovalEngine initialized")

    def process_decision(self, decision: Dict[str, Any]) -> tuple[ApprovalAction, Optional[str]]:
        """Process a policy decision and determine if approval is needed.
        
        Args:
            decision: Policy decision dict
            
        Returns:
            Tuple of (ApprovalAction, approval_request_id or None)
        """
        # Evaluate decision against rules
        action = self.rules.evaluate(decision)
        
        if action == ApprovalAction.AUTO_APPROVE:
            logger.info(f"Decision auto-approved: {decision.get('decision_id')}")
            return (action, None)
        
        if action == ApprovalAction.AUTO_REJECT:
            logger.info(f"Decision auto-rejected: {decision.get('decision_id')}")
            return (action, None)
        
        # Create approval request
        risk_score = self.rules.get_risk_score(decision)
        approval_id = str(uuid.uuid4())
        
        request = ApprovalRequest(
            id=approval_id,
            decision_id=decision.get("decision_id", "unknown"),
            decision=decision,
            risk_score=risk_score,
            status=ApprovalStatus.PENDING
        )
        
        self.pending_requests[approval_id] = request
        
        logger.info(
            f"Created approval request: id={approval_id}, "
            f"decision_id={request.decision_id}, risk_score={risk_score:.3f}"
        )
        
        return (action, approval_id)

    def approve(
        self,
        approval_id: str,
        approved_by: str,
        note: Optional[str] = None,
        override_weights: Optional[Dict[str, int]] = None
    ) -> bool:
        """Approve a pending request.
        
        Args:
            approval_id: Approval request ID
            approved_by: User who approved
            note: Optional approval note
            override_weights: Optional weight overrides
            
        Returns:
            True if approved successfully, False otherwise
        """
        if approval_id not in self.pending_requests:
            logger.error(f"Approval request not found: {approval_id}")
            return False
        
        request = self.pending_requests[approval_id]
        request.status = ApprovalStatus.APPROVED
        request.approved_by = approved_by
        request.approval_note = note
        request.updated_at = datetime.now()
        
        # Apply weight overrides if provided
        if override_weights:
            request.decision["metadata"]["weights"] = override_weights
            logger.info(f"Applied weight overrides: {override_weights}")
        
        # Move to completed
        self.completed_requests[approval_id] = request
        del self.pending_requests[approval_id]
        
        logger.info(
            f"Approved request: id={approval_id}, by={approved_by}, "
            f"note={note or 'N/A'}"
        )
        
        return True

    def reject(
        self,
        approval_id: str,
        rejected_by: str,
        note: Optional[str] = None
    ) -> bool:
        """Reject a pending request.
        
        Args:
            approval_id: Approval request ID
            rejected_by: User who rejected
            note: Optional rejection note
            
        Returns:
            True if rejected successfully, False otherwise
        """
        if approval_id not in self.pending_requests:
            logger.error(f"Approval request not found: {approval_id}")
            return False
        
        request = self.pending_requests[approval_id]
        request.status = ApprovalStatus.REJECTED
        request.approved_by = rejected_by
        request.approval_note = note
        request.updated_at = datetime.now()
        
        # Move to completed
        self.completed_requests[approval_id] = request
        del self.pending_requests[approval_id]
        
        logger.info(
            f"Rejected request: id={approval_id}, by={rejected_by}, "
            f"note={note or 'N/A'}"
        )
        
        return True

    def get_pending_requests(self) -> List[Dict]:
        """Get all pending approval requests.
        
        Returns:
            List of pending request dicts
        """
        return [req.to_dict() for req in self.pending_requests.values()]

    def get_request(self, approval_id: str) -> Optional[Dict]:
        """Get a specific approval request.
        
        Args:
            approval_id: Approval request ID
            
        Returns:
            Request dict or None if not found
        """
        request = self.pending_requests.get(approval_id) or self.completed_requests.get(approval_id)
        return request.to_dict() if request else None

    def get_completed_requests(self, limit: int = 100) -> List[Dict]:
        """Get completed approval requests.
        
        Args:
            limit: Maximum number of requests to return
            
        Returns:
            List of completed request dicts
        """
        requests = sorted(
            self.completed_requests.values(),
            key=lambda r: r.updated_at,
            reverse=True
        )
        return [req.to_dict() for req in requests[:limit]]

    def cleanup_expired(self, max_age_hours: int = 24) -> int:
        """Clean up old completed requests.
        
        Args:
            max_age_hours: Maximum age in hours
            
        Returns:
            Number of requests cleaned up
        """
        from datetime import timedelta
        
        cutoff = datetime.now() - timedelta(hours=max_age_hours)
        to_remove = [
            req_id for req_id, req in self.completed_requests.items()
            if req.updated_at < cutoff
        ]
        
        for req_id in to_remove:
            del self.completed_requests[req_id]
        
        if to_remove:
            logger.info(f"Cleaned up {len(to_remove)} expired requests")
        
        return len(to_remove)
