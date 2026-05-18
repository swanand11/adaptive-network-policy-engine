"""State Cache - In-memory cache for system state

Maintains current state of:
- CSP health status
- Recent metrics
- Recent decisions
- System statistics
"""

import logging
from typing import Dict, List, Optional
from datetime import datetime
from collections import deque
import threading

logger = logging.getLogger(__name__)


class StateCache:
    """Thread-safe in-memory cache for system state."""

    def __init__(self, max_history: int = 100):
        """Initialize state cache.
        
        Args:
            max_history: Maximum number of historical items to keep
        """
        self.max_history = max_history
        self._lock = threading.RLock()
        
        # CSP health status
        self.csp_health: Dict[str, Dict] = {
            "aws": {"status": "unknown", "last_update": None},
            "azure": {"status": "unknown", "last_update": None},
            "aks": {"status": "unknown", "last_update": None},
            "digitalocean": {"status": "unknown", "last_update": None},
        }
        
        # Recent metrics
        self.metrics_history: Dict[str, deque] = {
            "aws": deque(maxlen=max_history),
            "azure": deque(maxlen=max_history),
            "aks": deque(maxlen=max_history),
            "digitalocean": deque(maxlen=max_history),
        }
        
        # Recent decisions
        self.decisions_history: deque = deque(maxlen=max_history)
        
        # Recent approvals
        self.approvals_history: deque = deque(maxlen=max_history)
        
        # System statistics
        self.stats = {
            "total_requests": 0,
            "total_decisions": 0,
            "total_approvals": 0,
            "total_rejections": 0,
            "start_time": datetime.now().isoformat(),
        }
        
        # Current weights
        self.current_weights: Dict[str, int] = {
            "aws": 33,
            "azure": 33,
            "digitalocean": 34,
        }
        
        logger.info("StateCache initialized")

    def update_csp_health(self, csp: str, status: str, metrics: Optional[Dict] = None) -> None:
        """Update CSP health status.
        
        Args:
            csp: CSP name (aws, azure, digitalocean)
            status: Health status (healthy, degraded, unhealthy)
            metrics: Optional metrics dict
        """
        with self._lock:
            self.csp_health[csp] = {
                "status": status,
                "last_update": datetime.now().isoformat(),
                "metrics": metrics or {},
            }
            logger.debug(f"Updated {csp} health: {status}")

    def add_metrics(self, csp: str, metrics: Dict) -> None:
        """Add metrics for a CSP.
        
        Args:
            csp: CSP name
            metrics: Metrics dict
        """
        with self._lock:
            metrics["timestamp"] = datetime.now().isoformat()
            self.metrics_history[csp].append(metrics)
            logger.debug(f"Added metrics for {csp}")

    def add_decision(self, decision: Dict) -> None:
        """Add a policy decision.
        
        Args:
            decision: Decision dict
        """
        with self._lock:
            decision["timestamp"] = decision.get("timestamp") or datetime.now().isoformat()
            self.decisions_history.append(decision)
            self.stats["total_decisions"] += 1
            logger.debug(f"Added decision: {decision.get('decision_id')}")

    def add_approval(self, approval: Dict) -> None:
        """Add an approval/rejection.
        
        Args:
            approval: Approval dict
        """
        with self._lock:
            approval["timestamp"] = approval.get("timestamp") or datetime.now().isoformat()
            self.approvals_history.append(approval)
            
            if approval.get("status") == "approved":
                self.stats["total_approvals"] += 1
            elif approval.get("status") == "rejected":
                self.stats["total_rejections"] += 1
            
            logger.debug(f"Added approval: {approval.get('approval_id')}")

    def update_weights(self, weights: Dict[str, int]) -> None:
        """Update current weights.
        
        Args:
            weights: Weights dict
        """
        with self._lock:
            self.current_weights = weights.copy()
            logger.debug(f"Updated weights: {weights}")

    def get_csp_health(self) -> Dict:
        """Get all CSP health status.
        
        Returns:
            Dict of CSP health status
        """
        with self._lock:
            return self.csp_health.copy()

    def get_recent_metrics(self, csp: Optional[str] = None, limit: int = 50) -> Dict:
        """Get recent metrics.
        
        Args:
            csp: Optional CSP name to filter
            limit: Maximum number of items
            
        Returns:
            Dict of metrics by CSP
        """
        with self._lock:
            if csp:
                return {csp: list(self.metrics_history[csp])[-limit:]}
            
            return {
                csp_name: list(history)[-limit:]
                for csp_name, history in self.metrics_history.items()
            }

    def get_recent_decisions(self, limit: int = 50) -> List[Dict]:
        """Get recent decisions.
        
        Args:
            limit: Maximum number of items
            
        Returns:
            List of recent decisions
        """
        with self._lock:
            return list(self.decisions_history)[-limit:]

    def get_recent_approvals(self, limit: int = 50) -> List[Dict]:
        """Get recent approvals.
        
        Args:
            limit: Maximum number of items
            
        Returns:
            List of recent approvals
        """
        with self._lock:
            return list(self.approvals_history)[-limit:]

    def get_current_weights(self) -> Dict[str, int]:
        """Get current weights.
        
        Returns:
            Current weights dict
        """
        with self._lock:
            return self.current_weights.copy()

    def get_stats(self) -> Dict:
        """Get system statistics.
        
        Returns:
            Statistics dict
        """
        with self._lock:
            stats = self.stats.copy()
            stats["uptime_seconds"] = (
                datetime.now() - datetime.fromisoformat(stats["start_time"])
            ).total_seconds()
            return stats

    def increment_requests(self) -> None:
        """Increment total requests counter."""
        with self._lock:
            self.stats["total_requests"] += 1
