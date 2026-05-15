"""
Governance Agent: KL-Based Decision Selection

Consumes topography decisions in fixed-size batches, selects based on KL divergence
from previous state, applies threshold gating, and publishes governance decisions.

CONSTRAINTS:
  ❌ NO confidence-based ranking (KL-only)
  ❌ NO merge/blend of plans (select exactly one)
  ❌ NO re-optimization (only KL + threshold gate)
  ❌ NO dynamic threshold (hardcoded or read once)
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import TopoDecision, PolicyDecision, PolicyDecisionValue
from kafka_core.enums import RiskLevel, PolicyStatus

from .governance_logic import (
    actions_to_weights,
    select_best_plan,
    to_percent_weights,
)
from .state_store import GovernanceStateStore

logger = logging.getLogger(__name__)

# Ordered list of all CSPs
CSP_SET = ["aws", "aks", "do"]

# Initial uniform distribution (used if no previous weights exist)
DEFAULT_WEIGHTS = {csp: 1.0 / len(CSP_SET) for csp in CSP_SET}


class GovernanceAgent(KafkaConsumerTemplate):
    """
    Governance Agent: KL-based topography decision selector.

    Consumes topo.decision messages, batches K candidates, selects by KL divergence,
    applies threshold gate, and publishes policy.decisions with final weights.
    """

    def __init__(
        self,
        candidate_window_k: int = 3,
        threshold: float = 0.03,
        group_id: str = "governance_agent",
        use_mongo: bool = True,
    ):
        """
        Initialize Governance Agent.

        Args:
            candidate_window_k: Number of candidates to collect per decision cycle
            threshold: KL threshold for applying new weights (typically 0.03)
            group_id: Kafka consumer group ID
            use_mongo: Whether to attempt MongoDB persistence (falls back if unavailable)
        """
        super().__init__(topics=["topo.decisions"], group_id=group_id)

        self.candidate_window_k = candidate_window_k
        self.threshold = threshold

        # Producer for policy.decision
        self.producer = KafkaProducerTemplate()

        # State store (Mongo primary + in-memory fallback)
        self.store = GovernanceStateStore.create(use_mongo=use_mongo)

        # Candidate accumulation (within a batch cycle)
        self.candidate_batch: List[Tuple[str, Dict[str, float]]] = []
        self.root_correlation_id = None
        self.root_timestamp = None

        logger.info(
            f"GovernanceAgent initialized: K={candidate_window_k}, threshold={threshold}, "
            f"group_id={group_id}, use_mongo={use_mongo}"
        )

    def process_message(self, topic: str, message: Dict) -> bool:
        """
        Process a single topography decision message from topo.decisions topic.

        Accumulates candidates; when batch reaches size K, performs selection and publishes decision.

        Args:
            topic: Topic name ("topo.decisions")
            message: TopoDecisionValue dict with fields: service, actions, risk_level, status, timestamp, metadata
                     - actions: List of TopoAction with source/target/intensity
                     - metadata: Dict that may contain solver info, correlation_id, etc.

        Returns:
            True to commit offset (after batch processing), False to nack
        """
        try:
            # Extract message fields from TopoDecisionValue
            service = message.get("service", "unknown")
            actions = message.get("actions", [])
            timestamp = message.get("timestamp")
            metadata = message.get("metadata", {})
            
            # Generate decision_id from service + timestamp (unique identifier)
            decision_id = f"topo-{service}-{int(timestamp.timestamp() * 1000) if hasattr(timestamp, 'timestamp') else 0}"
            
            # Extract correlation_id from metadata if present, otherwise generate one
            correlation_id = metadata.get("correlation_id") or f"cid-{service}-{int(time.time())}"

            # Store first message's correlation_id and timestamp for batch tracing
            if not self.candidate_batch:
                self.root_correlation_id = correlation_id
                self.root_timestamp = timestamp

            # Convert actions to weights
            # Actions are TopoAction with source/target fields, need to convert to from_csp/to_csp format
            converted_actions = [
                {
                    "from_csp": action.get("source") or action.get("from"),
                    "to_csp": action.get("target") or action.get("to"),
                    "intensity": action.get("intensity", 0.0),
                }
                for action in actions
            ]
            
            W_k = actions_to_weights(converted_actions, CSP_SET)

            # Accumulate candidate
            self.candidate_batch.append((decision_id, W_k))
            logger.debug(
                f"Accumulated candidate: decision_id={decision_id}, batch_size={len(self.candidate_batch)}/{self.candidate_window_k}"
            )

            # If batch full, process it
            if len(self.candidate_batch) >= self.candidate_window_k:
                self._process_batch()
                self.candidate_batch.clear()
                self.root_correlation_id = None
                self.root_timestamp = None

            return True

        except Exception as e:
            logger.error(f"Error processing topo decision: {e}", exc_info=True)
            return False

    def _process_batch(self) -> None:
        """
        Process accumulated batch of candidates.

        1. Fetch previous applied weights (or use default) for KL baseline
        2. Select best plan by KL divergence (deterministic, no confidence)
        3. Classify risk level based on KL divergence magnitude
        4. Convert to integer percentages (sum=100)
        5. Publish governance decision with weights and risk level
        
        NOTE: Execution layer decides whether to apply; governance only selects and classifies.
        """
        if not self.candidate_batch:
            logger.warning("_process_batch called with empty batch")
            return

        logger.info(f"Processing batch of {len(self.candidate_batch)} candidates")

        # Fetch previous applied weights (or use default) for KL baseline
        W_prev = self.store.get_previous_weights()
        if W_prev is None:
            W_prev = DEFAULT_WEIGHTS
            logger.info("No previous weights; using uniform distribution")

        # Select best plan by KL divergence (deterministic, no confidence)
        best_decision_id, W_best, D_best = select_best_plan(self.candidate_batch, W_prev)

        # Convert to integer percentages (deterministic, sum=100)
        percent_weights = to_percent_weights(W_best)

        # Classify risk level based on magnitude of KL divergence (D_best)
        # Independent of whether execution layer will apply it
        if D_best > 0.1:
            risk_level = RiskLevel.HIGH
        elif D_best > 0.05:
            risk_level = RiskLevel.MEDIUM
        else:
            risk_level = RiskLevel.LOW

        # Build decision description
        decision_desc = (
            f"Selected topography plan '{best_decision_id}' with weights {percent_weights}. "
            f"KL divergence: {D_best:.6f}. Execution layer decides application."
        )

        # Publish governance decision
        governance_decision_id = str(uuid.uuid4())
        governance_value = PolicyDecisionValue(
            service="governance_agent",
            decision=decision_desc,
            risk_level=risk_level,
            status=PolicyStatus.PENDING,  # Always PENDING; executor decides approval/rejection
            timestamp=datetime.now(timezone.utc),
            metadata={
                "weights": percent_weights,
                "kl_divergence": D_best,
                "threshold_used": self.threshold,  # For reference/audit
                "selected_topo_id": best_decision_id,
            },
            correlation_id=self.root_correlation_id,
            parent_event_id=best_decision_id,
        )

        governance_event = PolicyDecision(
            key=governance_decision_id,
            value=governance_value,
        )

        self.producer.send("policy.decisions", governance_event)

        logger.info(
            f"Published governance decision: governance_id={governance_decision_id}, "
            f"selected_topo={best_decision_id}, D_best={D_best:.6f}, risk_level={risk_level.value}, "
            f"weights={percent_weights}"
        )

    def close(self) -> None:
        """Graceful shutdown."""
        logger.info("Closing Governance Agent")
        if self.producer:
            self.producer.close()
        if self.store and hasattr(self.store, "close"):
            self.store.close()
        super().close()
