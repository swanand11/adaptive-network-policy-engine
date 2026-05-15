# Plan: Governance KL Selection Agent

Implement a deterministic Governance Agent that consumes fixed-size batches from topo.decision, converts each candidate to weights, computes KL divergence against previous state, picks argmin, applies only when D_best > 0.03, and publishes policy.decision with integer weights summing to 100. Persistence: MongoDB primary + in-memory fallback.

---

## 1. Contract And Topic Wiring

### 1.1 Extend Kafka Schemas
**File**: `kafka_core/schemas.py`

Add new Pydantic models for topography input and governance output:

```python
class TopographyAction(BaseModel):
    """Single traffic action from topography decision."""
    from_csp: str = Field(..., description="Source CSP (aws, gcp, azure, do)")
    to_csp: str = Field(..., description="Target CSP")
    intensity: float = Field(..., description="Traffic intensity [0, 1]")

class TopographyDecisionValue(BaseModel):
    """Value schema for topo.decision topic (input to Governance)."""
    actions: List[TopographyAction]
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="solver, iteration, global_state_size, confidence, temperature, alpha, beta, gamma"
    )
    timestamp: datetime
    correlation_id: Optional[str] = None

class TopographyDecision(BaseModel):
    """Complete topography decision with key and value."""
    key: str = Field(..., description="Partition key (decision_id)")
    value: TopographyDecisionValue

class GovernanceDecisionValue(BaseModel):
    """Value schema for policy.decision topic (output from Governance)."""
    decision_id: str = Field(..., description="Unique decision ID")
    correlation_id: Optional[str] = None
    timestamp: datetime
    weights: Dict[str, int] = Field(..., description="CSP weights as integer percentages, sum=100")
    kl_divergence: float = Field(..., description="KL divergence of selected plan vs previous")
    threshold: float = Field(..., description="Threshold used for gating (0.03)")
    applied: bool = Field(..., description="Whether weights were applied (D_best > threshold)")

class GovernanceDecision(BaseModel):
    """Complete governance decision with key and value."""
    key: str = Field(..., description="Partition key (decision_id)")
    value: GovernanceDecisionValue
```

---

### 1.2 Register Topics in Config
**File**: `kafka_core/config.py`

Add topic configurations to `KafkaConfig.TOPICS`:

```python
"topo.decision": TopicConfig(
    name="topo.decision",
    partitions=2,
    retention_ms=7 * 24 * 60 * 60 * 1000,
    partition_key_field="decision_id",
),
"policy.decision": TopicConfig(
    name="policy.decision",
    partitions=2,
    retention_ms=7 * 24 * 60 * 60 * 1000,
    partition_key_field="decision_id",
),
```

---

### 1.3 Topics Auto-Created
**File**: `kafka_core/topic_initializer.py`

No changes needed; the existing `create_all_topics()` loop iterates `KafkaConfig.TOPICS` automatically.

---

## 2. Governance Core Logic (Pure Deterministic Functions)

### 2.1 Governance Logic Module
**File**: `agents/governance_agent/governance_logic.py` (new)

Pure, stateless functions for KL selection:

```python
"""
Governance Logic: KL-Based Selection

Pure deterministic functions for converting topography candidates to weights,
computing KL divergence, and applying threshold-based selection.
"""

import logging
from typing import Dict, List, Tuple, Optional
import math

logger = logging.getLogger(__name__)

EPSILON = 1e-6
MIN_WEIGHT = 0.05
THRESHOLD = 0.03


def actions_to_weights(
    actions: List[Dict[str, any]],
    csp_set: List[str],
) -> Dict[str, float]:
    """
    Convert topography actions to normalized CSP weights.

    Algorithm:
    1. Compute net flow per CSP: net_i = incoming_i - outgoing_i
    2. Base weight: base_i = 1 / N
    3. Adjusted weight: W_i = base_i + net_i
    4. Clamp: W_i >= 0.05
    5. Normalize: sum(W_i) = 1.0

    Args:
        actions: List of {"from": csp, "to": csp, "intensity": float}
        csp_set: List of all CSP names (aws, gcp, azure, do)

    Returns:
        Dict[csp_name] -> normalized weight [0, 1]
    """
    # Initialize incoming/outgoing flows
    incoming = {csp: 0.0 for csp in csp_set}
    outgoing = {csp: 0.0 for csp in csp_set}

    for action in actions:
        from_csp = action.get("from")
        to_csp = action.get("to")
        intensity = action.get("intensity", 0.0)

        if from_csp in outgoing:
            outgoing[from_csp] += intensity
        if to_csp in incoming:
            incoming[to_csp] += intensity

    # Compute net flow per CSP
    net = {csp: incoming[csp] - outgoing[csp] for csp in csp_set}

    # Base weight
    n = len(csp_set)
    base = 1.0 / n

    # Adjusted weight
    weights = {csp: base + net[csp] for csp in csp_set}

    # Clamp to minimum
    weights = {csp: max(weights[csp], MIN_WEIGHT) for csp in csp_set}

    # Normalize
    total = sum(weights.values())
    if total <= 0:
        # Fallback: uniform distribution
        weights = {csp: 1.0 / n for csp in csp_set}
    else:
        weights = {csp: weights[csp] / total for csp in csp_set}

    logger.debug(f"Converted actions to weights: {weights}")
    return weights


def safe_kl_divergence(
    W_k: Dict[str, float],
    W_prev: Dict[str, float],
) -> float:
    """
    Compute KL divergence from W_prev to W_k with epsilon safety.

    Formula: D_k = sum(q_i * log(q_i / p_i))
    where p_i = max(W_prev_i, epsilon), q_i = max(W_k_i, epsilon)

    Args:
        W_k: Candidate weights from topography decision
        W_prev: Previous applied weights

    Returns:
        KL divergence (non-negative float)
    """
    d_kl = 0.0

    for csp in W_k.keys():
        p_i = max(W_prev.get(csp, 1.0 / len(W_k)), EPSILON)
        q_i = max(W_k[csp], EPSILON)

        d_kl += q_i * math.log(q_i / p_i)

    logger.debug(f"KL divergence: {d_kl:.6f}")
    return d_kl


def select_best_plan(
    candidates: List[Tuple[str, Dict[str, float]]],
    W_prev: Dict[str, float],
) -> Tuple[str, Dict[str, float], float]:
    """
    Select candidate with lowest KL divergence (deterministic).

    Deterministic tie-breaker: lexicographic order on decision_id.

    Args:
        candidates: List of (decision_id, W_k) tuples
        W_prev: Previous applied weights

    Returns:
        (best_decision_id, best_weights, D_best)
    """
    best_decision_id = None
    best_weights = None
    best_kl = float('inf')

    for decision_id, W_k in candidates:
        D_k = safe_kl_divergence(W_k, W_prev)

        # Deterministic selection: lower D_k, or same D_k with lexicographically smaller decision_id
        if D_k < best_kl or (D_k == best_kl and (best_decision_id is None or decision_id < best_decision_id)):
            best_kl = D_k
            best_decision_id = decision_id
            best_weights = W_k

    logger.info(f"Selected best plan: decision_id={best_decision_id}, D_best={best_kl:.6f}")
    return best_decision_id, best_weights, best_kl


def apply_threshold(
    W_best: Dict[str, float],
    W_prev: Dict[str, float],
    D_best: float,
    threshold: float = THRESHOLD,
) -> Tuple[Dict[str, float], bool]:
    """
    Apply threshold gate: if D_best > threshold, use W_best; else use W_prev.

    Args:
        W_best: Candidate weights with lowest KL
        W_prev: Previous applied weights
        D_best: KL divergence of W_best from W_prev
        threshold: Threshold for applying new weights (default 0.03)

    Returns:
        (W_final, applied) where applied=True if W_best used, False if W_prev kept
    """
    applied = D_best > threshold

    if applied:
        W_final = W_best
        logger.info(f"Applied new weights: D_best={D_best:.6f} > threshold={threshold}")
    else:
        W_final = W_prev
        logger.info(f"Ignored new weights: D_best={D_best:.6f} <= threshold={threshold}")

    return W_final, applied


def to_percent_weights(W_final: Dict[str, float]) -> Dict[str, int]:
    """
    Convert normalized weights [0, 1] to integer percentages summing to exactly 100.

    Deterministic rounding: largest-remainder method ensures sum=100.

    Args:
        W_final: Normalized weights

    Returns:
        Dict[csp_name] -> integer percentage
    """
    csps = sorted(W_final.keys())
    fractions = [W_final[csp] * 100 for csp in csps]

    # Floor each fraction
    integers = [int(f) for f in fractions]
    remainders = [f - int(f) for f in fractions]

    # Distribute remainder to largest-remainder CSPs
    remainder_sum = 100 - sum(integers)
    remainder_indices = sorted(range(len(remainders)), key=lambda i: remainders[i], reverse=True)[:int(remainder_sum)]

    for i in remainder_indices:
        integers[i] += 1

    result = {csps[i]: integers[i] for i in range(len(csps))}
    logger.info(f"Converted to percent weights: {result}, sum={sum(result.values())}")
    return result
```

---

## 3. State Persistence (MongoDB + Fallback)

### 3.1 Governance State Store
**File**: `agents/governance_agent/state_store.py` (new)

```python
"""
Governance State Store: MongoDB Primary + In-Memory Fallback

Persists governance decisions (previous applied weights) for future KL computations.
Falls back to in-memory cache if MongoDB unavailable.
"""

import logging
import os
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

# Try to import pymongo; if unavailable, fallback is used
try:
    from pymongo import MongoClient
    HAS_PYMONGO = True
except ImportError:
    HAS_PYMONGO = False
    logger.warning("pymongo not available; governance store will use in-memory fallback")


class InMemoryStore:
    """Simple in-memory store for governance state."""

    def __init__(self):
        self.state = {
            "weights": None,
            "timestamp": None,
            "decision_id": None,
        }

    def get_previous_weights(self) -> Optional[Dict[str, float]]:
        """Get last applied weights or None."""
        return self.state.get("weights")

    def save_applied_weights(
        self,
        weights: Dict[str, float],
        decision_id: str,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Save applied weights."""
        self.state["weights"] = weights
        self.state["decision_id"] = decision_id
        self.state["correlation_id"] = correlation_id
        self.state["timestamp"] = datetime.utcnow()
        logger.debug(f"Saved weights to in-memory store: decision_id={decision_id}")


class MongoStore:
    """MongoDB-backed store for governance state."""

    def __init__(self, mongo_uri: str, db_name: str = "governance", collection_name: str = "state"):
        try:
            self.client = MongoClient(mongo_uri, connectTimeoutMS=5000, serverSelectionTimeoutMS=5000)
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
            # Verify connection
            self.client.server_info()
            logger.info(f"Connected to MongoDB: {mongo_uri}")
            self.fallback_store = InMemoryStore()
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            self.client = None
            self.collection = None
            self.fallback_store = InMemoryStore()

    def get_previous_weights(self) -> Optional[Dict[str, float]]:
        """Get last applied weights from MongoDB or fallback."""
        if not self.collection:
            return self.fallback_store.get_previous_weights()

        try:
            doc = self.collection.find_one(sort=[("timestamp", -1)])
            if doc:
                return doc.get("weights")
            return None
        except Exception as e:
            logger.error(f"Error reading from MongoDB: {e}; using fallback")
            return self.fallback_store.get_previous_weights()

    def save_applied_weights(
        self,
        weights: Dict[str, float],
        decision_id: str,
        correlation_id: Optional[str] = None,
    ) -> None:
        """Save applied weights to MongoDB and fallback."""
        doc = {
            "decision_id": decision_id,
            "correlation_id": correlation_id,
            "weights": weights,
            "timestamp": datetime.utcnow(),
        }

        if self.collection:
            try:
                self.collection.insert_one(doc)
                logger.debug(f"Saved weights to MongoDB: decision_id={decision_id}")
            except Exception as e:
                logger.error(f"Error writing to MongoDB: {e}; using fallback")
                self.fallback_store.save_applied_weights(weights, decision_id, correlation_id)
        else:
            self.fallback_store.save_applied_weights(weights, decision_id, correlation_id)

    def close(self) -> None:
        """Close MongoDB connection."""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")


class GovernanceStateStore:
    """Factory for creating appropriate store based on configuration."""

    @staticmethod
    def create(use_mongo: bool = True) -> Optional[MongoStore] or InMemoryStore:
        """
        Create store instance.

        Args:
            use_mongo: If True, try MongoDB; fall back if unavailable. If False, use in-memory.

        Returns:
            MongoStore or InMemoryStore
        """
        if use_mongo and HAS_PYMONGO:
            mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
            return MongoStore(mongo_uri)
        else:
            logger.info("Using in-memory governance state store")
            return InMemoryStore()
```

---

### 3.2 Add PyMongo Dependency
**File**: `requirements.txt`

Add line:
```
pymongo>=4.0.0
```

---

## 4. Governance Agent And Runner

### 4.1 Governance Agent
**File**: `agents/governance_agent/governance_agent.py` (new)

```python
"""
Governance Agent: KL-Based Decision Selection

Consumes fixed-size batches of topography decisions, selects based on KL divergence
from previous state, and applies threshold gating. Publishes governance decisions.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import TopographyDecisionValue, GovernanceDecisionValue

from .governance_logic import (
    actions_to_weights,
    select_best_plan,
    apply_threshold,
    to_percent_weights,
)
from .state_store import GovernanceStateStore

logger = logging.getLogger(__name__)

CSP_SET = ["aws", "gcp", "azure", "do"]  # Ordered list of all CSPs
DEFAULT_WEIGHTS = {csp: 25 for csp in CSP_SET}  # Initial uniform distribution


class GovernanceAgent(KafkaConsumerTemplate):
    """
    Governance Agent: KL-based topography decision selector.

    Consumes topo.decision, batches K candidates, selects by KL divergence,
    applies threshold gate, publishes policy.decision.
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
            threshold: KL threshold for applying new weights
            group_id: Kafka consumer group ID
            use_mongo: Whether to use MongoDB for persistence
        """
        super().__init__(topics=["topo.decision"], group_id=group_id)

        self.candidate_window_k = candidate_window_k
        self.threshold = threshold

        # Producer for policy.decision
        self.producer = KafkaProducerTemplate()

        # State store (Mongo + fallback)
        self.store = GovernanceStateStore.create(use_mongo=use_mongo)

        # Candidate accumulation
        self.candidate_batch: List[Tuple[str, Dict[str, float]]] = []

        logger.info(
            f"GovernanceAgent initialized: K={candidate_window_k}, threshold={threshold}, group_id={group_id}"
        )

    def process_message(self, topic: str, message: Dict) -> bool:
        """
        Process a single topography decision.

        Accumulates candidates; when batch is full, performs selection and publishes decision.

        Args:
            topic: Topic name (topo.decision)
            message: TopographyDecisionValue dict

        Returns:
            True to commit offset (after batch processing)
        """
        try:
            # Parse message
            decision_id = message.get("decision_id")
            actions = message.get("actions", [])
            timestamp = message.get("timestamp")
            correlation_id = message.get("correlation_id")

            # Convert actions to weights
            W_k = actions_to_weights(actions, CSP_SET)

            # Accumulate candidate
            self.candidate_batch.append((decision_id, W_k))
            logger.debug(f"Added candidate to batch: decision_id={decision_id}, batch_size={len(self.candidate_batch)}")

            # If batch full, process it
            if len(self.candidate_batch) >= self.candidate_window_k:
                self._process_batch(correlation_id, timestamp)
                self.candidate_batch.clear()

            return True

        except Exception as e:
            logger.error(f"Error processing topo decision: {e}", exc_info=True)
            return False

    def _process_batch(self, root_correlation_id: Optional[str], root_timestamp: Optional[str]) -> None:
        """
        Process accumulated batch of candidates.

        Selects best plan by KL, applies threshold, persists state, publishes governance decision.

        Args:
            root_correlation_id: Correlation ID from first message in batch (for tracing)
            root_timestamp: Timestamp from first message
        """
        if not self.candidate_batch:
            return

        # Get previous applied weights (or default)
        W_prev = self.store.get_previous_weights()
        if W_prev is None:
            W_prev = {csp: 1.0 / len(CSP_SET) for csp in CSP_SET}
            logger.info("No previous weights; using uniform distribution")

        # Select best plan by KL divergence
        best_decision_id, W_best, D_best = select_best_plan(self.candidate_batch, W_prev)

        # Apply threshold
        W_final, applied = apply_threshold(W_best, W_prev, D_best, threshold=self.threshold)

        # Convert to percent weights
        percent_weights = to_percent_weights(W_final)

        # Save applied weights
        if applied:
            self.store.save_applied_weights(W_final, best_decision_id, root_correlation_id)

        # Publish governance decision
        governance_decision_id = str(uuid.uuid4())
        governance_value = GovernanceDecisionValue(
            decision_id=governance_decision_id,
            correlation_id=root_correlation_id,
            timestamp=datetime.now(timezone.utc),
            weights=percent_weights,
            kl_divergence=D_best,
            threshold=self.threshold,
            applied=applied,
        )

        from kafka_core.schemas import GovernanceDecision
        governance_event = GovernanceDecision(
            key=governance_decision_id,
            value=governance_value,
        )

        self.producer.send("policy.decision", governance_event)

        logger.info(
            f"Published governance decision: decision_id={governance_decision_id}, "
            f"selected_topo={best_decision_id}, D_best={D_best:.6f}, applied={applied}"
        )

    def close(self) -> None:
        """Graceful shutdown."""
        if self.producer:
            self.producer.close()
        if self.store and hasattr(self.store, "close"):
            self.store.close()
        super().close()
```

---

### 4.2 Governance Runner
**File**: `runners/governance_agent_runner.py` (new)

Pattern mirrors [runners/service_agent_runner.py](runners/service_agent_runner.py); simplified for single governance agent instance:

```python
"""
Governance Agent Runner

Starts a single GovernanceAgent instance consuming from topo.decision
and publishing to policy.decision.
"""

import logging
import os
import signal

from agents.governance_agent.governance_agent import GovernanceAgent
from kafka_core.topic_initializer import TopicInitializer

logger = logging.getLogger(__name__)


class GovernanceAgentRunner:
    """Runner for Governance Agent."""

    def __init__(
        self,
        candidate_window_k: int = 3,
        threshold: float = 0.03,
        use_mongo: bool = True,
    ):
        self.agent = GovernanceAgent(
            candidate_window_k=candidate_window_k,
            threshold=threshold,
            use_mongo=use_mongo,
        )
        self._shutdown_event = False

    def _signal_handler(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown")
        self._shutdown_event = True

    def start(self) -> None:
        """Start the governance agent."""
        try:
            # Initialize topics
            logger.info("Initializing Kafka topics")
            initializer = TopicInitializer()
            initializer.create_all_topics()

            # Setup signal handlers
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)

            logger.info("Starting Governance Agent")
            self.agent.start()

        except KeyboardInterrupt:
            logger.info("Interrupted")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            raise
        finally:
            self.agent.close()


def main():
    """Entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    candidate_window_k = int(os.getenv("GOVERNANCE_WINDOW_K", "3"))
    threshold = float(os.getenv("GOVERNANCE_THRESHOLD", "0.03"))
    use_mongo = os.getenv("GOVERNANCE_USE_MONGO", "true").lower() == "true"

    runner = GovernanceAgentRunner(
        candidate_window_k=candidate_window_k,
        threshold=threshold,
        use_mongo=use_mongo,
    )
    runner.start()


if __name__ == "__main__":
    main()
```

---

### 4.3 Optional Configuration File
**File**: `config/governance.yml` (new, optional)

```yaml
# Governance Agent Configuration

group_id: "governance_agent"
candidate_window_k: 3
threshold: 0.03
min_weight: 0.05
epsilon: 1e-6
output_topic: "policy.decision"

persistence:
  use_mongo: true
  mongo_db: "governance"
  mongo_collection: "state"
```

---

## 5. Required Tests

### 5.1 Governance Logic Tests
**File**: `tests/unit/test_governance_logic.py` (new)

```python
"""Unit tests for governance KL selection logic."""

import pytest
from agents.governance_agent.governance_logic import (
    actions_to_weights,
    safe_kl_divergence,
    select_best_plan,
    apply_threshold,
    to_percent_weights,
)

CSP_SET = ["aws", "gcp", "azure", "do"]


def test_actions_to_weights_basic():
    """Convert actions to normalized weights."""
    actions = [
        {"from": "aws", "to": "gcp", "intensity": 0.1},
        {"from": "gcp", "to": "azure", "intensity": 0.05},
    ]
    weights = actions_to_weights(actions, CSP_SET)
    assert sum(weights.values()) == pytest.approx(1.0)
    assert all(w >= 0.05 for w in weights.values())


def test_safe_kl_divergence_identical():
    """KL divergence of identical distributions is zero."""
    W = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
    d = safe_kl_divergence(W, W)
    assert d == pytest.approx(0.0, abs=1e-5)


def test_safe_kl_divergence_different():
    """KL divergence detects differences."""
    W_prev = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
    W_k = {"aws": 0.25, "gcp": 0.5, "azure": 0.15, "do": 0.1}
    d = safe_kl_divergence(W_k, W_prev)
    assert d > 0


def test_select_best_plan_multiple():
    """Multiple plans selects lowest KL."""
    W_prev = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
    candidates = [
        ("plan1", {"aws": 0.26, "gcp": 0.25, "azure": 0.25, "do": 0.24}),
        ("plan2", {"aws": 0.50, "gcp": 0.25, "azure": 0.15, "do": 0.10}),
        ("plan3", {"aws": 0.25, "gcp": 0.26, "azure": 0.25, "do": 0.24}),
    ]
    best_id, best_w, d_best = select_best_plan(candidates, W_prev)
    assert best_id in ["plan1", "plan3"]  # Both have lower KL than plan2
    assert d_best < safe_kl_divergence(candidates[1][1], W_prev)


def test_apply_threshold_above():
    """D_best > threshold applies new weights."""
    W_prev = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
    W_best = {"aws": 0.25, "gcp": 0.5, "azure": 0.15, "do": 0.1}
    D_best = 0.1
    threshold = 0.03

    W_final, applied = apply_threshold(W_best, W_prev, D_best, threshold)
    assert applied
    assert W_final == W_best


def test_apply_threshold_below():
    """D_best <= threshold ignores new weights."""
    W_prev = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
    W_best = {"aws": 0.49, "gcp": 0.26, "azure": 0.15, "do": 0.10}
    D_best = 0.01
    threshold = 0.03

    W_final, applied = apply_threshold(W_best, W_prev, D_best, threshold)
    assert not applied
    assert W_final == W_prev


def test_to_percent_weights_sums_100():
    """Percent weights sum to exactly 100."""
    W_final = {"aws": 0.42, "gcp": 0.33, "azure": 0.15, "do": 0.10}
    percent = to_percent_weights(W_final)
    assert sum(percent.values()) == 100
    assert all(v > 0 for v in percent.values())


def test_zero_safe_kl_handling():
    """KL computation handles near-zero probabilities."""
    W_prev = {"aws": 0.99, "gcp": 0.001, "azure": 0.001, "do": 0.008}
    W_k = {"aws": 0.001, "gcp": 0.99, "azure": 0.001, "do": 0.008}
    d = safe_kl_divergence(W_k, W_prev)
    assert d > 0
    assert not (d == float('inf') or d != d)  # Not inf, not NaN
```

---

### 5.2 Governance Store Tests
**File**: `tests/unit/test_governance_store.py` (new)

```python
"""Unit tests for governance state persistence."""

import pytest
from agents.governance_agent.state_store import InMemoryStore, GovernanceStateStore


def test_in_memory_store_save_and_get():
    """In-memory store saves and retrieves weights."""
    store = InMemoryStore()
    weights = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
    store.save_applied_weights(weights, "dec1", "corr1")

    retrieved = store.get_previous_weights()
    assert retrieved == weights


def test_in_memory_store_empty():
    """In-memory store returns None when empty."""
    store = InMemoryStore()
    assert store.get_previous_weights() is None


def test_governance_store_factory_fallback():
    """Store factory falls back to in-memory when Mongo unavailable."""
    store = GovernanceStateStore.create(use_mongo=False)
    assert isinstance(store, InMemoryStore)

    weights = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
    store.save_applied_weights(weights, "dec1", "corr1")
    assert store.get_previous_weights() == weights
```

---

### 5.3 Integration Test
**File**: `tests/e2e/test_governance_pipeline.py` (new)

Pattern follows [tests/e2e/e2e_pipeline_test.py](tests/e2e/e2e_pipeline_test.py):

```python
"""
E2E test for governance decision pipeline.

Sends topo.decision messages, verifies policy.decision output structure and logic.
"""

import json
import logging
from datetime import datetime, timezone
from uuid import uuid4

from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka_core.config import KafkaConfig
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import TopographyDecision, TopographyDecisionValue, TopographyAction
from kafka_core.topic_initializer import TopicInitializer


logger = logging.getLogger(__name__)


def test_governance_deterministic_replay():
    """
    Governance produces deterministic output for same input.

    Send same batch twice, verify identical policy.decision output.
    """
    initializer = TopicInitializer()
    initializer.create_all_topics()

    producer = KafkaProducerTemplate()
    consumer = KafkaConsumer(
        "policy.decision",
        bootstrap_servers=KafkaConfig.BOOTSTRAP_SERVERS,
        group_id="test_governance",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        max_poll_records=10,
        session_timeout_ms=30000,
    )

    run_id = str(uuid4())

    # Send first batch
    for i in range(3):
        action = TopographyAction(from_csp="aws", to_csp="gcp", intensity=0.1 * i)
        topo_value = TopographyDecisionValue(
            actions=[action.dict()],
            metadata={"solver": "convex_qp"},
            timestamp=datetime.now(timezone.utc),
            correlation_id=run_id,
        )
        topo_event = TopographyDecision(key=f"topo_{i}", value=topo_value)
        producer.send("topo.decision", topo_event)

    # Consume governance decisions
    decisions_1 = []
    for record in consumer:
        decisions_1.append(record.value)
        if len(decisions_1) >= 1:
            break

    # Send second batch (same)
    for i in range(3):
        action = TopographyAction(from_csp="aws", to_csp="gcp", intensity=0.1 * i)
        topo_value = TopographyDecisionValue(
            actions=[action.dict()],
            metadata={"solver": "convex_qp"},
            timestamp=datetime.now(timezone.utc),
            correlation_id=run_id,
        )
        topo_event = TopographyDecision(key=f"topo_{i}_retry", value=topo_value)
        producer.send("topo.decision", topo_event)

    # Consume governance decisions
    decisions_2 = []
    for record in consumer:
        decisions_2.append(record.value)
        if len(decisions_2) >= 1:
            break

    # Verify: same weights and KL even if decision_id differs
    assert decisions_1[0]["weights"] == decisions_2[0]["weights"]
    assert decisions_1[0]["kl_divergence"] == decisions_2[0]["kl_divergence"]
    assert decisions_1[0]["threshold"] == decisions_2[0]["threshold"]

    assert sum(decisions_1[0]["weights"].values()) == 100
    assert sum(decisions_2[0]["weights"].values()) == 100

    producer.close()
    consumer.close()
```

---

## 6. Hard-Constraint Guardrails

Add explicit assertions and comments:

- **No confidence-based ranking**: governance_logic.py has no reference to `confidence` in selection path.
- **No merge/blend**: `select_best_plan()` picks exactly one candidate, no weighted average.
- **No re-optimization**: governance only applies threshold gate, no solver calls.
- **No dynamic threshold**: threshold hardcoded or read once from config, never adjusted based on state.

Add test `test_governance_constraints.py` to verify these at import/inspection time.

---

## 7. Verification Steps

1. ✅ Run unit tests: `pytest tests/unit/test_governance_logic.py tests/unit/test_governance_store.py -v`
2. ✅ Run E2E test: `pytest tests/e2e/test_governance_pipeline.py -v`
3. ✅ Deterministic replay: same batch → identical output.
4. ✅ Mongo down test: governance continues with in-memory fallback (logged).
5. ✅ Threshold boundary: D=0.029 → ignored, D=0.031 → applied.
6. ✅ Weights validation: all governance decisions have sum=100 and all weights ≥ 0.

---

## Summary of Decisions

| Decision | Value |
|----------|-------|
| **Persistence** | Mongo primary + in-memory fallback |
| **Output format** | Integer percentages summing to exactly 100 |
| **Candidate collection** | Fixed K per cycle (default 3) |
| **Selection method** | KL divergence only (argmin with lex tie-break) |
| **Threshold** | 0.03 (hardcoded in logic, configurable in runner) |
| **KL epsilon** | 1e-6 |
| **Min weight** | 0.05 |

---

## Notes for Refinement

1. Partition key for `policy.decision`: currently `decision_id`. Confirm with downstream executor if that's appropriate, or change to `correlation_id` if ordering by trace thread is needed.
2. Default `candidate_window_k=3` aligns with 3 primary CSPs (aws, gcp, azure). Adjust if production telemetry cadence requires larger batches.
3. Governance decision correlation_id: inherited from first message in batch for trace continuity.
4. Output integer conversion: uses largest-remainder method to ensure sum=100 deterministically.