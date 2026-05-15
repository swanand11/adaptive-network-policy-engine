"""
Governance Logic: KL-Based Selection

Pure, deterministic functions for converting topography candidates to weights,
computing KL divergence, and applying threshold-based selection.

CONSTRAINTS:
  ❌ NO confidence-based ranking (KL-only selection)
  ❌ NO merge/blend of candidate plans (select exactly one)
  ❌ NO re-optimization (only KL + threshold gate)
  ❌ NO dynamic threshold (hardcoded or read once at startup)
"""

import logging
import math
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)

# Constants
EPSILON = 1e-6
MIN_WEIGHT = 0.05
THRESHOLD = 0.03


def actions_to_weights(
    actions: List[Dict],
    csp_set: List[str],
) -> Dict[str, float]:
    """
    Convert topography actions to normalized CSP weights.

    Algorithm:
    1. Compute net flow per CSP: net_i = incoming_i - outgoing_i
    2. Base weight: base_i = 1 / N
    3. Adjusted weight: W_i = base_i + net_i
    4. Clamp: W_i >= MIN_WEIGHT (0.05)
    5. Normalize: sum(W_i) = 1.0

    Args:
        actions: List of {"from_csp": str, "to_csp": str, "intensity": float}
        csp_set: List of all CSP names (e.g., ["aws", "gcp", "azure", "do"])

    Returns:
        Dict[csp_name] -> normalized weight in [0, 1]
    """
    # Initialize incoming/outgoing flows
    incoming = {csp: 0.0 for csp in csp_set}
    outgoing = {csp: 0.0 for csp in csp_set}

    for action in actions:
        from_csp = action.get("from_csp")
        to_csp = action.get("to_csp")
        intensity = action.get("intensity", 0.0)

        if from_csp in outgoing:
            outgoing[from_csp] += intensity
        if to_csp in incoming:
            incoming[to_csp] += intensity

    # Compute net flow per CSP
    net = {csp: incoming[csp] - outgoing[csp] for csp in csp_set}

    # Base weight (uniform)
    n = len(csp_set)
    base = 1.0 / n

    # Adjusted weight (base + net flow)
    weights = {csp: base + net[csp] for csp in csp_set}

    # Clamp to minimum
    weights = {csp: max(weights[csp], MIN_WEIGHT) for csp in csp_set}

    # Normalize to sum=1.0
    total = sum(weights.values())
    if total <= 0:
        # Fallback: uniform distribution
        weights = {csp: 1.0 / n for csp in csp_set}
    else:
        weights = {csp: weights[csp] / total for csp in csp_set}

    logger.debug(f"actions_to_weights: {len(actions)} actions → {weights}")
    return weights


def safe_kl_divergence(
    W_k: Dict[str, float],
    W_prev: Dict[str, float],
) -> float:
    """
    Compute KL divergence from W_prev to W_k with epsilon safety.

    Formula: D_k = sum(q_i * log(q_i / p_i))
      where p_i = max(W_prev_i, epsilon)
            q_i = max(W_k_i, epsilon)

    Args:
        W_k: Candidate weights from topography decision
        W_prev: Previous applied weights

    Returns:
        KL divergence (non-negative float, never inf or NaN)
    """
    d_kl = 0.0

    for csp in W_k.keys():
        p_i = max(W_prev.get(csp, 1.0 / len(W_k)), EPSILON)
        q_i = max(W_k[csp], EPSILON)

        d_kl += q_i * math.log(q_i / p_i)

    logger.debug(f"safe_kl_divergence: D = {d_kl:.6f}")
    return d_kl


def select_best_plan(
    candidates: List[Tuple[str, Dict[str, float]]],
    W_prev: Dict[str, float],
) -> Tuple[str, Dict[str, float], float]:
    """
    Select candidate with lowest KL divergence (deterministic, no confidence).

    Deterministic tie-breaker: lexicographic order on decision_id.

    Args:
        candidates: List of (decision_id, W_k) tuples
        W_prev: Previous applied weights

    Returns:
        (best_decision_id, best_weights, D_best)
    """
    if not candidates:
        raise ValueError("No candidates to select from")

    best_decision_id = None
    best_weights = None
    best_kl = float('inf')

    for decision_id, W_k in candidates:
        D_k = safe_kl_divergence(W_k, W_prev)

        # Deterministic selection: lower D_k wins, or lexicographically smaller decision_id on tie
        if D_k < best_kl or (D_k == best_kl and (best_decision_id is None or decision_id < best_decision_id)):
            best_kl = D_k
            best_decision_id = decision_id
            best_weights = W_k

    logger.info(
        f"select_best_plan: selected decision_id={best_decision_id}, D_best={best_kl:.6f} from {len(candidates)} candidates"
    )
    return best_decision_id, best_weights, best_kl


def apply_threshold(
    W_best: Dict[str, float],
    W_prev: Dict[str, float],
    D_best: float,
    threshold: float = THRESHOLD,
) -> Tuple[Dict[str, float], bool]:
    """
    Apply threshold gate: if D_best > threshold, use W_best; else use W_prev.

    This prevents oscillations by ignoring small, disruptive changes.

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
        logger.info(f"apply_threshold: APPLIED (D_best={D_best:.6f} > threshold={threshold})")
    else:
        W_final = W_prev
        logger.info(f"apply_threshold: IGNORED (D_best={D_best:.6f} <= threshold={threshold})")

    return W_final, applied


def to_percent_weights(W_final: Dict[str, float]) -> Dict[str, int]:
    """
    Convert normalized weights [0, 1] to integer percentages summing to exactly 100.

    Deterministic rounding: largest-remainder method ensures sum = 100.

    Args:
        W_final: Normalized weights summing to ~1.0

    Returns:
        Dict[csp_name] -> integer percentage, sum always = 100
    """
    csps = sorted(W_final.keys())
    fractions = [W_final[csp] * 100 for csp in csps]

    # Floor each fraction
    integers = [int(f) for f in fractions]
    remainders = [f - int(f) for f in fractions]

    # Distribute remainder to CSPs with largest fractional parts
    remainder_sum = 100 - sum(integers)
    if remainder_sum > 0:
        remainder_indices = sorted(
            range(len(remainders)),
            key=lambda i: remainders[i],
            reverse=True,
        )[:remainder_sum]

        for i in remainder_indices:
            integers[i] += 1

    result = {csps[i]: integers[i] for i in range(len(csps))}
    final_sum = sum(result.values())

    logger.info(f"to_percent_weights: {result}, sum={final_sum}")
    assert final_sum == 100, f"Weights must sum to 100, got {final_sum}"

    return result
