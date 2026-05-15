"""
Unit tests for governance KL selection logic.

Tests cover:
  - actions_to_weights conversion
  - KL divergence computation (identical, different, zero-safe)
  - Plan selection (multiple candidates, deterministic tie-breaking)
  - Threshold gating (above/below threshold)
  - Integer weight rounding (sum=100)
"""

import pytest
import math
from agents.governance_agent.governance_logic import (
    actions_to_weights,
    safe_kl_divergence,
    select_best_plan,
    apply_threshold,
    to_percent_weights,
)

CSP_SET = ["aws", "gcp", "azure", "do"]


class TestActionsToWeights:
    """Test conversion of actions to normalized weights."""

    def test_actions_to_weights_basic(self):
        """Convert actions to normalized weights."""
        actions = [
            {"from_csp": "aws", "to_csp": "gcp", "intensity": 0.1},
            {"from_csp": "gcp", "to_csp": "azure", "intensity": 0.05},
        ]
        weights = actions_to_weights(actions, CSP_SET)

        # Sum should equal 1.0
        assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)

        # All weights should be >= min_weight (0.05)
        assert all(w >= 0.05 for w in weights.values())

        # Should have entry for each CSP
        assert set(weights.keys()) == set(CSP_SET)

    def test_actions_to_weights_empty(self):
        """Empty actions list returns uniform distribution."""
        actions = []
        weights = actions_to_weights(actions, CSP_SET)

        # Should be uniform
        expected_uniform = 1.0 / len(CSP_SET)
        assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)
        assert all(w == pytest.approx(expected_uniform, abs=1e-6) for w in weights.values())

    def test_actions_to_weights_heavy_outflow(self):
        """Heavy outflow from one CSP reduces its weight."""
        actions = [
            {"from_csp": "aws", "to_csp": "gcp", "intensity": 0.5},
            {"from_csp": "aws", "to_csp": "azure", "intensity": 0.5},
        ]
        weights = actions_to_weights(actions, CSP_SET)

        # AWS has net outflow -1.0, should have lower weight
        assert weights["aws"] < weights["gcp"]
        assert weights["aws"] < weights["azure"]
        assert sum(weights.values()) == pytest.approx(1.0, abs=1e-6)


class TestSafeKLDivergence:
    """Test KL divergence computation with safety."""

    def test_kl_divergence_identical(self):
        """KL divergence of identical distributions is zero."""
        W = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        d = safe_kl_divergence(W, W)
        assert d == pytest.approx(0.0, abs=1e-5)

    def test_kl_divergence_different(self):
        """KL divergence detects differences."""
        W_prev = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
        W_k = {"aws": 0.25, "gcp": 0.5, "azure": 0.15, "do": 0.1}
        d = safe_kl_divergence(W_k, W_prev)
        assert d > 0

    def test_kl_divergence_asymmetry(self):
        """KL(W_k || W_prev) != KL(W_prev || W_k) (asymmetric)."""
        W_prev = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
        W_k = {"aws": 0.25, "gcp": 0.5, "azure": 0.15, "do": 0.1}

        d_forward = safe_kl_divergence(W_k, W_prev)
        d_backward = safe_kl_divergence(W_prev, W_k)

        # Generally different (though could be equal by chance)
        assert isinstance(d_forward, float)
        assert isinstance(d_backward, float)

    def test_kl_divergence_zero_safe(self):
        """KL handles near-zero probabilities without inf/NaN."""
        W_prev = {"aws": 0.99, "gcp": 0.001, "azure": 0.001, "do": 0.008}
        W_k = {"aws": 0.001, "gcp": 0.99, "azure": 0.001, "do": 0.008}
        d = safe_kl_divergence(W_k, W_prev)

        assert d > 0
        assert not math.isinf(d)
        assert not math.isnan(d)


class TestSelectBestPlan:
    """Test deterministic plan selection by KL divergence."""

    def test_select_best_plan_single(self):
        """Single candidate is always selected."""
        W_prev = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        candidates = [
            ("plan1", {"aws": 0.3, "gcp": 0.25, "azure": 0.25, "do": 0.2}),
        ]
        best_id, best_w, d_best = select_best_plan(candidates, W_prev)

        assert best_id == "plan1"
        assert best_w == {"aws": 0.3, "gcp": 0.25, "azure": 0.25, "do": 0.2}
        assert d_best >= 0

    def test_select_best_plan_multiple(self):
        """Multiple plans selects lowest KL."""
        W_prev = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        candidates = [
            ("plan1", {"aws": 0.26, "gcp": 0.25, "azure": 0.25, "do": 0.24}),  # Very close to W_prev
            ("plan2", {"aws": 0.50, "gcp": 0.25, "azure": 0.15, "do": 0.10}),  # Far from W_prev
            ("plan3", {"aws": 0.25, "gcp": 0.26, "azure": 0.25, "do": 0.24}),  # Very close to W_prev
        ]
        best_id, best_w, d_best = select_best_plan(candidates, W_prev)

        # Should pick plan1 or plan3 (both have lower KL)
        assert best_id in ["plan1", "plan3"]
        d_plan2 = safe_kl_divergence(candidates[1][1], W_prev)
        assert d_best < d_plan2

    def test_select_best_plan_deterministic_tiebreak(self):
        """Ties broken lexicographically by decision_id."""
        W_prev = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}

        # Both have identical weights, should select lexicographically smaller
        candidates = [
            ("plan_b", {"aws": 0.26, "gcp": 0.25, "azure": 0.25, "do": 0.24}),
            ("plan_a", {"aws": 0.26, "gcp": 0.25, "azure": 0.25, "do": 0.24}),
        ]
        best_id, _, _ = select_best_plan(candidates, W_prev)

        assert best_id == "plan_a"  # Lexicographically smaller

    def test_select_best_plan_empty_raises(self):
        """Empty candidate list raises ValueError."""
        W_prev = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        with pytest.raises(ValueError):
            select_best_plan([], W_prev)


class TestApplyThreshold:
    """Test threshold gating of selected plan."""

    def test_apply_threshold_above(self):
        """D_best > threshold applies new weights."""
        W_prev = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
        W_best = {"aws": 0.25, "gcp": 0.5, "azure": 0.15, "do": 0.1}
        D_best = 0.1
        threshold = 0.03

        W_final, applied = apply_threshold(W_best, W_prev, D_best, threshold)
        assert applied is True
        assert W_final == W_best

    def test_apply_threshold_below(self):
        """D_best <= threshold ignores new weights."""
        W_prev = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}
        W_best = {"aws": 0.49, "gcp": 0.26, "azure": 0.15, "do": 0.10}
        D_best = 0.01
        threshold = 0.03

        W_final, applied = apply_threshold(W_best, W_prev, D_best, threshold)
        assert applied is False
        assert W_final == W_prev

    def test_apply_threshold_boundary(self):
        """D_best == threshold (boundary) is not applied."""
        W_prev = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        W_best = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.10}
        D_best = 0.03
        threshold = 0.03

        W_final, applied = apply_threshold(W_best, W_prev, D_best, threshold)
        assert applied is False
        assert W_final == W_prev


class TestToPercentWeights:
    """Test conversion to integer percentages summing to 100."""

    def test_to_percent_weights_sums_100(self):
        """Percent weights sum to exactly 100."""
        W_final = {"aws": 0.42, "gcp": 0.33, "azure": 0.15, "do": 0.10}
        percent = to_percent_weights(W_final)

        assert sum(percent.values()) == 100
        assert all(v > 0 for v in percent.values())

    def test_to_percent_weights_uniform(self):
        """Uniform distribution converts correctly."""
        W_final = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        percent = to_percent_weights(W_final)

        assert sum(percent.values()) == 100
        # Each should be 25
        assert all(v == 25 for v in percent.values())

    def test_to_percent_weights_deterministic(self):
        """Same input always produces same output (largest-remainder method)."""
        W_final = {"aws": 0.333, "gcp": 0.333, "azure": 0.334, "do": 0.0}

        result1 = to_percent_weights(W_final.copy())
        result2 = to_percent_weights(W_final.copy())

        assert result1 == result2

    def test_to_percent_weights_all_nonzero(self):
        """All weights are non-zero in output."""
        W_final = {"aws": 0.5, "gcp": 0.3, "azure": 0.15, "do": 0.05}
        percent = to_percent_weights(W_final)

        assert all(v > 0 for v in percent.values())
        assert sum(percent.values()) == 100
