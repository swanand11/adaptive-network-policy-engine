"""
Unit tests for governance state persistence.

Tests cover:
  - In-memory store save/retrieve
  - Empty store behavior
  - Factory fallback to in-memory when Mongo unavailable
"""

import pytest
from agents.governance_agent.state_store import InMemoryStore, GovernanceStateStore


class TestInMemoryStore:
    """Test in-memory governance state store."""

    def test_in_memory_store_save_and_get(self):
        """In-memory store saves and retrieves weights."""
        store = InMemoryStore()
        weights = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}

        store.save_applied_weights(weights, "decision_id_1", "correlation_1")

        retrieved = store.get_previous_weights()
        assert retrieved == weights

    def test_in_memory_store_multiple_saves(self):
        """Multiple saves overwrite previous state."""
        store = InMemoryStore()

        weights1 = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        store.save_applied_weights(weights1, "decision_1", "corr_1")

        weights2 = {"aws": 0.5, "gcp": 0.3, "azure": 0.15, "do": 0.05}
        store.save_applied_weights(weights2, "decision_2", "corr_2")

        retrieved = store.get_previous_weights()
        assert retrieved == weights2

    def test_in_memory_store_empty(self):
        """In-memory store returns None when empty."""
        store = InMemoryStore()
        assert store.get_previous_weights() is None

    def test_in_memory_store_metadata_saved(self):
        """Metadata (decision_id, correlation_id, timestamp) saved."""
        store = InMemoryStore()
        weights = {"aws": 0.5, "gcp": 0.25, "azure": 0.15, "do": 0.1}

        store.save_applied_weights(weights, "decision_x", "corr_x")

        # Internal state is updated
        assert store.state["decision_id"] == "decision_x"
        assert store.state["correlation_id"] == "corr_x"
        assert store.state["weights"] == weights
        assert store.state["timestamp"] is not None


class TestGovernanceStateStoreFactory:
    """Test factory pattern for creating stores."""

    def test_factory_creates_in_memory_when_no_mongo(self):
        """Factory creates InMemoryStore when use_mongo=False."""
        store = GovernanceStateStore.create(use_mongo=False)
        assert isinstance(store, InMemoryStore)

    def test_factory_in_memory_works(self):
        """Created in-memory store works correctly."""
        store = GovernanceStateStore.create(use_mongo=False)

        weights = {"aws": 0.4, "gcp": 0.3, "azure": 0.2, "do": 0.1}
        store.save_applied_weights(weights, "test_decision", "test_corr")

        retrieved = store.get_previous_weights()
        assert retrieved == weights

    def test_factory_fallback_on_mongo_unavailable(self):
        """Factory attempts Mongo but falls back gracefully if pymongo missing or connection fails."""
        # This test verifies fallback behavior exists
        # Actual Mongo connection is tested in integration tests
        store = GovernanceStateStore.create(use_mongo=True)

        # Should be either Mongo (if available) or InMemory (if not)
        assert store is not None

        # Verify basic interface works
        weights = {"aws": 0.25, "gcp": 0.25, "azure": 0.25, "do": 0.25}
        store.save_applied_weights(weights, "fallback_test", "corr_fallback")

        retrieved = store.get_previous_weights()
        assert retrieved == weights
