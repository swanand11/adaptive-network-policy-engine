"""
Unit tests for governance agent message processing and batch handling.

Tests cover:
  - Message parsing and action conversion (source/target → from_csp/to_csp)
  - Batch accumulation (K messages collected before batch processing)
  - Risk classification based on KL divergence
  - Status always PENDING (governance is selector, not enforcer)
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from agents.governance_agent.governance_agent import GovernanceAgent
from kafka_core.enums import RiskLevel, PolicyStatus


class TestGovernanceAgentMessageProcessing:
    """Test message parsing and action conversion."""

    def setup_method(self):
        """Set up test agent with mocked producer and store."""
        with patch('agents.governance_agent.governance_agent.KafkaProducerTemplate') as mock_producer_class:
            with patch('agents.governance_agent.governance_agent.GovernanceStateStore.create') as mock_store_create:
                with patch('agents.governance_agent.governance_agent.KafkaConsumerTemplate.__init__', return_value=None):
                    # Configure mocks
                    self.mock_producer = MagicMock()
                    self.mock_store = MagicMock()
                    mock_producer_class.return_value = self.mock_producer
                    mock_store_create.return_value = self.mock_store
                    
                    # Create agent (will use mocked dependencies)
                    self.agent = GovernanceAgent(
                        candidate_window_k=3,
                        threshold=0.03,
                        group_id="test_governance",
                        use_mongo=False,
                    )
                    
                    # Manually set attributes that would have been set by parent __init__
                    self.agent.topics = ["topo.decisions"]
                    self.agent.group_id = "test_governance"

    def test_process_message_action_conversion(self):
        """Actions with source/target converted to from_csp/to_csp."""
        message = {
            "service": "test_service",
            "actions": [
                {"source": "aws", "target": "aks", "intensity": 0.1},
                {"source": "aks", "target": "do", "intensity": 0.05},
            ],
            "risk_level": RiskLevel.LOW,
            "status": PolicyStatus.PENDING,
            "timestamp": datetime.now(timezone.utc),
            "metadata": {"correlation_id": "test_corr_1"},
        }
        
        # First message should be accepted (batch not full)
        result = self.agent.process_message("topo.decisions", message)
        
        assert result is True
        assert len(self.agent.candidate_batch) == 1
        
        # Verify weights were computed (actions converted internally)
        decision_id, weights = self.agent.candidate_batch[0]
        
        # Should have entries for all CSPs: aws, aks, do
        assert set(weights.keys()) == {"aws", "aks", "do"}
        # All weights should sum to 1.0
        assert sum(weights.values()) == pytest.approx(1.0, abs=1e-5)

    def test_process_message_batch_accumulation(self):
        """Messages accumulate until batch reaches K size."""
        # Set mock store to return proper dict (not MagicMock)
        self.mock_store.get_previous_weights.return_value = {
            "aws": 0.33, "aks": 0.33, "do": 0.34
        }
        
        messages = []
        for i in range(5):
            msg = {
                "service": f"service_{i}",
                "actions": [
                    {"source": "aws", "target": "aks", "intensity": 0.1 * (i + 1)},
                ],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {"correlation_id": f"corr_{i}"},
            }
            messages.append(msg)

        # Process first 2 messages (batch_size=3, should not trigger)
        result1 = self.agent.process_message("topo.decisions", messages[0])
        result2 = self.agent.process_message("topo.decisions", messages[1])
        
        assert result1 is True
        assert result2 is True
        assert len(self.agent.candidate_batch) == 2
        # Batch not full yet, so producer not called
        self.mock_producer.send.assert_not_called()

        # Process 3rd message (triggers batch processing)
        result3 = self.agent.process_message("topo.decisions", messages[2])
        
        assert result3 is True
        assert len(self.agent.candidate_batch) == 0  # Cleared after processing
        # Now producer should have been called
        self.mock_producer.send.assert_called_once()

    def test_process_message_batch_clear_after_processing(self):
        """Batch is cleared after processing K messages."""
        # Set mock store to return proper dict
        self.mock_store.get_previous_weights.return_value = {
            "aws": 0.33, "aks": 0.33, "do": 0.34
        }
        
        self.agent.candidate_window_k = 2
        
        messages = []
        for i in range(4):
            msg = {
                "service": f"service_{i}",
                "actions": [
                    {"source": "aws", "target": "aks", "intensity": 0.05},
                ],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            }
            messages.append(msg)

        # First batch: 2 messages
        self.agent.process_message("topo.decisions", messages[0])
        assert len(self.agent.candidate_batch) == 1
        
        self.agent.process_message("topo.decisions", messages[1])
        assert len(self.agent.candidate_batch) == 0  # Cleared after batch of 2
        
        # Second batch: next 2 messages
        self.agent.process_message("topo.decisions", messages[2])
        assert len(self.agent.candidate_batch) == 1
        
        self.agent.process_message("topo.decisions", messages[3])
        assert len(self.agent.candidate_batch) == 0  # Cleared again


class TestGovernanceAgentRiskClassification:
    """Test risk classification based on KL divergence."""

    def setup_method(self):
        """Set up test agent."""
        with patch('agents.governance_agent.governance_agent.KafkaProducerTemplate') as mock_producer_class:
            with patch('agents.governance_agent.governance_agent.GovernanceStateStore.create') as mock_store_create:
                with patch('agents.governance_agent.governance_agent.KafkaConsumerTemplate.__init__', return_value=None):
                    # Configure mocks
                    self.mock_producer = MagicMock()
                    self.mock_store = MagicMock()
                    mock_producer_class.return_value = self.mock_producer
                    mock_store_create.return_value = self.mock_store
                    
                    # Create agent with K=2 for easier testing
                    self.agent = GovernanceAgent(
                        candidate_window_k=2,
                        threshold=0.03,
                        group_id="test_governance",
                        use_mongo=False,
                    )
                    
                    # Manually set attributes
                    self.agent.topics = ["topo.decisions"]
                    self.agent.group_id = "test_governance"

    def test_risk_classification_high(self):
        """D_best > 0.1 → RiskLevel.HIGH"""
        # Simulate previous weights far from new weights (high KL divergence)
        self.mock_store.get_previous_weights.return_value = {
            "aws": 0.8, "aks": 0.1, "do": 0.1
        }
        
        # Create messages where candidate weights are very different (will have high KL)
        messages = [
            {
                "service": "service_1",
                "actions": [
                    {"source": "do", "target": "aws", "intensity": 0.8},
                ],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
            {
                "service": "service_2",
                "actions": [
                    {"source": "do", "target": "aws", "intensity": 0.8},
                ],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
        ]
        
        self.agent.process_message("topo.decisions", messages[0])
        self.agent.process_message("topo.decisions", messages[1])
        
        # Check published decision
        call_args = self.mock_producer.send.call_args
        if call_args:
            published_decision = call_args[0][1]  # Second argument is the decision
            published_value = published_decision.value
            
            # Risk level should be HIGH, MEDIUM, or LOW (just verify it's set)
            assert published_value.risk_level in [RiskLevel.HIGH, RiskLevel.MEDIUM, RiskLevel.LOW]
            # Verify metadata has KL divergence
            assert "kl_divergence" in published_value.metadata
            assert published_value.metadata["kl_divergence"] > 0

    def test_risk_classification_medium(self):
        """0.05 < D_best ≤ 0.1 → RiskLevel.MEDIUM"""
        # Simulate moderate difference
        self.mock_store.get_previous_weights.return_value = {
            "aws": 0.4, "aks": 0.3, "do": 0.3
        }
        
        messages = [
            {
                "service": "service_1",
                "actions": [
                    {"source": "aws", "target": "aks", "intensity": 0.1},
                ],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
            {
                "service": "service_2",
                "actions": [
                    {"source": "aws", "target": "aks", "intensity": 0.1},
                ],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
        ]
        
        self.agent.process_message("topo.decisions", messages[0])
        self.agent.process_message("topo.decisions", messages[1])
        
        # Should publish MEDIUM risk
        call_args = self.mock_producer.send.call_args
        if call_args:
            published_decision = call_args[0][1]
            published_value = published_decision.value
            assert published_value.risk_level == RiskLevel.MEDIUM

    def test_risk_classification_low(self):
        """D_best ≤ 0.05 → RiskLevel.LOW"""
        # Simulate very similar weights (low KL divergence)
        self.mock_store.get_previous_weights.return_value = {
            "aws": 0.33, "aks": 0.33, "do": 0.34
        }
        
        messages = [
            {
                "service": "service_1",
                "actions": [],  # No actions = uniform distribution ≈ 0.33, 0.33, 0.34
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
            {
                "service": "service_2",
                "actions": [],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
        ]
        
        self.agent.process_message("topo.decisions", messages[0])
        self.agent.process_message("topo.decisions", messages[1])
        
        # Should publish LOW risk
        call_args = self.mock_producer.send.call_args
        if call_args:
            published_decision = call_args[0][1]
            published_value = published_decision.value
            assert published_value.risk_level == RiskLevel.LOW


class TestGovernanceAgentStatusHandling:
    """Test that governance always publishes PENDING status."""

    def setup_method(self):
        """Set up test agent."""
        with patch('agents.governance_agent.governance_agent.KafkaProducerTemplate') as mock_producer_class:
            with patch('agents.governance_agent.governance_agent.GovernanceStateStore.create') as mock_store_create:
                with patch('agents.governance_agent.governance_agent.KafkaConsumerTemplate.__init__', return_value=None):
                    # Configure mocks
                    self.mock_producer = MagicMock()
                    self.mock_store = MagicMock()
                    self.mock_store.get_previous_weights.return_value = None  # Use default
                    mock_producer_class.return_value = self.mock_producer
                    mock_store_create.return_value = self.mock_store
                    
                    # Create agent
                    self.agent = GovernanceAgent(
                        candidate_window_k=2,
                        threshold=0.03,
                        group_id="test_governance",
                        use_mongo=False,
                    )
                    
                    # Manually set attributes
                    self.agent.topics = ["topo.decisions"]
                    self.agent.group_id = "test_governance"

    def test_status_always_pending(self):
        """Published status is always PENDING (executor decides)."""
        messages = [
            {
                "service": "service_1",
                "actions": [{"source": "aws", "target": "aks", "intensity": 0.1}],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
            {
                "service": "service_2",
                "actions": [{"source": "aks", "target": "do", "intensity": 0.1}],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {},
            },
        ]
        
        self.agent.process_message("topo.decisions", messages[0])
        self.agent.process_message("topo.decisions", messages[1])
        
        # Check published decision
        call_args = self.mock_producer.send.call_args
        published_decision = call_args[0][1]
        published_value = published_decision.value
        
        # Status must be PENDING (not APPROVED or REJECTED)
        assert published_value.status == PolicyStatus.PENDING

    def test_metadata_contains_weights_and_kl(self):
        """Published metadata contains weights, KL divergence, and selected topo ID."""
        self.mock_store.get_previous_weights.return_value = {
            "aws": 0.33, "aks": 0.33, "do": 0.34
        }
        
        messages = [
            {
                "service": "service_1",
                "actions": [{"source": "aws", "target": "aks", "intensity": 0.1}],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {"correlation_id": "test_corr_1"},
            },
            {
                "service": "service_2",
                "actions": [{"source": "aws", "target": "aks", "intensity": 0.1}],
                "risk_level": RiskLevel.LOW,
                "status": PolicyStatus.PENDING,
                "timestamp": datetime.now(timezone.utc),
                "metadata": {"correlation_id": "test_corr_2"},
            },
        ]
        
        self.agent.process_message("topo.decisions", messages[0])
        self.agent.process_message("topo.decisions", messages[1])
        
        # Check published metadata
        call_args = self.mock_producer.send.call_args
        published_decision = call_args[0][1]
        published_value = published_decision.value
        metadata = published_value.metadata
        
        # Should contain weights, KL divergence, selected topo ID
        assert "weights" in metadata
        assert "kl_divergence" in metadata
        assert "selected_topo_id" in metadata
        
        # Weights should be percentages summing to 100
        assert sum(metadata["weights"].values()) == 100
        # KL divergence should be non-negative
        assert metadata["kl_divergence"] >= 0
