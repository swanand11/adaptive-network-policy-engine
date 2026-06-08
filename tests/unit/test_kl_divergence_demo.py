"""
Demonstration script to show KL divergence values for multiple events.
Processes batches of events and displays the calculated KL divergence.
"""

import pytest
from datetime import datetime, timezone
from unittest.mock import Mock, patch, MagicMock
from agents.governance_agent.governance_agent import GovernanceAgent
from kafka_core.enums import RiskLevel, PolicyStatus


def test_kl_divergence_values_multiple_scenarios():
    """Process multiple scenarios and display KL divergence values."""
    
    with patch('agents.governance_agent.governance_agent.KafkaProducerTemplate') as mock_producer_class:
        with patch('agents.governance_agent.governance_agent.GovernanceStateStore.create') as mock_store_create:
            with patch('agents.governance_agent.governance_agent.KafkaConsumerTemplate.__init__', return_value=None):
                # Configure mocks
                mock_producer = MagicMock()
                mock_store = MagicMock()
                mock_producer_class.return_value = mock_producer
                mock_store_create.return_value = mock_store
                
                # Create agent with K=2 for easier batching
                agent = GovernanceAgent(
                    candidate_window_k=2,
                    threshold=0.03,
                    group_id="test_governance",
                    use_mongo=False,
                )
                
                # Manually set attributes
                agent.topics = ["topo.decisions"]
                agent.group_id = "test_governance"
                
                print("\n" + "="*80)
                print("KL DIVERGENCE VALUES FOR MULTIPLE GOVERNANCE AGENT EVENTS")
                print("="*80 + "\n")
                
                # Scenario 1: Low divergence (similar to previous)
                print("SCENARIO 1: Low Divergence")
                print("-" * 40)
                mock_store.get_previous_weights.return_value = {
                    "aws": 0.33, "aks": 0.33, "do": 0.34
                }
                
                messages_low = [
                    {
                        "service": "service_low_1",
                        "actions": [{"source": "aws", "target": "aks", "intensity": 0.1}],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                    {
                        "service": "service_low_2",
                        "actions": [{"source": "aks", "target": "do", "intensity": 0.1}],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                ]
                
                agent.process_message("topo.decisions", messages_low[0])
                agent.process_message("topo.decisions", messages_low[1])
                
                call_args = mock_producer.send.call_args
                if call_args:
                    decision = call_args[0][1]
                    value = decision.value
                    kl_div = value.metadata.get("kl_divergence", 0)
                    risk = value.risk_level
                    weights = value.metadata.get("weights", {})
                    
                    print(f"Previous weights: {mock_store.get_previous_weights.return_value}")
                    print(f"Calculated weights: {weights}")
                    print(f"KL Divergence: {kl_div:.6f}")
                    print(f"Risk Level: {risk}")
                    print()
                
                # Scenario 2: Medium divergence
                print("SCENARIO 2: Medium Divergence")
                print("-" * 40)
                mock_store.reset_mock()
                mock_producer.reset_mock()
                mock_store.get_previous_weights.return_value = {
                    "aws": 0.4, "aks": 0.3, "do": 0.3
                }
                
                messages_med = [
                    {
                        "service": "service_med_1",
                        "actions": [
                            {"source": "aws", "target": "aks", "intensity": 0.3},
                            {"source": "aks", "target": "do", "intensity": 0.2},
                        ],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                    {
                        "service": "service_med_2",
                        "actions": [
                            {"source": "aws", "target": "aks", "intensity": 0.3},
                            {"source": "aks", "target": "do", "intensity": 0.2},
                        ],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                ]
                
                agent.process_message("topo.decisions", messages_med[0])
                agent.process_message("topo.decisions", messages_med[1])
                
                call_args = mock_producer.send.call_args
                if call_args:
                    decision = call_args[0][1]
                    value = decision.value
                    kl_div = value.metadata.get("kl_divergence", 0)
                    risk = value.risk_level
                    weights = value.metadata.get("weights", {})
                    
                    print(f"Previous weights: {mock_store.get_previous_weights.return_value}")
                    print(f"Calculated weights: {weights}")
                    print(f"KL Divergence: {kl_div:.6f}")
                    print(f"Risk Level: {risk}")
                    print()
                
                # Scenario 3: High divergence (very different)
                print("SCENARIO 3: High Divergence")
                print("-" * 40)
                mock_store.reset_mock()
                mock_producer.reset_mock()
                mock_store.get_previous_weights.return_value = {
                    "aws": 0.8, "aks": 0.1, "do": 0.1
                }
                
                messages_high = [
                    {
                        "service": "service_high_1",
                        "actions": [
                            {"source": "do", "target": "aws", "intensity": 0.8},
                        ],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                    {
                        "service": "service_high_2",
                        "actions": [
                            {"source": "do", "target": "aws", "intensity": 0.8},
                        ],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                ]
                
                agent.process_message("topo.decisions", messages_high[0])
                agent.process_message("topo.decisions", messages_high[1])
                
                call_args = mock_producer.send.call_args
                if call_args:
                    decision = call_args[0][1]
                    value = decision.value
                    kl_div = value.metadata.get("kl_divergence", 0)
                    risk = value.risk_level
                    weights = value.metadata.get("weights", {})
                    
                    print(f"Previous weights: {mock_store.get_previous_weights.return_value}")
                    print(f"Calculated weights: {weights}")
                    print(f"KL Divergence: {kl_div:.6f}")
                    print(f"Risk Level: {risk}")
                    print()
                
                # Scenario 4: Extreme high divergence
                print("SCENARIO 4: Extreme Divergence")
                print("-" * 40)
                mock_store.reset_mock()
                mock_producer.reset_mock()
                mock_store.get_previous_weights.return_value = {
                    "aws": 0.05, "aks": 0.05, "do": 0.9
                }
                
                messages_extreme = [
                    {
                        "service": "service_extreme_1",
                        "actions": [
                            {"source": "aws", "target": "aks", "intensity": 0.9},
                        ],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                    {
                        "service": "service_extreme_2",
                        "actions": [
                            {"source": "aws", "target": "aks", "intensity": 0.9},
                        ],
                        "risk_level": RiskLevel.LOW,
                        "status": PolicyStatus.PENDING,
                        "timestamp": datetime.now(timezone.utc),
                        "metadata": {},
                    },
                ]
                
                agent.process_message("topo.decisions", messages_extreme[0])
                agent.process_message("topo.decisions", messages_extreme[1])
                
                call_args = mock_producer.send.call_args
                if call_args:
                    decision = call_args[0][1]
                    value = decision.value
                    kl_div = value.metadata.get("kl_divergence", 0)
                    risk = value.risk_level
                    weights = value.metadata.get("weights", {})
                    
                    print(f"Previous weights: {mock_store.get_previous_weights.return_value}")
                    print(f"Calculated weights: {weights}")
                    print(f"KL Divergence: {kl_div:.6f}")
                    print(f"Risk Level: {risk}")
                    print()
                
                print("="*80)
                print("SUMMARY")
                print("="*80)
                print("KL Divergence Classification:")
                print("  - LOW:    KL ≤ 0.05")
                print("  - MEDIUM: 0.05 < KL ≤ 0.1")
                print("  - HIGH:   KL > 0.1")
                print("="*80 + "\n")


if __name__ == "__main__":
    test_kl_divergence_values_multiple_scenarios()
