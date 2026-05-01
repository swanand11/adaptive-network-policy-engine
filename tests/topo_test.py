import pytest
from agents.aws.topo import TopographyAgent
import config

@pytest.fixture
def agent():
    """Create a TopographyAgent instance with no Kafka side effects."""
    agent = TopographyAgent(service_id="aws")

    # Disable producer side effects
    agent.producer = None
    agent.publish_actions = lambda actions: None

    return agent


def seed_global_state(agent):
    """Helper to populate agent with test data."""
    agent.global_state = {
        "aws": {"L_i": 0.8, "L_opt_i": 0.4, "confidence": 0.9},   # overloaded
        "aks": {"L_i": 0.2, "L_opt_i": 0.5, "confidence": 0.8},   # underloaded
        "do":  {"L_i": 0.3, "L_opt_i": 0.3, "confidence": 0.7},   # neutral
    }


def test_compute_pressures(agent):
    seed_global_state(agent)

    pressures = agent.compute_pressures()

    assert pytest.approx(pressures["aws"]) == -0.4
    assert pytest.approx(pressures["aks"]) == 0.3
    assert pytest.approx(pressures["do"]) == 0.0


def test_partition_nodes(agent):
    seed_global_state(agent)

    pressures = agent.compute_pressures()
    overloaded, underloaded = agent.partition_nodes(pressures)

    assert "aws" in overloaded
    assert "aks" in underloaded
    assert "do" not in overloaded
    assert "do" not in underloaded


def test_compute_flows(agent):
    seed_global_state(agent)

    pressures = agent.compute_pressures()
    overloaded, underloaded = agent.partition_nodes(pressures)

    actions = agent.compute_flows(overloaded, underloaded, pressures)

    assert isinstance(actions, list)
    assert len(actions) > 0

    for action in actions:
        assert "from" in action
        assert "to" in action
        assert "intensity" in action
        assert action["intensity"] > 0


def test_compute_redistribution_actions(agent):
    seed_global_state(agent)

    actions = agent.compute_redistribution_actions()

    assert isinstance(actions, list)
    assert any(a["from"] == "aws" and a["to"] == "aks" for a in actions)


def test_no_actions_when_balanced(agent):
    agent.global_state = {
        "aws": {"L_i": 0.4, "L_opt_i": 0.4, "confidence": 0.9},
        "aks": {"L_i": 0.5, "L_opt_i": 0.5, "confidence": 0.8},
    }

    actions = agent.compute_redistribution_actions()
    assert actions == []


def test_normalize_flows_constraints(agent):
    seed_global_state(agent)

    pressures = agent.compute_pressures()
    overloaded, underloaded = agent.partition_nodes(pressures)

    raw_flows = {
        ("aws", "aks"): 10.0  # intentionally large to test scaling
    }

    normalized = agent.normalize_flows(raw_flows, overloaded, underloaded, pressures)

    total_outgoing = sum(v for (i, _), v in normalized.items() if i == "aws")
    total_incoming = sum(v for (_, j), v in normalized.items() if j == "aks")

    assert total_outgoing <= abs(pressures["aws"]) + 1e-6
    assert total_incoming <= pressures["aks"] + 1e-6


def test_assess_risk_level(agent):
    low_actions = [{"from": "a", "to": "b", "intensity": 0.1}]
    medium_actions = [{"from": "a", "to": "b", "intensity": 0.4}]
    high_actions = [{"from": "a", "to": "b", "intensity": 0.9}]

    assert str(agent.assess_risk_level(low_actions)).lower().endswith("low")
    assert str(agent.assess_risk_level(medium_actions)).lower().endswith("medium")
    assert str(agent.assess_risk_level(high_actions)).lower().endswith("high")