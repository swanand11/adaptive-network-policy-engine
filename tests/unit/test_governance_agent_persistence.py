from datetime import datetime, timezone

from agents.governance_agent.governance_agent import GovernanceAgent


class DummyStore:
    def __init__(self):
        self.saved = []

    def get_previous_weights(self):
        return {"aws": 0.4, "aks": 0.3, "do": 0.3}

    def save_applied_weights(self, weights, decision_id, correlation_id=None):
        self.saved.append((weights, decision_id, correlation_id))


class DummyProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, payload):
        self.sent.append((topic, payload))


def test_process_batch_persists_selected_weights(monkeypatch):
    agent = GovernanceAgent.__new__(GovernanceAgent)
    agent.threshold = 0.03
    agent.candidate_batch = [
        ("topo-a", {"aws": 0.45, "aks": 0.30, "do": 0.25}),
        ("topo-b", {"aws": 0.40, "aks": 0.35, "do": 0.25}),
    ]
    agent.root_correlation_id = "corr-123"
    agent.root_timestamp = datetime.now(timezone.utc)
    agent.store = DummyStore()
    agent.producer = DummyProducer()

    monkeypatch.setattr("agents.governance_agent.governance_agent.uuid.uuid4", lambda: "gov-id-1")

    agent._process_batch()

    assert len(agent.producer.sent) == 1
    assert len(agent.store.saved) == 1

    saved_weights, saved_decision_id, saved_corr = agent.store.saved[0]
    assert saved_decision_id == "gov-id-1"
    assert saved_corr == "corr-123"
    assert isinstance(saved_weights, dict)
    assert set(saved_weights.keys()) == {"aws", "aks", "do"}
