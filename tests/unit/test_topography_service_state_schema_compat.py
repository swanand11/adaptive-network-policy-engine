from agents.aws.topo import TopographyAgent


class DummyConsumer:
    def close(self):
        pass


class DummyProducer:
    def send(self, *args, **kwargs):
        return True

    def close(self):
        return None


def _base_service_state_message():
    return {
        "service": "checkout",
        "cloud": "aws",
        "timestamp": "2026-05-18T10:00:00Z",
        "belief": {
            "latency_ewma": 120.0,
            "trend": "stable",
            "confidence": 0.92,
            "status": "healthy",
        },
        "intent": {
            "current_load": 0.65,
            "optimal_load": 0.55,
        },
        "metadata": {},
        "correlation_id": "corr-1",
        "parent_event_id": "parent-1",
    }


def test_process_message_handles_missing_producer_agent(caplog):
    agent = TopographyAgent(service_id="aws", consumer=DummyConsumer(), producer=DummyProducer())
    agent._compute_and_publish = lambda: None

    with caplog.at_level("WARNING"):
        ok = agent.process_message("service.state", _base_service_state_message())

    assert ok is True
    assert "checkout" in agent.global_state
    assert "Missing producer_agent field in service.state payload" in caplog.text


def test_process_message_handles_forward_compat_fields():
    agent = TopographyAgent(service_id="aws", consumer=DummyConsumer(), producer=DummyProducer())
    agent._compute_and_publish = lambda: None

    message = _base_service_state_message()
    message.update(
        {
            "producer_agent": "service-agent-aws",
            "event_id": "evt-123",
            "source_offset": "service.state@1:42",
            "depth": 3,
        }
    )

    ok = agent.process_message("service.state", message)

    assert ok is True
    assert agent.latest_event_id == "evt-123"
    assert agent.latest_parent_event_id == "evt-123"
    assert agent.latest_source_offset == "service.state@1:42"
    assert agent.latest_depth == 3
