import time
import json

from agents.aws.topo import TopographyAgent as AWSTopo
from agents.aks.topo import TopographyAgent as AKSTopo
from agents.do.topo import TopographyAgent as DOTopo

from mocks.normalized_load_simulator import NormalizedLoadSimulator


# ---------------------------------------------------------------------------
# Mock Kafka
# ---------------------------------------------------------------------------

class MockProducer:
    def __init__(self, name):
        self.name = name
        self.sent_events = []

    def send(self, topic, event):
        self.sent_events.append((topic, event))
        return True

    def close(self):
        pass


class MockConsumer:
    def close(self):
        pass


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

def convert_to_agent_message(sim_msg):
    return {
        "value": {
            "service": sim_msg["service_id"],
            "metrics": {
                "L_i": sim_msg["L_i"],
                "L_opt_i": sim_msg["L_opt_i"],
                "confidence": sim_msg["confidence"],
            },
        }
    }


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def main():
    simulator = NormalizedLoadSimulator()

    # Force imbalance
    simulator.services[0]["baseline_L"] = 0.8
    simulator.services[1]["baseline_L"] = 0.2
    simulator.services[2]["baseline_L"] = 0.35

    # Create ALL agents
    agents = {
        "aws": AWSTopo("aws", MockConsumer(), MockProducer("aws")),
        "aks": AKSTopo("aks", MockConsumer(), MockProducer("aks")),
        "do":  DOTopo("do",  MockConsumer(), MockProducer("do")),
    }

    print("\n=== Multi-Agent Simulation ===\n")

    for iteration in range(5):
        print(f"\n--- Iteration {iteration + 1} ---")

        # Generate simulator messages
        sim_messages = []
        for service in simulator.services:
            sim_msg = simulator.generate_load_message(service)
            sim_messages.append(sim_msg)

            print("\n[Simulator Output]")
            print(json.dumps(sim_msg, indent=2))

        # Feed SAME messages to ALL agents
        for agent in agents.values():
            for sim_msg in sim_messages:
                agent_msg = convert_to_agent_message(sim_msg)
                agent.process_message("metrics.events", agent_msg)

        # Trigger computation for ALL agents
        for name, agent in agents.items():
            agent._compute_and_publish()

        # Print outputs per agent
        print("\n[Agent Decisions]")
        for name, agent in agents.items():
            producer = agent.producer

            print(f"\nAgent: {name}")
            for topic, decision in producer.sent_events:
                payload = json.loads(decision.value.decision)
                print(json.dumps(payload, indent=2))

            # clear for next iteration
            producer.sent_events.clear()

        time.sleep(1)

    print("\n=== Done ===")


if __name__ == "__main__":
    main()