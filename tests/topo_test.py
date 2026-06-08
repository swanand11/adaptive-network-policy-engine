
import time
import json
from pathlib import Path
import sys
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from agents.aws.topo import TopographyAgent as AWSTopo
from agents.aks.topo import TopographyAgent as AKSTopo
from agents.do.topo import TopographyAgent as DOTopo
from kafka_core.producer_base import KafkaProducerTemplate
from mocks.normalized_load_simulator import NormalizedLoadSimulator


# ---------------------------------------------------------------------------
# Mock Consumer (we still mock input side)
# ---------------------------------------------------------------------------

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

    # Create ALL agents with REAL Kafka producer
    agents = {
        "aws": AWSTopo("aws", MockConsumer(), KafkaProducerTemplate()),
        "aks": AKSTopo("aks", MockConsumer(), KafkaProducerTemplate()),
        "do":  DOTopo("do",  MockConsumer(), KafkaProducerTemplate()),
    }

    print("\n=== Multi-Agent Simulation (Kafka Producer Mode) ===\n")

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

        # Trigger computation + publish
        print("\n[Agent Decisions → Kafka]")
        for name, agent in agents.items():
            print(f"\nAgent: {name}")

            # Compute actions manually so we can print them
            actions = agent._compute_redistribution_actions()

            if not actions:
                print("No actions")
                continue

            # Pretty print actions
            payload = {
                "actions": [
                    {
                        "from": a["from"],
                        "to": a["to"],
                        "intensity": a["intensity"],
                    }
                    for a in actions
                ]
            }

            print("Actions:")
            print(json.dumps(payload, indent=2))

            # Show decision metadata with new fields
            print("\nDecision Metadata:")
            metadata = {
                "agent_type": "topography",
                "solver": "convex_qp_entropy_regularised",
                "iteration": agent.iteration_count,
                "global_state_size": len(agent.global_state),
                "confidence": (
                    sum(state["confidence"] for state in agent.global_state.values())
                    / len(agent.global_state)
                ) if agent.global_state else 0.0,
                "temperature": agent.temperature * ((1 - agent.gamma) ** agent.iteration_count),
                "alpha": agent.alpha,
                "beta": agent.beta,
                "gamma": agent.gamma,
            }
            print(json.dumps(metadata, indent=2))

            # Now publish to Kafka
            agent._publish_actions(actions)

        time.sleep(1)

    # Ensure all messages are flushed before exit
    print("\nFlushing producers...")
    for agent in agents.values():
        agent.producer.close()

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
