"""Topography Agent for AWS

Responsible for computing system-level actions to move the system toward optimal normalized load signals.
Operates in a distributed, event-driven architecture using Kafka as shared state layer.

Core Logic:
1. Consume global state of normalized loads from all services
2. Compute pressure per service: P_i = L_opt_i - L_i
3. Partition into overloaded (P_i < 0) and underloaded (P_i > 0) sets
4. Compute redistribution flows between overloaded and underloaded services
5. Produce action signals for traffic redistribution

Architecture:
- No direct inter-agent communication
- Kafka = shared state layer
- Each agent independently computes same result
- Eventual convergence expected
"""

import logging
import time
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from datetime import datetime
import json

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import PolicyDecision, PolicyDecisionValue
from kafka_core.enums import PolicyStatus, RiskLevel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopographyAgent(KafkaConsumerTemplate):
    """Topography Agent that computes load redistribution actions."""

    def __init__(self, service_id: str = "aws"):
        """
        Initialize the Topography Agent.

        Args:
            service_id: Identifier for this service instance (e.g., 'aws', 'aks', 'do')
        """
        super().__init__(
            topics=["metrics.events"],  # Consume normalized load updates
            group_id=f"topography-{service_id}"
        )

        self.service_id = service_id
        self.producer = KafkaProducerTemplate()

        # Global state: service_id -> {L_i, L_opt_i, confidence}
        self.global_state: Dict[str, Dict] = {}

        # Computation parameters
        self.alpha = 0.2  # Local correction factor
        self.beta = 0.1   # Smoothing factor
        self.temperature = 1.0  # Exploration temperature
        self.gamma = 0.01  # Temperature decay

        # Processing state
        self.iteration_count = 0
        self.last_computation_time = 0
        self.computation_interval = 5.0  # seconds between computations

        logger.info(f"Topography Agent initialized for service {service_id}")

    def process_message(self, topic: str, message: dict) -> bool:
        """
        Process incoming normalized load message.

        Expected message format from mock:
        MetricsEvent with metrics containing:
        {
            "L_i": 0.6,
            "L_opt_i": 0.4,
            "confidence": 0.82
        }
        """
        try:
            # Extract service_id from the message
            # For MetricsEvent, service_id is in value.service
            if "value" in message and "service" in message["value"]:
                service_id = message["value"]["service"]
                metrics = message["value"].get("metrics", {})

                # Check if this message contains normalized load data
                if "L_i" in metrics and "L_opt_i" in metrics:
                    # Update global state
                    self.global_state[service_id] = {
                        "L_i": metrics.get("L_i", 0.0),
                        "L_opt_i": metrics.get("L_opt_i", 0.0),
                        "confidence": metrics.get("confidence", 0.0)
                    }

                    logger.debug(f"Updated state for {service_id}: {self.global_state[service_id]}")

                    # Check if we should compute actions
                    current_time = time.time()
                    if current_time - self.last_computation_time >= self.computation_interval:
                        self.compute_and_publish_actions()
                        self.last_computation_time = current_time
                        self.iteration_count += 1

                    return True
                else:
                    # Not a normalized load message, skip
                    return True
            else:
                logger.warning(f"Invalid message format: {message}")
                return False

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    def compute_and_publish_actions(self):
        """Compute redistribution actions and publish them."""
        if len(self.global_state) < 2:
            logger.info("Not enough services in global state for redistribution")
            return

        try:
            actions = self.compute_redistribution_actions()

            if actions:
                self.publish_actions(actions)
                logger.info(f"Published {len(actions)} redistribution actions")
            else:
                logger.info("No redistribution actions needed")

        except Exception as e:
            logger.error(f"Error computing actions: {e}")

    def compute_redistribution_actions(self) -> List[Dict]:
        """
        Compute redistribution actions based on current global state.

        Returns:
            List of action dictionaries: [{"from": str, "to": str, "intensity": float}, ...]
        """
        if not self.global_state:
            return []

        # Step 1: Compute pressure per service
        pressures = self.compute_pressures()

        # Step 2: Partition nodes
        overloaded, underloaded = self.partition_nodes(pressures)

        if not overloaded or not underloaded:
            logger.info("No overloaded or underloaded services found")
            return []

        # Step 3: Compute redistribution flows
        actions = self.compute_flows(overloaded, underloaded, pressures)

        # Step 4: Apply iterative refinement (optional)
        # self.apply_iterative_refinement()

        return actions

    def compute_pressures(self) -> Dict[str, float]:
        """
        Compute pressure for each service.

        P_i = L_opt_i - L_i
        P_i > 0: underloaded (can accept traffic)
        P_i < 0: overloaded (needs to shed traffic)
        """
        pressures = {}
        for service_id, state in self.global_state.items():
            L_i = state["L_i"]
            L_opt_i = state["L_opt_i"]
            pressure = L_opt_i - L_i
            pressures[service_id] = pressure
            logger.debug(f"Pressure for {service_id}: {pressure:.3f} (L_i={L_i:.3f}, L_opt={L_opt_i:.3f})")

        return pressures

    def partition_nodes(self, pressures: Dict[str, float]) -> Tuple[List[str], List[str]]:
        """
        Partition services into overloaded and underloaded sets.

        Returns:
            Tuple of (overloaded_services, underloaded_services)
        """
        overloaded = [sid for sid, p in pressures.items() if p < 0]
        underloaded = [sid for sid, p in pressures.items() if p > 0]

        logger.info(f"Overloaded services: {overloaded}")
        logger.info(f"Underloaded services: {underloaded}")

        return overloaded, underloaded

    def compute_flows(self, overloaded: List[str], underloaded: List[str],
                     pressures: Dict[str, float]) -> List[Dict]:
        """
        Compute redistribution flows between overloaded and underloaded services.

        For each pair (i ∈ O, j ∈ U):
        flow_i→j ∝ w_i * w_j * |P_i| * P_j

        Normalize flows so that:
        - outgoing from i ≤ |P_i|
        - incoming to j ≤ P_j
        """
        actions = []

        # Compute raw flows
        raw_flows = defaultdict(float)
        total_possible_flow = 0

        for i in overloaded:
            for j in underloaded:
                w_i = self.global_state[i]["confidence"]
                w_j = self.global_state[j]["confidence"]
                p_i = abs(pressures[i])  # |P_i|
                p_j = pressures[j]       # P_j

                # Flow proportional to weights and pressures
                flow = w_i * w_j * p_i * p_j
                raw_flows[(i, j)] = flow
                total_possible_flow += flow

        if total_possible_flow == 0:
            return []

        # Normalize flows
        normalized_actions = self.normalize_flows(raw_flows, overloaded, underloaded, pressures)

        # Convert to action format
        for (from_service, to_service), intensity in normalized_actions.items():
            if intensity > 0.001:  # Only include significant actions
                actions.append({
                    "from": from_service,
                    "to": to_service,
                    "intensity": round(intensity, 3)
                })

        return actions

    def normalize_flows(self, raw_flows: Dict[Tuple[str, str], float],
                       overloaded: List[str], underloaded: List[str],
                       pressures: Dict[str, float]) -> Dict[Tuple[str, str], float]:
        """
        Normalize flows to ensure constraints are met.

        - outgoing from i ≤ |P_i|
        - incoming to j ≤ P_j
        """
        normalized = {}

        # First, scale flows to respect outgoing constraints
        outgoing_scaled = {}
        for i in overloaded:
            outgoing_flows = {pair: flow for pair, flow in raw_flows.items() if pair[0] == i}
            if not outgoing_flows:
                continue

            total_outgoing = sum(outgoing_flows.values())
            max_outgoing = abs(pressures[i])

            if total_outgoing > max_outgoing:
                # Scale down to fit constraint
                scale_factor = max_outgoing / total_outgoing
                for pair, flow in outgoing_flows.items():
                    outgoing_scaled[pair] = flow * scale_factor
            else:
                outgoing_scaled.update(outgoing_flows)

        # Then, scale flows to respect incoming constraints
        incoming_scaled = {}
        for j in underloaded:
            incoming_flows = {pair: flow for pair, flow in outgoing_scaled.items() if pair[1] == j}
            if not incoming_flows:
                continue

            total_incoming = sum(incoming_flows.values())
            max_incoming = pressures[j]

            if total_incoming > max_incoming:
                # Scale down to fit constraint
                scale_factor = max_incoming / total_incoming
                for pair, flow in incoming_flows.items():
                    incoming_scaled[pair] = flow * scale_factor
            else:
                incoming_scaled.update(incoming_flows)

        # Final normalization to ensure total flow balance
        total_flow = sum(incoming_scaled.values())
        if total_flow > 0:
            # Normalize to sum to 1 (or some maximum total flow)
            max_total_flow = 1.0
            if total_flow > max_total_flow:
                scale_factor = max_total_flow / total_flow
                for pair in incoming_scaled:
                    incoming_scaled[pair] *= scale_factor

        return incoming_scaled

    def apply_iterative_refinement(self):
        """
        Apply iterative refinement to improve convergence.

        L_i(k+1) = L_i(k) + α(L_opt_i - L_i(k)) + β Σ W_ij (L_j(k) - L_i(k)) + T_i * ξ_i
        """
        # Update temperature
        self.temperature *= (1 - self.gamma)

        # This is a simplified version - full implementation would require
        # adjacency matrix W_ij and noise term ξ_i
        pass

    def publish_actions(self, actions: List[Dict]):
        """
        Publish computed actions to policy.decisions topic.

        Args:
            actions: List of action dicts [{"from": str, "to": str, "intensity": float}, ...]
        """
        decision_id = f"topo-{self.service_id}-{int(time.time())}"

        decision_value = PolicyDecisionValue(
            service=self.service_id,
            decision=json.dumps({"actions": actions}),
            risk_level=self.assess_risk_level(actions),
            status=PolicyStatus.PENDING,
            timestamp=datetime.now(),
            metadata={
                "agent_type": "topography",
                "iteration": self.iteration_count,
                "global_state_size": len(self.global_state),
                "temperature": self.temperature
            }
        )

        decision = PolicyDecision(
            key=decision_id,
            value=decision_value
        )

        success = self.producer.send("policy.decisions", decision)
        if success:
            logger.info(f"Published decision {decision_id} with {len(actions)} actions")
        else:
            logger.error(f"Failed to publish decision {decision_id}")

    def assess_risk_level(self, actions: List[Dict]) -> RiskLevel:
        """
        Assess risk level of the computed actions.

        Args:
            actions: List of action dicts

        Returns:
            RiskLevel enum value
        """
        if not actions:
            return RiskLevel.LOW

        total_intensity = sum(action["intensity"] for action in actions)
        max_intensity = max(action["intensity"] for action in actions)

        # Simple risk assessment based on total and max intensity
        if total_intensity > 0.8 or max_intensity > 0.5:
            return RiskLevel.HIGH
        elif total_intensity > 0.5 or max_intensity > 0.3:
            return RiskLevel.MEDIUM
        else:
            return RiskLevel.LOW

    def close(self):
        """Graceful shutdown."""
        self.producer.close()
        super().close()

if __name__ == "__main__":
    # For testing/development
    import sys

    service_id = sys.argv[1] if len(sys.argv) > 1 else "aws"
    agent = TopographyAgent(service_id=service_id)

    try:
        logger.info(f"Starting Topography Agent for {service_id}")
        agent.start()
    except KeyboardInterrupt:
        logger.info("Shutting down Topography Agent")
        agent.close()
