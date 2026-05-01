"""Mock Normalized Load Publisher

Simulates sending normalized load messages to Kafka for testing the Topography Agent.
This replaces the actual metric collection and normalization process during development.

Message format:
{
    "service_id": "aws",  # or "aks", "do"
    "L_i": 0.6,          # current normalized load
    "L_opt_i": 0.4,      # optimal normalized load
    "confidence": 0.82   # confidence in the measurement
}
"""

import time
import random
import logging
from datetime import datetime
from typing import Dict, List

from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import MetricsEvent, MetricsEventValue
from kafka_core.enums import CloudProvider

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class NormalizedLoadSimulator:
    """Simulates normalized load publishing for multiple cloud services."""

    def __init__(self):
        self.producer = KafkaProducerTemplate()

        # Service configurations
        self.services = [
            {
                "id": "aws",
                "cloud": CloudProvider.AWS,
                "L_opt": 0.4,  # Optimal load
                "baseline_L": 0.3,  # Baseline current load
                "confidence": 0.85
            },
            {
                "id": "aks",
                "cloud": CloudProvider.AZURE,
                "L_opt": 0.35,
                "baseline_L": 0.5,
                "confidence": 0.78
            },
            {
                "id": "do",
                "cloud": CloudProvider.DIGITALOCEAN,
                "L_opt": 0.25,
                "baseline_L": 0.2,
                "confidence": 0.92
            }
        ]

        # Simulation parameters
        self.update_interval = 3.0  # seconds
        self.load_variation = 0.1   # random variation in load
        self.confidence_variation = 0.05  # random variation in confidence

    def generate_load_message(self, service: Dict) -> Dict:
        """
        Generate a normalized load message for a service.

        Args:
            service: Service configuration dict

        Returns:
            Message dict with service_id, L_i, L_opt_i, confidence
        """
        # Add some random variation to current load
        L_i = service["baseline_L"] + random.gauss(0, self.load_variation)
        L_i = max(0.0, min(1.0, L_i))  # Clamp to [0,1]

        # Add some random variation to confidence
        confidence = service["confidence"] + random.gauss(0, self.confidence_variation)
        confidence = max(0.5, min(1.0, confidence))  # Clamp to [0.5,1.0]

        return {
            "service_id": service["id"],
            "L_i": round(L_i, 3),
            "L_opt_i": service["L_opt"],
            "confidence": round(confidence, 3)
        }

    def publish_load_update(self, service: Dict):
        """
        Publish a load update for a service.

        Args:
            service: Service configuration dict
        """
        message = self.generate_load_message(service)

        # Create MetricsEvent (even though we're using it for normalized loads)
        # In a real system, this might be a separate topic
        event_value = MetricsEventValue(
            service=service["id"],
            cloud=service["cloud"],
            timestamp=datetime.now(),
            metrics={
                "L_i": message["L_i"],
                "L_opt_i": message["L_opt_i"],
                "confidence": message["confidence"]
            },
            correlation_id=f"load-sim-{service['id']}-{int(time.time())}"
        )

        event = MetricsEvent(
            key=f"normalized-load-{service['id']}",
            value=event_value
        )

        success = self.producer.send("metrics.events", event)
        if success:
            logger.info(f"Published load update for {service['id']}: L_i={message['L_i']:.3f}, "
                       f"L_opt={message['L_opt_i']:.3f}, conf={message['confidence']:.3f}")
        else:
            logger.error(f"Failed to publish load update for {service['id']}")

    def run_simulation(self, duration_seconds: int = 60):
        """
        Run the load simulation for a specified duration.

        Args:
            duration_seconds: How long to run the simulation
        """
        logger.info("Starting normalized load simulation")
        logger.info(f"Services: {[s['id'] for s in self.services]}")

        start_time = time.time()
        iteration = 0

        try:
            while time.time() - start_time < duration_seconds:
                iteration += 1
                logger.info(f"=== Iteration {iteration} ===")

                # Publish load updates for all services
                for service in self.services:
                    self.publish_load_update(service)

                # Wait before next update
                time.sleep(self.update_interval)

        except KeyboardInterrupt:
            logger.info("Simulation interrupted by user")

        finally:
            self.producer.close()
            logger.info("Simulation ended")

    def create_imbalanced_scenario(self):
        """Create an imbalanced load scenario for testing."""
        logger.info("Creating imbalanced load scenario")

        # AWS overloaded
        self.services[0]["baseline_L"] = 0.8  # High load

        # AKS underloaded
        self.services[1]["baseline_L"] = 0.2  # Low load

        # DO normal
        self.services[2]["baseline_L"] = 0.35  # Near optimal

        logger.info("Scenario: AWS overloaded, AKS underloaded, DO normal")
        self.run_simulation(duration_seconds=30)


if __name__ == "__main__":
    import sys

    simulator = NormalizedLoadSimulator()

    if len(sys.argv) > 1 and sys.argv[1] == "imbalanced":
        # Run imbalanced scenario for testing
        simulator.create_imbalanced_scenario()
    else:
        # Run normal simulation
        duration = int(sys.argv[1]) if len(sys.argv) > 1 else 60
        simulator.run_simulation(duration_seconds=duration)