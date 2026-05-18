"""AKS Topology Agent Runner

Starts the AKS topology agent to consume service.state and produce topo.decisions.
"""

import logging
import os
import signal
from typing import Optional

from agents.aks.topo import TopographyAgent
from kafka_core.topic_initializer import TopicInitializer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


class AKSTopoAgentRunner:
    """Runner for AKS Topology Agent with lifecycle management."""

    def __init__(self, service_id: str = "aks"):
        self.service_id = service_id
        self.agent: Optional[TopographyAgent] = None
        self._shutdown_event = False

    def _signal_handler(self, signum, frame):
        """Handle SIGTERM and SIGINT signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self._shutdown_event = True

    def start(self) -> None:
        """Start the AKS topology agent."""
        try:
            # Initialize Kafka topics
            logger.info("Initializing Kafka topics")
            initializer = TopicInitializer()
            results = initializer.create_all_topics()

            success_count = sum(1 for ok in results.values() if ok)
            logger.info(f"Topic initialization: {success_count}/{len(results)} successful")

            # Setup signal handlers
            signal.signal(signal.SIGTERM, self._signal_handler)
            signal.signal(signal.SIGINT, self._signal_handler)

            # Create and start agent
            logger.info(f"Starting AKS Topology Agent (service_id={self.service_id})")
            self.agent = TopographyAgent(service_id=self.service_id)

            # Run agent (blocking, until shutdown signal)
            self.agent.start()

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error in AKS Topology Agent Runner: {e}", exc_info=True)
            raise
        finally:
            if self.agent:
                self.agent.close()
            logger.info("AKS Topology Agent Runner shutdown complete")


def main():
    """Entry point for AKS Topology Agent Runner."""
    service_id = os.getenv("SERVICE_ID", "aks")
    
    logger.info(f"AKS Topology Agent Runner starting with service_id={service_id}")
    
    runner = AKSTopoAgentRunner(service_id=service_id)
    runner.start()


if __name__ == "__main__":
    main()
