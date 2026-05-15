"""
Governance Agent Runner

Starts a GovernanceAgent instance to consume topo.decision messages,
select plans by KL divergence, and publish policy.decision.

Mirrors the pattern from service_agent_runner.py with signal handling and graceful shutdown.
"""

import logging
import os
import signal
from typing import Optional

from agents.governance_agent.governance_agent import GovernanceAgent
from kafka_core.topic_initializer import TopicInitializer

logger = logging.getLogger(__name__)


class GovernanceAgentRunner:
    """Runner for Governance Agent with lifecycle management."""

    def __init__(
        self,
        candidate_window_k: int = 3,
        threshold: float = 0.03,
        use_mongo: bool = True,
    ):
        """
        Initialize Governance Agent Runner.

        Args:
            candidate_window_k: Batch size for candidate collection
            threshold: KL threshold for applying weights
            use_mongo: Whether to use MongoDB for persistence
        """
        self.candidate_window_k = candidate_window_k
        self.threshold = threshold
        self.use_mongo = use_mongo
        self.agent: Optional[GovernanceAgent] = None
        self._shutdown_event = False

    def _signal_handler(self, signum, frame):
        """Handle SIGTERM and SIGINT signals."""
        logger.info(f"Received signal {signum}, initiating graceful shutdown")
        self._shutdown_event = True

    def start(self) -> None:
        """Start the governance agent."""
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
            logger.info("Starting Governance Agent")
            self.agent = GovernanceAgent(
                candidate_window_k=self.candidate_window_k,
                threshold=self.threshold,
                use_mongo=self.use_mongo,
            )

            # Run agent (blocking, until shutdown signal)
            self.agent.start()

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Fatal error in Governance Agent Runner: {e}", exc_info=True)
            raise
        finally:
            if self.agent:
                self.agent.close()
            logger.info("Governance Agent Runner shutdown complete")


def main():
    """Entry point for Governance Agent Runner."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    # Read environment variables
    candidate_window_k = int(os.getenv("GOVERNANCE_WINDOW_K", "3"))
    threshold = float(os.getenv("GOVERNANCE_THRESHOLD", "0.03"))
    use_mongo_str = os.getenv("GOVERNANCE_USE_MONGO", "true").lower()
    use_mongo = use_mongo_str in ("true", "1", "yes")

    logger.info(
        f"Governance Agent Runner configuration: "
        f"window_k={candidate_window_k}, threshold={threshold}, use_mongo={use_mongo}"
    )

    runner = GovernanceAgentRunner(
        candidate_window_k=candidate_window_k,
        threshold=threshold,
        use_mongo=use_mongo,
    )
    runner.start()


if __name__ == "__main__":
    main()
