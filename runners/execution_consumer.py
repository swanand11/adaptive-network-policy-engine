"""Execution Consumer — Consumes policy.decisions and updates load balancer weights.

Usage
-----
    python -m runners.execution_consumer

Consumes events from policy.decisions topic and updates the load balancer
weights by calling the proxy's /update_weights endpoint.

Expected message format in metadata:
- aws_wi: AWS weight (0-100)
- aks_wi: AKS weight (0-100)
- do_wi: DigitalOcean weight (0-100)
"""

import logging
import sys
import requests
from pathlib import Path
from typing import Dict, Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kafka_core.consumer_base import KafkaConsumerTemplate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("execution_consumer")


class ExecutionConsumer(KafkaConsumerTemplate):
    """Consumer that updates load balancer weights from policy decisions."""

    def __init__(self):
        super().__init__(
            topics=["policy.decisions"],
            group_id="execution_consumer_group"
        )
        self.proxy_url = "http://load-balancer-proxy:9000/update_weights"

    def process_message(self, topic: str, message: Dict[str, Any]) -> bool:
        """
        Process policy decision message and update proxy weights.

        Args:
            topic: Kafka topic
            message: Deserialized message

        Returns:
            True if processing succeeded
        """
        try:
            if topic != "policy.decisions":
                logger.warning(f"Unexpected topic: {topic}")
                return True

            # Support both wrapped and direct formats
            payload = message.get("value") if isinstance(message.get("value"), dict) else message
            metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}

            risk_level = str(payload.get("risk_level") or metadata.get("risk_level") or "low").lower()
            status = str(payload.get("status") or metadata.get("status") or "pending").lower()

            # High-risk decisions MUST be approved before application
            if risk_level == "high" and status != "approved":
                logger.info(
                    f"Skipping high-risk decision pending human approval: "
                    f"risk={risk_level}, status={status}, id={payload.get('key') or payload.get('decision_id') or 'unknown'}"
                )
                return True

            # Extract weights robustly from flat or nested format
            weights = metadata.get("weights") or payload.get("weights") or {}
            aws_wi = metadata.get("aws_wi") or weights.get("aws") or payload.get("aws_wi")
            aks_wi = metadata.get("aks_wi") or weights.get("aks") or payload.get("aks_wi")
            do_wi = metadata.get("do_wi") or weights.get("do") or payload.get("do_wi")

            if aws_wi is None or aks_wi is None or do_wi is None:
                logger.warning(f"Missing weights in message: {message}")
                return True

            # Validate weights sum to 100
            total = aws_wi + aks_wi + do_wi
            if total != 100:
                logger.warning(f"Weights don't sum to 100: {aws_wi} + {aks_wi} + {do_wi} = {total}")
                return True

            # Update proxy weights
            weights_data = {
                "aws_wi": aws_wi,
                "aks_wi": aks_wi,
                "do_wi": do_wi
            }

            logger.info(f"Updating weights: {weights_data}")

            response = requests.post(
                self.proxy_url,
                json=weights_data,
                timeout=5
            )

            if response.status_code == 200:
                logger.info("Weights updated successfully")
                return True
            else:
                logger.error(f"Failed to update weights: {response.status_code} {response.text}")
                return False

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False


def main():
    """Main consumer loop."""
    logger.info("Starting Execution Consumer...")

    try:
        consumer = ExecutionConsumer()
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        consumer.close()
    except Exception as e:
        logger.error(f"Consumer failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()