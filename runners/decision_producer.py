"""Governance Stub — Consumes topo.decisions and emits to policy.decisions.

Usage
-----
    python -m runners.decision_producer

Consumes events from topo.decisions.
Emits minimal stub decisions to policy.decisions.
"""

import logging
import sys
import time
from pathlib import Path
from typing import Dict, Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("governance_stub")


class DictEvent:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class GovernanceStub(KafkaConsumerTemplate):
    """Consumer that reads topo.decisions and emits stub to policy.decisions."""

    def __init__(self):
        super().__init__(
            topics=["topo.decisions"],
            group_id="governance_agent"
        )
        self.producer = KafkaProducerTemplate()

    def process_message(self, topic: str, message: Dict[str, Any]) -> bool:
        try:
            value = message.get("value", {})
            source = value.get("service", "unknown")
            
            stub_payload = {
                "decision": "ALLOW",
                "reason": "stub",
                "source": source,
                "timestamp": int(time.time())
            }
            
            event = DictEvent(key="governance_stub", value=stub_payload)
            self.producer.send("policy.decisions", event)
            logger.info(f"Emitted stub decision for source: {source}")
            
            return True
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    def close(self):
        super().close()
        if hasattr(self, 'producer'):
            self.producer.close()


def main():
    """Main consumer loop."""
    logger.info("Starting Governance Stub...")
    try:
        stub = GovernanceStub()
        stub.start()
    except KeyboardInterrupt:
        logger.info("Shutting down stub...")
        stub.close()
    except Exception as e:
        logger.error(f"Stub failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()