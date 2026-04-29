import logging
import os

from kafka_core.agents import ServiceAgent
from kafka_core.topic_initializer import TopicInitializer


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    service_id = os.getenv("SERVICE_ID", "service-api")
    logging.info("Initializing Kafka topics for Service Agent")
    initializer = TopicInitializer()
    initializer.create_all_topics()

    logging.info("Starting Service Agent for %s", service_id)
    agent = ServiceAgent(service_id=service_id, latency_threshold=200.0)
    try:
        agent.start()
    except KeyboardInterrupt:
        logging.info("Service Agent interrupted by user")
    finally:
        agent.close()


if __name__ == "__main__":
    main()
