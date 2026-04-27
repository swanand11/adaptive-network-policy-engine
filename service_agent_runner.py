import logging

from kafka_core.agents import ServiceAgent
from kafka_core.topic_initializer import TopicInitializer


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    logging.info("Initializing Kafka topics for Service Agent")
    initializer = TopicInitializer()
    initializer.create_all_topics()

    logging.info("Starting Service Agent for service-api")
    agent = ServiceAgent(service_id="service-api", latency_threshold=200.0)
    try:
        agent.start()
    except KeyboardInterrupt:
        logging.info("Service Agent interrupted by user")
    finally:
        agent.close()


if __name__ == "__main__":
    main()
