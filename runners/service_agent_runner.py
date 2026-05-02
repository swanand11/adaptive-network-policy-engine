"""
Service Agent Runner - Parallel Partition Processing

Runs ServiceAgent instances in parallel across Kafka partitions while maintaining
strict message ordering within each partition.

Architecture:
- Main thread: polls Kafka consumer, dispatches to partition queues
- Worker threads: one per partition, processes queue messages sequentially
- Partition workers route to appropriate ServiceAgent instance
- Offset commits only after successful processing

Key Features:
- Parallel processing across partitions (multiple workers run simultaneously)
- Strict ordering within partition (single-threaded worker per partition)
- Graceful shutdown with thread cleanup
- Configuration-driven agent instantiation
"""

import logging
import os
import signal
import threading
import time
from typing import Any, Callable, Dict, Optional

from kafka import KafkaConsumer, ConsumerRebalanceListener
from kafka.errors import KafkaError

from agents.agent_registry import AgentRegistry
from agents.partition_worker import PartitionWorker
from config.agent_config_loader import AgentConfigLoader
from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.dispatcher import Dispatcher
from kafka_core.partition_queue_manager import PartitionQueueManager
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.producer_registry import CloudProducerRegistry
from kafka_core.topic_initializer import TopicInitializer

logger = logging.getLogger(__name__)


class ParallelServiceAgentRunner(ConsumerRebalanceListener):
    """
    Manages parallel partition processing with configuration-driven agent instantiation.

    Responsibilities:
    - Load agent configuration from YAML
    - Initialize Kafka consumer, producer, queues, agents
    - Create and manage partition worker threads
    - Handle consumer rebalancing (partition assignment/revocation)
    - Implement graceful shutdown
    - Dispatch messages to partition queues
    - Coordinate offset commits
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the parallel runner.

        Args:
            config_path: Path to agents.yml. If None, uses environment or default.
        """
        # Load configuration
        self.config = AgentConfigLoader.get_from_env_or_default(config_path)
        self.group_id = self.config["group_id"]

        # Core components
        self.partition_queue_manager = PartitionQueueManager(
            default_maxsize=self.config["queue_config"]["max_size"]
        )
        self.agent_registry = AgentRegistry()
        self.dispatcher = Dispatcher()

        # Kafka components - use cloud-specific producer registry instead of single producer
        self.consumer: Optional[KafkaConsumer] = None
        self.producer_registry = CloudProducerRegistry()

        # Thread management
        self.worker_threads: Dict[int, threading.Thread] = {}
        self.partition_workers: Dict[int, PartitionWorker] = {}
        self._shutdown_event = threading.Event()
        self._threads_lock = threading.Lock()

        logger.info(f"ParallelServiceAgentRunner initialized with group_id={self.group_id}")

    def _initialize_consumer(self) -> None:
        """Initialize Kafka consumer for metrics.events topic."""
        try:
            from kafka_core.config import KafkaConfig

            config = KafkaConfig.get_consumer_config(self.group_id)
            self.consumer = KafkaConsumer(
                "metrics.events",
                bootstrap_servers=config.get("bootstrap_servers", ["localhost:9092"]),
                group_id=self.group_id,
                auto_offset_reset=config.get("auto_offset_reset", "earliest"),
                enable_auto_commit=False,  # Manual offset management
                value_deserializer=lambda m: (
                    __import__("json").loads(m.decode("utf-8")) if m else None
                ),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )

            # Set rebalance listener for partition assignment/revocation
            self.consumer.subscribe(
                ["metrics.events"],
                listener=self
            )

            logger.info(f"Consumer initialized for group {self.group_id}")

        except Exception as e:
            logger.error(f"Failed to initialize consumer: {e}")
            raise

    def on_partitions_revoked(self, partitions: Any) -> None:
        """
        Callback when partitions are revoked during rebalancing.

        Called before new partition assignment. Stops worker threads for revoked partitions.

        Args:
            partitions: List of TopicPartition objects being revoked
        """
        logger.info(f"Partitions revoked: {partitions}")

        revoked_partition_ids = [p.partition for p in partitions]

        for partition_id in revoked_partition_ids:
            self._stop_worker(partition_id)

    def on_partitions_assigned(self, partitions: Any) -> None:
        """
        Callback when new partitions are assigned during rebalancing.

        Called after partition assignment. Starts worker threads for assigned partitions.

        Args:
            partitions: List of TopicPartition objects being assigned
        """
        logger.info(f"Partitions assigned: {partitions}")

        assigned_partition_ids = [p.partition for p in partitions]

        for partition_id in assigned_partition_ids:
            self._start_worker(partition_id)

    def _start_worker(self, partition_id: int) -> None:
        """
        Start a worker thread for a partition.

        Creates queue, PartitionWorker instance, and thread. Starts thread as daemon.

        Args:
            partition_id: Partition ID to create worker for
        """
        with self._threads_lock:
            if partition_id in self.worker_threads:
                logger.debug(f"Worker already running for partition {partition_id}")
                return

            # Create queue for this partition
            queue = self.partition_queue_manager.get_or_create_queue(partition_id)

            # Create partition worker
            worker = PartitionWorker(
                partition_id=partition_id,
                queue=queue,
                agent_registry=self.agent_registry,
                producer_registry=self.producer_registry,
                group_id=self.group_id,
                manual_commit_fn=self._make_commit_fn(),
                runner_config=self.config,
                logger_name=f"PartitionWorker[{partition_id}]",
                queue_timeout_seconds=self.config["queue_config"]["timeout_seconds"],
            )

            self.partition_workers[partition_id] = worker

            # Create and start thread
            thread = threading.Thread(
                target=worker.run,
                name=f"PartitionWorkerThread-{partition_id}",
                daemon=True,
            )
            thread.start()
            self.worker_threads[partition_id] = thread

            logger.info(f"Started worker thread for partition {partition_id}")

    def _stop_worker(self, partition_id: int) -> None:
        """
        Stop a worker thread for a partition.

        Signals worker to stop, waits for thread to join with timeout.

        Args:
            partition_id: Partition ID to stop worker for
        """
        with self._threads_lock:
            if partition_id not in self.worker_threads:
                return

            worker = self.partition_workers.get(partition_id)
            thread = self.worker_threads.get(partition_id)

            if worker:
                worker.stop()

            if thread:
                # Wait for thread to finish (max 30 seconds)
                thread.join(timeout=30)
                if thread.is_alive():
                    logger.warning(
                        f"Worker thread for partition {partition_id} did not stop within timeout"
                    )

                del self.worker_threads[partition_id]
                logger.info(f"Stopped worker thread for partition {partition_id}")

    def _make_commit_fn(self) -> Callable[[str, int, int], None]:
        """
        Create a commit function for partition workers.

        Returns:
            Callable that takes (topic, partition, offset) and commits offset
        """

        def commit_offset(topic: str, partition: int, offset: int) -> None:
            if not self.consumer:
                logger.error("Consumer not initialized for offset commit")
                return

            try:
                self.consumer.commit(
                    {
                        __import__("kafka").TopicPartition(
                            topic, partition
                        ): __import__("kafka").structs.OffsetAndMetadata(offset + 1)
                    }
                )
            except KafkaError as e:
                logger.error(f"Failed to commit offset {offset} for {topic}[{partition}]: {e}")

        return commit_offset

    def run(self) -> None:
        """
        Main runner loop.

        Polls consumer for messages, dispatches to partition queues.
        Continues until shutdown_event is set.
        """
        logger.info("Starting parallel consumer poll loop")

        try:
            while not self._shutdown_event.is_set():
                if not self.consumer:
                    logger.error("Consumer not initialized")
                    break

                # Poll with 1 second timeout
                records = self.consumer.poll(timeout_ms=1000, max_records=100)

                if not records:
                    continue

                # Dispatch all records to partition queues
                for topic_partition, record_list in records.items():
                    for record in record_list:
                        success = self.dispatcher.dispatch(
                            record, self.partition_queue_manager._queues
                        )
                        if not success:
                            logger.debug(
                                f"Dropped message: partition={record.partition}, offset={record.offset}"
                            )

        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")
        except Exception as e:
            logger.error(f"Unexpected error in consumer loop: {e}", exc_info=True)
        finally:
            self._shutdown()

    def _shutdown(self) -> None:
        """
        Graceful shutdown sequence.

        1. Signal workers to stop
        2. Wait for workers to finish
        3. Close agents
        4. Close producer
        5. Close consumer
        """
        logger.info("Starting graceful shutdown")

        # Stop all worker threads
        logger.info("Stopping all worker threads")
        with self._threads_lock:
            partition_ids = list(self.worker_threads.keys())

        for partition_id in partition_ids:
            self._stop_worker(partition_id)

        # Close all agents
        logger.info("Closing all agents")
        self.agent_registry.close_all()

        # Close all cloud-specific producers
        logger.info("Closing all cloud producers")
        self.producer_registry.close_all()

        # Close consumer
        if self.consumer:
            try:
                self.consumer.close()
                logger.info("Consumer closed")
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

        logger.info("Shutdown complete")

    def start(self) -> None:
        """
        Start the runner (blocking until shutdown).

        Initializes all components and runs the consumer loop.
        """
        try:
            # Initialize topics
            logger.info("Initializing Kafka topics")
            initializer = TopicInitializer()
            initializer.create_all_topics()

            # Initialize consumer
            self._initialize_consumer()

            # Log configuration
            logger.info(f"Loaded configuration for {len(self.config['agents'])} agents:")
            for agent_spec in self.config["agents"]:
                logger.info(
                    f"  - {agent_spec['service_id']}@{agent_spec['cloud']} "
                    f"(alpha={agent_spec['alpha']}, latency_threshold={agent_spec['latency_threshold']})"
                )

            # Run main loop
            self.run()

        except Exception as e:
            logger.error(f"Fatal error in runner: {e}", exc_info=True)
            raise


def _setup_signal_handlers(runner: ParallelServiceAgentRunner) -> None:
    """
    Setup signal handlers for graceful shutdown.

    Handles SIGTERM and SIGINT to trigger shutdown_event.

    Args:
        runner: ParallelServiceAgentRunner instance
    """

    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown")
        runner._shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)


def main() -> None:
    """
    Entry point for service agent runner.

    Loads config, initializes runner, starts processing.
    """
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    logger.info("Service Agent Runner (Parallel) starting")

    # Get config path from environment or use default
    config_path = os.getenv("AGENTS_CONFIG_PATH")

    # Create and setup runner
    runner = ParallelServiceAgentRunner(config_path=config_path)
    _setup_signal_handlers(runner)

    # Start runner (blocking)
    try:
        runner.start()
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

