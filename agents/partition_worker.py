"""
Partition Worker

Single-threaded worker that pulls messages from a partition queue,
routes to ServiceAgent, and manages offset commits.

One worker thread per Kafka partition ensures strict message ordering within partition.
"""

import logging
import time
from queue import Empty
from typing import Any, Callable, Optional

from kafka_core.utils import extract_key_parts

logger = logging.getLogger(__name__)


class PartitionWorker:
    """
    Single-threaded worker for processing messages from a partition queue.

    Responsibilities:
    - Pull messages from partition queue (blocking)
    - Extract service_id and cloud from message key
    - Get or create ServiceAgent via agent_registry
    - Call agent.process_message()
    - Produce output via shared producer
    - Commit offset ONLY after successful processing
    - Handle errors (log + no commit = retry on restart)
    """

    def __init__(
        self,
        partition_id: int,
        queue: Any,
        agent_registry: Any,
        producer_registry: Any,
        group_id: str,
        manual_commit_fn: Callable,
        logger_name: Optional[str] = None,
        queue_timeout_seconds: float = 30.0,
    ):
        """
        Initialize a partition worker.

        Args:
            partition_id: Kafka partition ID this worker handles.
            queue: Queue object (from PartitionQueueManager) to pull messages from.
            agent_registry: AgentRegistry instance for getting ServiceAgent instances.
            producer_registry: CloudProducerRegistry for getting cloud-specific producers.
            group_id: Kafka consumer group ID (for offset commit).
            manual_commit_fn: Callable(topic, partition, offset) for offset commits.
            logger_name: Logger name (for debugging). If None, uses partition_id.
            queue_timeout_seconds: Timeout for queue.get() calls.
        """
        self.partition_id = partition_id
        self.queue = queue
        self.agent_registry = agent_registry
        self.producer_registry = producer_registry
        self.group_id = group_id
        self.manual_commit_fn = manual_commit_fn
        self.queue_timeout_seconds = queue_timeout_seconds

        logger_name = logger_name or f"PartitionWorker[{partition_id}]"
        self.logger = logging.getLogger(logger_name)

        self._running = False
        self._messages_processed = 0
        self._errors = 0

        self.logger.info(
            f"PartitionWorker initialized for partition {partition_id}, "
            f"queue_timeout={queue_timeout_seconds}s"
        )

    def run(self):
        """
        Main worker loop. Runs until stop() is called or exception occurs.

        Process flow:
        1. Pull message from queue (blocking with timeout)
        2. Extract service_id, cloud from message key
        3. Get or create ServiceAgent
        4. Process message via agent.process_message()
        5. On success: produce output + commit offset
        6. On error: log + skip commit (message will retry on restart)
        """
        self._running = True
        self.logger.info(f"Starting worker loop for partition {self.partition_id}")

        while self._running:
            try:
                # Step 1: Pull message from queue (blocking with timeout)
                try:
                    record = self.queue.get(timeout=self.queue_timeout_seconds)
                except Empty:
                    # Queue is empty and timeout reached - normal, just continue
                    continue

                start_time = time.time()

                # Step 2: Extract service_id and cloud from record key
                service_id, cloud = extract_key_parts(record.key)
                if not service_id or not cloud:
                    self.logger.warning(
                        f"[offset={record.offset}] Invalid key format: {record.key!r}, "
                        f"skipping message"
                    )
                    self._errors += 1
                    # Skip this message, don't commit
                    continue

                # Step 3: Get or create ServiceAgent
                try:
                    agent = self.agent_registry.get_or_create_agent(
                        service_id,
                        cloud,
                        agent_factory=self._create_service_agent,
                        agent_config=None,
                    )
                except Exception as e:
                    self.logger.error(
                        f"[offset={record.offset}] Failed to create agent "
                        f"(service_id={service_id}, cloud={cloud}): {e}",
                        exc_info=True,
                    )
                    self._errors += 1
                    continue

                # Step 4: Process message via agent
                try:
                    success = agent.process_message(record.topic, record.value)

                    if not success:
                        # Agent returned False - skip commit, message will retry
                        self.logger.debug(
                            f"[{service_id}@{cloud}:offset={record.offset}] "
                            f"process_message returned False, skipping commit"
                        )
                        self._errors += 1
                        continue

                except Exception as e:
                    self.logger.error(
                        f"[{service_id}@{cloud}:offset={record.offset}] "
                        f"Exception during process_message: {e}",
                        exc_info=True,
                    )
                    self._errors += 1
                    # Don't commit - message will retry on restart
                    continue

                # Step 5: On success, commit offset
                try:
                    self.manual_commit_fn(record.topic, record.partition, record.offset)
                    elapsed_ms = (time.time() - start_time) * 1000
                    self._messages_processed += 1
                    self.logger.info(
                        f"[{service_id}@{cloud}:offset={record.offset}] "
                        f"processed and committed in {elapsed_ms:.1f}ms"
                    )

                except Exception as e:
                    self.logger.error(
                        f"[{service_id}@{cloud}:offset={record.offset}] "
                        f"Failed to commit offset: {e}",
                        exc_info=True,
                    )
                    self._errors += 1
                    # Offset commit failed - message will retry on restart

            except Exception as e:
                # Unexpected error in main loop
                self.logger.error(
                    f"Unexpected error in worker loop: {e}",
                    exc_info=True,
                )
                self._errors += 1
                # Continue loop to prevent worker from dying

        self.logger.info(
            f"Worker stopped. Messages processed: {self._messages_processed}, "
            f"Errors: {self._errors}"
        )

    def stop(self):
        """
        Signal worker to stop.

        The worker will exit the run() loop on next iteration.
        """
        self._running = False
        self.logger.info(f"Stop signal sent to partition {self.partition_id} worker")

    def get_stats(self) -> dict:
        """
        Get worker statistics.

        Returns:
            Dict with keys: messages_processed, errors
        """
        return {
            "partition_id": self.partition_id,
            "messages_processed": self._messages_processed,
            "errors": self._errors,
        }

    def _create_service_agent(self, service_id: str, cloud: str, config: dict):
        """
        Factory function for creating ServiceAgent instances.

        This is passed to agent_registry.get_or_create_agent() to lazily
        create agents on first use.

        Args:
            service_id: Service ID.
            cloud: Cloud provider.
            config: Configuration dict (not used currently, kept for future).

        Returns:
            ServiceAgent instance.

        Raises:
            Exception: If ServiceAgent creation fails.
        """
        # Import here to avoid circular imports
        from agents.service_agent.service_agent import ServiceAgent

        # Get cloud-specific producer from registry
        cloud_producer = self.producer_registry.get_or_create_producer(cloud)

        # Create ServiceAgent with cloud-specific producer
        agent = ServiceAgent(
            service_id=service_id,
            cloud=cloud,
            cloud_producer=cloud_producer,  # NEW: pass cloud-specific producer
            # Use default alpha, thresholds, etc.
            alpha=0.3,
            latency_threshold=200.0,
            error_threshold=0.05,
            cpu_threshold=0.8,
            window_size=5,
        )
        return agent
