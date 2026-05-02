"""
Kafka Dispatcher

Routes incoming ConsumerRecords to partition-specific queues in a non-blocking manner.
Handles backpressure by dropping messages when queues are full, with alerting.
"""

import logging
from queue import Full
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class Dispatcher:
    """
    Non-blocking dispatcher for routing records to partition queues.

    Dispatches incoming Kafka ConsumerRecords to partition-specific queues
    without blocking. Handles queue overflow by dropping messages and alerting.
    """

    def __init__(self):
        """Initialize the Dispatcher."""
        self._dropped_count: Dict[int, int] = {}
        logger.info("Dispatcher initialized")

    def dispatch(self, record, partition_queues: Dict[int, object]) -> bool:
        """
        Dispatch a ConsumerRecord to the appropriate partition queue.

        Non-blocking operation. If queue is full, logs alert and returns False
        (message is dropped).

        Args:
            record: Kafka ConsumerRecord with attributes:
                   - partition: int, target partition
                   - offset: int, message offset
                   - value: bytes/str, message value
            partition_queues: Dict[int, Queue] from PartitionQueueManager.

        Returns:
            True if message was queued successfully.
            False if queue was full and message was dropped.

        Raises:
            KeyError: If partition not found in partition_queues.
            (Shouldn't happen if queues created for all assigned partitions)
        """
        partition = record.partition
        offset = record.offset

        # Validate partition exists in queue map
        if partition not in partition_queues:
            logger.warning(
                f"Partition {partition} not in queue map. "
                f"This may indicate rebalancing. Dropping record offset={offset}"
            )
            self._track_dropped(partition)
            return False

        queue = partition_queues[partition]

        try:
            # Non-blocking put: raises Full if queue is full
            queue.put(record, block=False)
            return True

        except Full:
            # Queue is full - drop and alert
            self._track_dropped(partition)
            logger.warning(
                f"Queue full for partition {partition}, dropping record offset={offset}. "
                f"Queue depth: {queue.qsize()}, total dropped on this partition: {self._dropped_count[partition]}"
            )
            return False

    def get_dropped_count(self, partition: Optional[int] = None) -> int:
        """
        Get count of dropped messages.

        Args:
            partition: If specified, returns dropped count for that partition.
                      If None, returns total across all partitions.

        Returns:
            Number of dropped messages.
        """
        if partition is not None:
            return self._dropped_count.get(partition, 0)

        return sum(self._dropped_count.values())

    def get_all_dropped_counts(self) -> Dict[int, int]:
        """
        Get dropped message counts for all partitions.

        Returns:
            Dict mapping partition_id to dropped count.
        """
        return dict(self._dropped_count)

    def reset_dropped_counts(self):
        """Reset all dropped message counters."""
        self._dropped_count.clear()
        logger.info("Reset dropped message counters")

    def _track_dropped(self, partition: int):
        """Internal: increment dropped counter for a partition."""
        if partition not in self._dropped_count:
            self._dropped_count[partition] = 0
        self._dropped_count[partition] += 1
