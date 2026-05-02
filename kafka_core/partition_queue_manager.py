"""
Partition Queue Manager

Manages a thread-safe mapping of Kafka partition IDs to message queues.
Each partition gets an isolated queue for backpressure and ordering.

One queue per partition ensures strict message ordering within partition.
"""

import logging
import threading
from queue import Queue
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class PartitionQueueManager:
    """
    Thread-safe manager for partition-level message queues.
    
    Maintains a Dict[int, Queue] mapping where each queue buffers messages
    for a specific Kafka partition. Ensures isolated processing per partition
    while allowing parallel queue operations.
    """

    def __init__(self, default_maxsize: int = 1000):
        """
        Initialize the PartitionQueueManager.

        Args:
            default_maxsize: Default maximum size for each partition queue.
                           When a queue reaches this size, further puts will
                           raise queue.Full unless called with block=False.
        """
        self._queues: Dict[int, Queue] = {}
        self._lock = threading.Lock()
        self._default_maxsize = default_maxsize
        logger.info(f"PartitionQueueManager initialized with max_queue_size={default_maxsize}")

    def get_or_create_queue(self, partition_id: int, maxsize: Optional[int] = None) -> Queue:
        """
        Get or create a queue for a specific partition.

        Thread-safe lazy initialization. If queue doesn't exist, creates it with
        configured maxsize.

        Args:
            partition_id: Kafka partition ID (non-negative integer).
            maxsize: Queue maximum size. If None, uses default_maxsize.

        Returns:
            Queue instance for this partition.

        Raises:
            ValueError: If partition_id is negative.
        """
        if partition_id < 0:
            raise ValueError(f"partition_id must be non-negative, got {partition_id}")

        if partition_id in self._queues:
            return self._queues[partition_id]

        with self._lock:
            # Double-check inside lock
            if partition_id in self._queues:
                return self._queues[partition_id]

            queue_size = maxsize if maxsize is not None else self._default_maxsize
            queue = Queue(maxsize=queue_size)
            self._queues[partition_id] = queue
            logger.info(f"Created queue for partition {partition_id} (maxsize={queue_size})")
            return queue

    def get_queue(self, partition_id: int) -> Optional[Queue]:
        """
        Get an existing queue for a partition, or None if not created.

        Args:
            partition_id: Kafka partition ID.

        Returns:
            Queue if partition has been initialized, None otherwise.
        """
        return self._queues.get(partition_id)

    def clear_partition(self, partition_id: int) -> int:
        """
        Clear all messages from a partition's queue.

        Useful during rebalancing when a partition is revoked.

        Args:
            partition_id: Partition ID to clear.

        Returns:
            Number of messages that were in the queue before clearing.
        """
        with self._lock:
            if partition_id not in self._queues:
                return 0

            queue = self._queues[partition_id]
            count = 0
            while not queue.empty():
                try:
                    queue.get_nowait()
                    count += 1
                except Exception:
                    break

            logger.info(f"Cleared partition {partition_id} queue ({count} messages removed)")
            return count

    def clear_all(self) -> int:
        """
        Clear all queues (useful during graceful shutdown).

        Returns:
            Total number of messages cleared across all queues.
        """
        with self._lock:
            total_cleared = 0
            for partition_id in list(self._queues.keys()):
                queue = self._queues[partition_id]
                while not queue.empty():
                    try:
                        queue.get_nowait()
                        total_cleared += 1
                    except Exception:
                        break

            logger.info(f"Cleared all partition queues ({total_cleared} messages removed)")
            return total_cleared

    def get_queue_sizes(self) -> Dict[int, int]:
        """
        Get current queue sizes for all partitions.

        Useful for monitoring and debugging.

        Returns:
            Dict mapping partition_id to current queue size.
        """
        with self._lock:
            return {partition_id: queue.qsize() for partition_id, queue in self._queues.items()}

    def get_partition_ids(self) -> list:
        """
        Get list of all partition IDs that have been initialized.

        Returns:
            List of partition IDs.
        """
        with self._lock:
            return list(self._queues.keys())

    def close(self):
        """
        Close the queue manager and clear all queues.
        
        Call during graceful shutdown to ensure clean cleanup.
        """
        self.clear_all()
        with self._lock:
            self._queues.clear()
        logger.info("PartitionQueueManager closed")
