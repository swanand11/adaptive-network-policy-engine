"""
Cloud Producer Registry

Manages cloud-specific Kafka producers (one per cloud provider).
Implements lazy initialization and thread-safe access.

Design:
- One producer per cloud provider (AWS, GCP, Azure)
- All agents for the same cloud share one producer
- librdkafka KafkaProducer is thread-safe for concurrent send() calls
- No explicit locking needed for send() - only for lazy initialization
"""

import logging
import threading
from typing import Dict, Optional

from kafka_core.producer_base import KafkaProducerTemplate

logger = logging.getLogger(__name__)


class CloudProducerRegistry:
    """
    Thread-safe registry for cloud-specific Kafka producers.

    Maintains a Dict[cloud, KafkaProducerTemplate] mapping where each cloud
    provider gets one producer instance. Multiple agents for the same cloud
    share the same producer.

    Lazy initialization: producers created on first use via
    get_or_create_producer(cloud).

    Thread Safety:
    - Uses double-check locking pattern for initialization
    - KafkaProducer.send() is thread-safe (librdkafka handles concurrency)
    - No explicit locking needed for send() calls
    """

    def __init__(self):
        """Initialize the CloudProducerRegistry."""
        self._producers: Dict[str, KafkaProducerTemplate] = {}
        self._lock = threading.Lock()
        logger.info("CloudProducerRegistry initialized")

    def get_or_create_producer(self, cloud: str) -> KafkaProducerTemplate:
        """
        Get or create a producer for a specific cloud provider.

        Thread-safe lazy initialization. If producer doesn't exist for this cloud,
        creates it with standard configuration. Multiple calls for the same cloud
        return the same producer instance.

        Args:
            cloud: Cloud provider name (e.g., "aws", "gcp", "azure").
                  Case-insensitive - normalized to lowercase.

        Returns:
            KafkaProducerTemplate instance for this cloud.

        Raises:
            ValueError: If cloud is empty or None.
            KafkaProducerError: If producer creation fails.
        """
        if not cloud:
            raise ValueError("cloud must be non-empty")

        # Normalize cloud name to lowercase
        cloud = cloud.lower()

        # Fast path: check without lock (double-check pattern)
        if cloud in self._producers:
            return self._producers[cloud]

        # Slow path: create with lock
        with self._lock:
            # Double-check inside lock
            if cloud in self._producers:
                return self._producers[cloud]

            # Create new producer for this cloud
            try:
                producer = KafkaProducerTemplate()
                self._producers[cloud] = producer
                logger.info(f"Created producer for cloud: {cloud}")
                return producer
            except Exception as e:
                logger.error(f"Failed to create producer for cloud {cloud}: {e}", exc_info=True)
                raise

    def get_producer(self, cloud: str) -> Optional[KafkaProducerTemplate]:
        """
        Get an existing producer, or None if not yet created.

        Non-blocking. Does not create producer if it doesn't exist.

        Args:
            cloud: Cloud provider name.

        Returns:
            KafkaProducerTemplate if exists, None otherwise.
        """
        return self._producers.get(cloud.lower())

    def get_all_producers(self) -> Dict[str, KafkaProducerTemplate]:
        """
        Get all registered cloud producers.

        Returns:
            Dict mapping cloud name to KafkaProducerTemplate.
        """
        with self._lock:
            return dict(self._producers)

    def close_all(self):
        """
        Close all cloud producers.

        Call during graceful shutdown. Calls producer.close() on each instance.
        """
        with self._lock:
            producers_to_close = list(self._producers.items())

        for cloud, producer in producers_to_close:
            try:
                producer.close()
                logger.info(f"Closed producer for cloud: {cloud}")
            except Exception as e:
                logger.error(f"Error closing producer for cloud {cloud}: {e}", exc_info=True)

        with self._lock:
            self._producers.clear()

        logger.info("CloudProducerRegistry closed, all producers shut down")

    def get_count(self) -> int:
        """
        Get count of initialized cloud producers.

        Returns:
            Number of cloud producers currently in registry.
        """
        return len(self._producers)

    def get_clouds(self) -> list:
        """
        Get list of all cloud providers with initialized producers.

        Returns:
            List of cloud names.
        """
        with self._lock:
            return list(self._producers.keys())
