"""
Kafka Core Utilities

Helper functions for kafka_core module.
"""

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def extract_key_parts(key: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract service_id and cloud from a Kafka record key.

    Expected key format: "service-id@cloud" (e.g., "service-api@aws")

    Args:
        key: Record key string. Can be None.

    Returns:
        Tuple of (service_id, cloud) if key is valid.
        Tuple of (None, None) if key is None or invalid format.

    Example:
        >>> extract_key_parts("service-api@aws")
        ('service-api', 'aws')

        >>> extract_key_parts("invalid")
        (None, None)

        >>> extract_key_parts(None)
        (None, None)
    """
    if not key:
        logger.debug(f"Key is None or empty")
        return None, None

    try:
        parts = key.split("@", 1)
        if len(parts) != 2:
            logger.warning(f"Key '{key}' does not contain '@' separator, expected format: 'service-id@cloud'")
            return None, None

        service_id, cloud = parts
        if not service_id or not cloud:
            logger.warning(f"Key '{key}' has empty service_id or cloud after split")
            return None, None

        return service_id, cloud

    except Exception as e:
        logger.error(f"Error extracting key parts from '{key}': {e}")
        return None, None
