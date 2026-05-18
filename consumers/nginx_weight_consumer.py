"""NGINX Weight Consumer - Bridge between Kafka and NGINX Lua

Consumes policy.approved from Kafka, normalizes weights to sum=100,
and pushes to Redis for NGINX Lua to read dynamically.

FLOW:
  Kafka policy.approved → Normalize weights → Redis → NGINX Lua

NORMALIZATION:
  Input:  {"aws": 0, "azure": 33, "do": 33}  (sum=66)
  Output: {"aws": 0, "azure": 50, "do": 50}  (sum=100)

REDIS KEY:
  nginx:weights → JSON string of normalized weights

USAGE:
  python consumers/nginx_weight_consumer.py
"""

import json
import logging
import time
from typing import Dict, Optional
from datetime import datetime

try:
    import redis
except ImportError:
    redis = None

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.schemas import PolicyDecision

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WeightNormalizer:
    """Normalize weights to sum to 100."""

    @staticmethod
    def normalize(weights: Dict[str, float]) -> Dict[str, int]:
        """Normalize weights to integer percentages summing to 100.
        
        Args:
            weights: Dict mapping CSP names to weights (can be any positive numbers)
            
        Returns:
            Dict mapping CSP names to integer percentages (sum=100)
            
        Examples:
            >>> normalize({"aws": 0, "azure": 33, "do": 33})
            {"aws": 0, "azure": 50, "do": 50}
            
            >>> normalize({"aws": 70, "azure": 20, "do": 10})
            {"aws": 70, "azure": 20, "do": 10}
        """
        if not weights:
            logger.warning("Empty weights dict, returning empty")
            return {}
        
        total = sum(weights.values())
        
        if total == 0:
            # All zeros - distribute equally
            equal_weight = 100 // len(weights)
            remainder = 100 % len(weights)
            normalized = {csp: equal_weight for csp in weights}
            # Add remainder to first CSP
            first_csp = list(weights.keys())[0]
            normalized[first_csp] += remainder
            logger.info(f"All weights zero, distributing equally: {normalized}")
            return normalized
        
        # Calculate normalized percentages
        normalized = {}
        for csp, weight in weights.items():
            normalized[csp] = round((weight / total) * 100)
        
        # Adjust for rounding errors to ensure sum=100
        current_sum = sum(normalized.values())
        if current_sum != 100:
            diff = 100 - current_sum
            # Add/subtract difference to CSP with highest weight
            max_csp = max(normalized, key=normalized.get)
            normalized[max_csp] += diff
            logger.debug(f"Adjusted {max_csp} by {diff} to ensure sum=100")
        
        logger.info(f"Normalized weights: {weights} → {normalized} (sum={sum(normalized.values())})")
        return normalized


class NGINXWeightConsumer(KafkaConsumerTemplate):
    """Consumer that normalizes policy decisions and pushes to Redis for NGINX."""

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_key: str = "nginx:weights",
        group_id: str = "nginx_weight_consumer"
    ):
        """Initialize NGINX weight consumer.
        
        Args:
            redis_host: Redis host
            redis_port: Redis port
            redis_db: Redis database number
            redis_key: Redis key for storing weights
            group_id: Kafka consumer group ID
        """
        super().__init__(topics=["policy.approved"], group_id=group_id)
        
        self.redis_key = redis_key
        self.normalizer = WeightNormalizer()
        
        # Initialize Redis connection
        if redis is None:
            raise ImportError("redis-py not installed. Install with: pip install redis")
        
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
        
        logger.info(
            f"NGINXWeightConsumer initialized: redis_key={redis_key}, group_id={group_id}"
        )

    def process_message(self, topic: str, message: Dict) -> bool:
        """Process approved policy decision and update NGINX weights.
        
        Args:
            topic: Topic name ("policy.approved")
            message: PolicyDecisionValue dict with metadata containing weights
            
        Returns:
            True to commit offset, False to nack
        """
        try:
            # Extract weights from metadata
            metadata = message.get("metadata", {})
            weights = metadata.get("weights")
            
            if not weights:
                logger.warning("No weights found in policy decision metadata, skipping")
                return True
            
            # Extract additional context
            decision_id = message.get("decision_id", "unknown")
            risk_level = message.get("risk_level", "unknown")
            status = message.get("status", "unknown")
            kl_divergence = metadata.get("kl_divergence", 0.0)
            
            logger.info(
                f"Processing policy decision: id={decision_id}, "
                f"risk={risk_level}, status={status}, kl={kl_divergence:.6f}"
            )
            logger.info(f"Raw weights: {weights}")
            
            # Normalize weights
            normalized_weights = self.normalizer.normalize(weights)
            
            # Prepare data for Redis
            redis_data = {
                "upstreams": normalized_weights,
                "updated_at": datetime.now().isoformat(),
                "decision_id": decision_id,
                "risk_level": risk_level,
                "kl_divergence": kl_divergence
            }
            
            # Push to Redis
            self.redis_client.set(
                self.redis_key,
                json.dumps(redis_data)
            )
            
            logger.info(f"✓ Updated NGINX weights in Redis: {normalized_weights}")
            
            # Also publish to a Redis pub/sub channel for real-time updates
            self.redis_client.publish(
                "nginx:weight_updates",
                json.dumps(redis_data)
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing policy decision: {e}", exc_info=True)
            return False

    def get_current_weights(self) -> Optional[Dict]:
        """Get current weights from Redis.
        
        Returns:
            Current weights dict or None if not found
        """
        try:
            data = self.redis_client.get(self.redis_key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Error reading weights from Redis: {e}")
            return None

    def close(self) -> None:
        """Graceful shutdown."""
        logger.info("Closing NGINX Weight Consumer")
        if self.redis_client:
            self.redis_client.close()
        super().close()


def main():
    """Main entry point."""
    import os
    
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_db = int(os.getenv("REDIS_DB", "0"))
    redis_key = os.getenv("REDIS_KEY", "nginx:weights")
    
    logger.info("Starting NGINX Weight Consumer")
    logger.info(f"Redis: {redis_host}:{redis_port}/{redis_db}")
    logger.info(f"Redis Key: {redis_key}")
    
    consumer = NGINXWeightConsumer(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_db=redis_db,
        redis_key=redis_key
    )
    
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Received interrupt, shutting down")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
