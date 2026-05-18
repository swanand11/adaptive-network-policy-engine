"""Kafka Stream - Bridge between Kafka and WebSocket

Consumes messages from Kafka topics and broadcasts to WebSocket clients.
"""

import logging
import asyncio
import json
from typing import Set, Callable, Optional
from kafka_core.consumer_base import KafkaConsumerTemplate

logger = logging.getLogger(__name__)


class KafkaStreamBridge:
    """Bridge between Kafka consumers and WebSocket broadcasts."""

    def __init__(self, state_cache, broadcast_callback: Callable):
        """Initialize Kafka stream bridge.
        
        Args:
            state_cache: StateCache instance
            broadcast_callback: Async function to broadcast messages
        """
        self.state_cache = state_cache
        self.broadcast_callback = broadcast_callback
        self._running = False
        self._consumers = []
        
        logger.info("KafkaStreamBridge initialized")

    async def start(self) -> None:
        """Start consuming from Kafka topics."""
        self._running = True
        
        # Start consumers in background tasks
        tasks = [
            asyncio.create_task(self._consume_metrics()),
            asyncio.create_task(self._consume_decisions()),
            asyncio.create_task(self._consume_approvals()),
            asyncio.create_task(self._consume_weights()),
        ]
        
        logger.info("Started Kafka stream consumers")
        
        # Wait for all tasks
        await asyncio.gather(*tasks, return_exceptions=True)

    async def stop(self) -> None:
        """Stop consuming from Kafka."""
        self._running = False
        
        # Close all consumers
        for consumer in self._consumers:
            try:
                consumer.close()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")
        
        logger.info("Stopped Kafka stream consumers")

    async def _consume_metrics(self) -> None:
        """Consume metrics.events topic."""
        try:
            loop = asyncio.get_event_loop()
            consumer = MetricsConsumer(self.state_cache, self.broadcast_callback, loop)
            self._consumers.append(consumer)
            
            # Run in executor to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, consumer.start)
            
        except Exception as e:
            logger.error(f"Error in metrics consumer: {e}", exc_info=True)

    async def _consume_decisions(self) -> None:
        """Consume policy.decisions topic."""
        try:
            loop = asyncio.get_event_loop()
            consumer = DecisionsConsumer(self.state_cache, self.broadcast_callback, loop)
            self._consumers.append(consumer)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, consumer.start)
            
        except Exception as e:
            logger.error(f"Error in decisions consumer: {e}", exc_info=True)

    async def _consume_approvals(self) -> None:
        """Consume policy.approved topic."""
        try:
            loop = asyncio.get_event_loop()
            consumer = ApprovalsConsumer(self.state_cache, self.broadcast_callback, loop)
            self._consumers.append(consumer)
            
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, consumer.start)
            
        except Exception as e:
            logger.error(f"Error in approvals consumer: {e}", exc_info=True)

    async def _consume_weights(self) -> None:
        """Monitor Redis for weight updates."""
        try:
            import redis
            redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
            
            while self._running:
                try:
                    # Get current weights from Redis
                    weights_str = redis_client.get('nginx:weights')
                    if weights_str:
                        weights_data = json.loads(weights_str)
                        upstreams = weights_data.get('upstreams', {})
                        
                        # Update cache
                        self.state_cache.update_weights(upstreams)
                        
                        # Broadcast to WebSocket clients
                        await self.broadcast_callback('weights', upstreams)
                    
                    await asyncio.sleep(2)  # Check every 2 seconds
                    
                except Exception as e:
                    logger.error(f"Error reading weights from Redis: {e}")
                    await asyncio.sleep(5)
                    
        except Exception as e:
            logger.error(f"Error in weights monitor: {e}", exc_info=True)


class MetricsConsumer(KafkaConsumerTemplate):
    """Consumer for metrics.events topic."""

    def __init__(self, state_cache, broadcast_callback, loop):
        super().__init__(topics=["metrics.events"], group_id="backend_metrics_consumer")
        self.state_cache = state_cache
        self.broadcast_callback = broadcast_callback
        self.loop = loop

    def process_message(self, topic: str, message: dict) -> bool:
        """Process metrics message."""
        try:
            # Extract CSP and metrics
            csp = message.get("cloud", "unknown")
            metrics = {
                "cpu": message.get("cpu_usage_percent", 0),
                "memory": message.get("memory_usage_percent", 0),
                "latency": message.get("latency_ms", 0),
                "error_rate": message.get("error_rate_percent", 0),
            }
            
            # Update cache
            self.state_cache.add_metrics(csp, metrics)
            
            # Determine health status
            if metrics["error_rate"] > 10 or metrics["cpu"] > 90:
                status = "unhealthy"
            elif metrics["error_rate"] > 5 or metrics["cpu"] > 70:
                status = "degraded"
            else:
                status = "healthy"
            
            self.state_cache.update_csp_health(csp, status, metrics)
            
            # Broadcast to WebSocket (thread-safe)
            asyncio.run_coroutine_threadsafe(
                self.broadcast_callback('metrics', {
                    "csp": csp,
                    "metrics": metrics,
                    "status": status,
                }),
                self.loop
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing metrics: {e}")
            return False


class DecisionsConsumer(KafkaConsumerTemplate):
    """Consumer for policy.decisions topic."""

    def __init__(self, state_cache, broadcast_callback, loop):
        super().__init__(topics=["policy.decisions"], group_id="backend_decisions_consumer")
        self.state_cache = state_cache
        self.broadcast_callback = broadcast_callback
        self.loop = loop

    def process_message(self, topic: str, message: dict) -> bool:
        """Process decision message."""
        try:
            decision = {
                "decision_id": message.get("decision_id"),
                "risk_level": message.get("risk_level"),
                "status": message.get("status"),
                "metadata": message.get("metadata", {}),
            }
            
            # Update cache
            self.state_cache.add_decision(decision)
            
            # Broadcast to WebSocket (thread-safe)
            asyncio.run_coroutine_threadsafe(
                self.broadcast_callback('decision', decision),
                self.loop
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing decision: {e}")
            return False


class ApprovalsConsumer(KafkaConsumerTemplate):
    """Consumer for policy.approved topic."""

    def __init__(self, state_cache, broadcast_callback, loop):
        super().__init__(topics=["policy.approved"], group_id="backend_approvals_consumer")
        self.state_cache = state_cache
        self.broadcast_callback = broadcast_callback
        self.loop = loop

    def process_message(self, topic: str, message: dict) -> bool:
        """Process approval message."""
        try:
            approval = {
                "approval_id": message.get("decision_id"),
                "status": "approved",
                "metadata": message.get("metadata", {}),
            }
            
            # Update cache
            self.state_cache.add_approval(approval)
            
            # Broadcast to WebSocket (thread-safe)
            asyncio.run_coroutine_threadsafe(
                self.broadcast_callback('approval', approval),
                self.loop
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing approval: {e}")
            return False
