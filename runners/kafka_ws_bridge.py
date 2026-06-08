import logging
import threading
import time
from typing import Dict, Any

from kafka_core.consumer_base import KafkaConsumerTemplate

logger = logging.getLogger("kafka_ws_bridge")

class KafkaWebSocketBridge:
    """Consumes Kafka events and broadcasts them via WebSocketServer."""

    def __init__(self, ws_server, group_id: str = "ui_websocket_bridge"):
        self.ws_server = ws_server
        self.group_id = group_id
        
        # Subscribe to all relevant UI topics
        self.topics = [
            "metrics.events",
            "service.state",
            "topo.decisions",
            "policy.decisions",
            "policy.approved"
        ]
        
        self.consumer = KafkaConsumerTemplate(
            group_id=self.group_id,
            topics=self.topics,
            auto_offset_reset="latest"
        )
        self._stop_event = threading.Event()
        self.thread = None

    def start(self):
        """Start the bridge consumer in a background thread."""
        if self.thread and self.thread.is_alive():
            return
            
        self._stop_event.clear()
        self.thread = threading.Thread(
            target=self._run,
            name="KafkaWebSocketBridgeThread",
            daemon=True
        )
        self.thread.start()
        logger.info(f"✓ Started KafkaWebSocketBridge listening to {self.topics}")

    def stop(self):
        """Stop the consumer loop."""
        self._stop_event.set()
        if self.thread:
            self.thread.join(timeout=2.0)
        self.consumer.close()
        logger.info("KafkaWebSocketBridge stopped")

    def _run(self):
        """Main consumer loop."""
        while not self._stop_event.is_set():
            try:
                # Poll for messages
                messages = self.consumer.poll(timeout_ms=100)
                
                for topic, msg_dict in messages:
                    # Determine how to format the data for the frontend
                    data = msg_dict
                    
                    # Also append topic to data so frontend knows what event it is
                    if isinstance(data, dict):
                        data["topic"] = topic
                    
                    if topic == "metrics.events":
                        # Some topics might go to metrics, some to events
                        self.ws_server.add_metrics(data)
                        self.ws_server.add_event(topic, data)
                    else:
                        self.ws_server.add_event(topic, data)
                        
            except Exception as e:
                logger.error(f"Error in KafkaWebSocketBridge consumer loop: {e}", exc_info=True)
                time.sleep(1) # Prevent tight loop on error
