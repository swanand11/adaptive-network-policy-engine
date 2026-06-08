"""WebSocket Handler - Real-time streaming to UI clients

Manages WebSocket connections and broadcasts system events.
"""

import logging
import asyncio
import json
from typing import Set
from datetime import datetime

try:
    from fastapi import WebSocket, WebSocketDisconnect
except ImportError:
    WebSocket = None
    WebSocketDisconnect = None

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and broadcasts."""

    def __init__(self):
        """Initialize connection manager."""
        self.active_connections: Set[WebSocket] = set()
        self._lock = asyncio.Lock()
        logger.info("ConnectionManager initialized")

    async def connect(self, websocket: WebSocket) -> None:
        """Accept and register a new WebSocket connection.
        
        Args:
            websocket: WebSocket connection
        """
        await websocket.accept()
        async with self._lock:
            self.active_connections.add(websocket)
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection.
        
        Args:
            websocket: WebSocket connection
        """
        async with self._lock:
            self.active_connections.discard(websocket)
        logger.info(f"Client disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, message_type: str, data: dict) -> None:
        """Broadcast a message to all connected clients.
        
        Args:
            message_type: Type of message (metrics, decision, approval, etc.)
            data: Message data
        """
        if not self.active_connections:
            return

        message = {
            "type": message_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }
        
        message_json = json.dumps(message)
        
        # Send to all connections
        disconnected = set()
        async with self._lock:
            for connection in self.active_connections:
                try:
                    await connection.send_text(message_json)
                except Exception as e:
                    logger.error(f"Error sending to client: {e}")
                    disconnected.add(connection)
        
        # Remove disconnected clients
        if disconnected:
            async with self._lock:
                self.active_connections -= disconnected
            logger.info(f"Removed {len(disconnected)} disconnected clients")

    async def send_personal(self, websocket: WebSocket, message_type: str, data: dict) -> None:
        """Send a message to a specific client.
        
        Args:
            websocket: WebSocket connection
            message_type: Type of message
            data: Message data
        """
        message = {
            "type": message_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }
        
        try:
            await websocket.send_text(json.dumps(message))
        except Exception as e:
            logger.error(f"Error sending personal message: {e}")
            await self.disconnect(websocket)

    def get_connection_count(self) -> int:
        """Get number of active connections.
        
        Returns:
            Number of active connections
        """
        return len(self.active_connections)
