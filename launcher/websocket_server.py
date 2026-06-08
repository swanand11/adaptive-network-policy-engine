"""WebSocket Server - Real-time streaming of logs, metrics, and events."""

import asyncio
import json
import logging
from typing import Set, Dict, Any
from datetime import datetime
from collections import deque

try:
    import websockets
    from websockets.server import WebSocketServerProtocol
except ImportError:
    websockets = None
    WebSocketServerProtocol = None

logger = logging.getLogger(__name__)


class WebSocketServer:
    """WebSocket server for real-time system monitoring."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8765):
        self.host = host
        self.port = port
        self.clients: Set[WebSocketServerProtocol] = set()
        self.log_buffer: deque = deque(maxlen=1000)
        self.metrics_buffer: deque = deque(maxlen=100)
        self.events_buffer: deque = deque(maxlen=100)
        self._server = None
        # Stored once the async event loop starts so sync threads can schedule
        # coroutines safely via asyncio.run_coroutine_threadsafe.
        self._loop: asyncio.AbstractEventLoop | None = None
        logger.info(f"WebSocketServer initialized on {host}:{port}")

    async def register(self, websocket: WebSocketServerProtocol) -> None:
        """Register a new client connection."""
        self.clients.add(websocket)
        logger.info(
            "Client connected: %s (clients=%s)",
            websocket.remote_address,
            len(self.clients),
        )
        
        # Send buffered data to new client
        await self._send_buffered_data(websocket)

    async def unregister(self, websocket: WebSocketServerProtocol) -> None:
        """Unregister a client connection."""
        self.clients.discard(websocket)
        logger.info(
            "Client disconnected: %s (clients=%s)",
            websocket.remote_address,
            len(self.clients),
        )

    async def _send_buffered_data(self, websocket: WebSocketServerProtocol) -> None:
        """Send buffered data to a newly connected client."""
        try:
            logger.info(
                "Sending buffered data to %s logs=%s metrics=%s events=%s",
                websocket.remote_address,
                len(self.log_buffer),
                len(self.metrics_buffer),
                len(self.events_buffer),
            )
            # Send recent logs
            if self.log_buffer:
                await websocket.send(json.dumps({
                    "type": "logs",
                    "data": list(self.log_buffer)
                }))
            
            # Send recent metrics
            if self.metrics_buffer:
                await websocket.send(json.dumps({
                    "type": "metrics",
                    "data": list(self.metrics_buffer)
                }))
            
            # Send recent events
            if self.events_buffer:
                await websocket.send(json.dumps({
                    "type": "events",
                    "data": list(self.events_buffer)
                }))
        except Exception as e:
            logger.exception("Error sending buffered data: %s", e)

    async def broadcast(self, message_type: str, data: Any) -> None:
        """Broadcast message to all connected clients.
        
        Args:
            message_type: Type of message (logs, metrics, events, health)
            data: Message data
        """
        if not self.clients:
            logger.info(
                "Broadcast skipped type=%s reason=no_clients",
                message_type,
            )
            return

        message = json.dumps({
            "type": message_type,
            "data": data,
            "timestamp": datetime.now().isoformat()
        })
        logger.info(
            "Broadcast triggered type=%s clients=%s",
            message_type,
            len(self.clients),
        )

        # Buffer the message
        if message_type == "logs":
            self.log_buffer.append(data)
        elif message_type == "metrics":
            self.metrics_buffer.append(data)
        elif message_type == "events":
            self.events_buffer.append(data)

        # Broadcast to all clients
        disconnected = set()
        for client in self.clients:
            try:
                logger.debug(
                    "websocket.send called type=%s client=%s",
                    message_type,
                    client.remote_address,
                )
                await client.send(message)
                logger.debug(
                    "websocket.send success type=%s client=%s",
                    message_type,
                    client.remote_address,
                )
            except websockets.exceptions.ConnectionClosed:
                logger.warning(
                    "websocket.send failed type=%s client=%s reason=connection_closed",
                    message_type,
                    client.remote_address,
                )
                disconnected.add(client)
            except Exception as e:
                logger.exception("Error broadcasting to client: %s", e)
                disconnected.add(client)

        # Remove disconnected clients
        for client in disconnected:
            await self.unregister(client)

    async def handle_client(self, websocket: WebSocketServerProtocol, path: str = "/") -> None:
        """Handle client connection and messages.

        Args:
            websocket: WebSocket connection
            path: Request path (optional – websockets v12+ no longer passes this)
        """
        await self.register(websocket)

        try:
            async for message in websocket:
                try:
                    data = json.loads(message)
                    await self._handle_client_message(websocket, data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from client: {message}")
        except Exception:
            # ConnectionClosed and all its subclasses – just clean up silently
            logger.debug("Client handler loop exited for %s", websocket.remote_address)
        finally:
            await self.unregister(websocket)

    async def _handle_client_message(
        self,
        websocket: WebSocketServerProtocol,
        data: Dict[str, Any]
    ) -> None:
        """Handle incoming client message.
        
        Args:
            websocket: Client websocket
            data: Message data
        """
        msg_type = data.get("type")
        
        if msg_type == "ping":
            await websocket.send(json.dumps({"type": "pong"}))
        elif msg_type == "subscribe":
            # Handle subscription requests
            channels = data.get("channels", [])
            logger.info(f"Client subscribed to: {channels}")
        elif msg_type == "get_history":
            # Send historical data
            await self._send_buffered_data(websocket)
        else:
            logger.warning(f"Unknown message type: {msg_type}")

    async def start(self) -> None:
        """Start WebSocket server."""
        if websockets is None:
            logger.error("websockets library not installed, cannot start server")
            return

        # Capture the running event loop so sync helpers can schedule tasks.
        self._loop = asyncio.get_running_loop()

        logger.info(f"Starting WebSocket server on ws://{self.host}:{self.port}")
        self._server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port
        )
        logger.info("WebSocket server started")

    async def stop(self) -> None:
        """Stop WebSocket server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("WebSocket server stopped")

    def _schedule(self, coro) -> None:
        """Schedule a coroutine from any thread.

        Uses ``asyncio.run_coroutine_threadsafe`` when an event loop is
        available (the normal case once :meth:`start` has been called),
        and falls back to buffering-only when the loop is not yet running.
        """
        if self._loop is not None and self._loop.is_running():
            future = asyncio.run_coroutine_threadsafe(coro, self._loop)

            def _done_cb(done_future):
                try:
                    done_future.result()
                except Exception as exc:
                    logger.exception("Scheduled websocket coroutine failed: %s", exc)

            future.add_done_callback(_done_cb)
            return
        # If the loop is not running yet, the data will appear in the next
        # ``_send_buffered_data`` call when a client connects.
        logger.warning("WebSocket loop not running; message buffered only")

    def add_log(self, level: str, service: str, message: str) -> None:
        """Add log entry (sync wrapper for async broadcast)."""
        log_entry = {
            "level": level,
            "service": service,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        }
        self.log_buffer.append(log_entry)
        self._schedule(self.broadcast("logs", log_entry))

    def add_metrics(self, metrics: Dict[str, Any]) -> None:
        """Add metrics data (sync wrapper for async broadcast)."""
        metrics_entry = {**metrics, "timestamp": datetime.now().isoformat()}
        self.metrics_buffer.append(metrics_entry)
        self._schedule(self.broadcast("metrics", metrics_entry))

    def add_event(self, event_type: str, data: Dict[str, Any]) -> None:
        """Add event (sync wrapper for async broadcast)."""
        event_entry = {
            "event_type": event_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }
        self.events_buffer.append(event_entry)
        self._schedule(self.broadcast("events", event_entry))


class SSEServer:
    """Server-Sent Events server for one-way streaming."""

    def __init__(self, host: str = "0.0.0.0", port: int = 8766):
        self.host = host
        self.port = port
        self.log_buffer: deque = deque(maxlen=1000)
        logger.info(f"SSEServer initialized on {host}:{port}")

    async def stream_logs(self, request) -> None:
        """Stream logs via SSE."""
        # This would integrate with aiohttp or similar
        pass

    async def stream_metrics(self, request) -> None:
        """Stream metrics via SSE."""
        pass

    async def stream_health(self, request) -> None:
        """Stream health status via SSE."""
        pass
