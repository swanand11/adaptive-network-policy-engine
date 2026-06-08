"""Backend API - FastAPI server for Multi-Cloud Policy Orchestrator

Provides REST API and WebSocket streaming for the React UI.

Endpoints:
  GET  /health                    - Health check
  GET  /api/csp/health            - Get CSP health status
  GET  /api/metrics               - Get recent metrics
  GET  /api/decisions             - Get recent decisions
  GET  /api/approvals             - Get recent approvals
  GET  /api/weights               - Get current weights
  GET  /api/stats                 - Get system statistics
  WS   /ws                        - WebSocket for real-time updates
"""

import logging
import asyncio
import os
from typing import Optional
from datetime import datetime
from contextlib import asynccontextmanager

try:
    from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    import uvicorn
except ImportError:
    FastAPI = None
    WebSocket = None
    WebSocketDisconnect = None
    CORSMiddleware = None
    uvicorn = None
    HTTPException = None

from .state_cache import StateCache
from .websocket import ConnectionManager
from .kafka_stream import KafkaStreamBridge

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Global instances
state_cache = StateCache()
connection_manager = ConnectionManager()
kafka_bridge: Optional[KafkaStreamBridge] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown."""
    global kafka_bridge
    
    # Startup
    logger.info("Starting Backend API")
    
    # Start Kafka stream bridge
    kafka_bridge = KafkaStreamBridge(state_cache, connection_manager.broadcast)
    asyncio.create_task(kafka_bridge.start())
    
    logger.info("Backend API started")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Backend API")
    if kafka_bridge:
        await kafka_bridge.stop()
    logger.info("Backend API shutdown complete")


def create_app() -> FastAPI:
    """Create and configure FastAPI application.
    
    Returns:
        FastAPI app instance
    """
    if FastAPI is None:
        raise ImportError("FastAPI not installed. Install with: pip install fastapi uvicorn")
    
    app = FastAPI(
        title="Multi-Cloud Policy Orchestrator API",
        description="Backend API for Multi-Agent Multi-Cloud Network Policy Orchestrator",
        version="1.0.0",
        lifespan=lifespan,
    )
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # In production, specify allowed origins
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["*"],
        max_age=3600,
    )
    
    # Health check
    @app.get("/health")
    async def health():
        """Health check endpoint."""
        return {
            "status": "healthy",
            "service": "backend-api",
            "timestamp": datetime.now().isoformat(),
            "connections": connection_manager.get_connection_count(),
        }
    
    # CSP health status
    @app.get("/api/csp/health")
    async def get_csp_health():
        """Get health status of all CSPs."""
        try:
            health = state_cache.get_csp_health()
            return {
                "status": "success",
                "data": health,
            }
        except Exception as e:
            logger.error(f"Error getting CSP health: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # Recent metrics
    @app.get("/api/metrics")
    async def get_metrics(csp: Optional[str] = None, limit: int = 50):
        """Get recent metrics.
        
        Args:
            csp: Optional CSP name to filter
            limit: Maximum number of items (default: 50)
        """
        try:
            metrics = state_cache.get_recent_metrics(csp, limit)
            return {
                "status": "success",
                "data": metrics,
            }
        except Exception as e:
            logger.error(f"Error getting metrics: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # Recent decisions
    @app.get("/api/decisions")
    async def get_decisions(limit: int = 50):
        """Get recent policy decisions.
        
        Args:
            limit: Maximum number of items (default: 50)
        """
        try:
            decisions = state_cache.get_recent_decisions(limit)
            return {
                "status": "success",
                "data": decisions,
                "count": len(decisions),
            }
        except Exception as e:
            logger.error(f"Error getting decisions: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # Recent approvals
    @app.get("/api/approvals")
    async def get_approvals(limit: int = 50):
        """Get recent approvals.
        
        Args:
            limit: Maximum number of items (default: 50)
        """
        try:
            approvals = state_cache.get_recent_approvals(limit)
            return {
                "status": "success",
                "data": approvals,
                "count": len(approvals),
            }
        except Exception as e:
            logger.error(f"Error getting approvals: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # Current weights
    @app.get("/api/weights")
    async def get_weights():
        """Get current traffic distribution weights."""
        try:
            weights = state_cache.get_current_weights()
            return {
                "status": "success",
                "data": weights,
            }
        except Exception as e:
            logger.error(f"Error getting weights: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # System statistics
    @app.get("/api/stats")
    async def get_stats():
        """Get system statistics."""
        try:
            stats = state_cache.get_stats()
            return {
                "status": "success",
                "data": stats,
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    # WebSocket endpoint
    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        """WebSocket endpoint for real-time updates."""
        await connection_manager.connect(websocket)
        
        try:
            # Send initial state
            await connection_manager.send_personal(
                websocket,
                "initial_state",
                {
                    "csp_health": state_cache.get_csp_health(),
                    "weights": state_cache.get_current_weights(),
                    "stats": state_cache.get_stats(),
                }
            )
            
            # Keep connection alive and handle incoming messages
            while True:
                try:
                    data = await websocket.receive_text()
                    logger.debug(f"Received from client: {data}")
                    
                    # Echo back (or handle commands)
                    await connection_manager.send_personal(
                        websocket,
                        "echo",
                        {"message": data}
                    )
                    
                except WebSocketDisconnect:
                    break
                except Exception as e:
                    logger.error(f"Error in WebSocket loop: {e}")
                    break
        
        finally:
            await connection_manager.disconnect(websocket)
    
    return app


def main():
    """Main entry point."""
    port = int(os.getenv("BACKEND_PORT", "8000"))
    host = os.getenv("BACKEND_HOST", "0.0.0.0")
    
    logger.info("=" * 80)
    logger.info("Multi-Cloud Policy Orchestrator - Backend API")
    logger.info("=" * 80)
    logger.info(f"Starting server on http://{host}:{port}")
    logger.info("Endpoints:")
    logger.info(f"  GET  http://{host}:{port}/health")
    logger.info(f"  GET  http://{host}:{port}/api/csp/health")
    logger.info(f"  GET  http://{host}:{port}/api/metrics")
    logger.info(f"  GET  http://{host}:{port}/api/decisions")
    logger.info(f"  GET  http://{host}:{port}/api/approvals")
    logger.info(f"  GET  http://{host}:{port}/api/weights")
    logger.info(f"  GET  http://{host}:{port}/api/stats")
    logger.info(f"  WS   ws://{host}:{port}/ws")
    logger.info("=" * 80)
    
    app = create_app()
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
