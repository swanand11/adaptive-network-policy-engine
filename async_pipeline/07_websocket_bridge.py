import sys
import os
import asyncio
from pathlib import Path
import logging

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"

from launcher.websocket_server import WebSocketServer
from launcher.unified_pipeline import KafkaWebSocketBridge

async def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger("async_bridge")
    
    ws_host = os.environ.get("WS_HOST", "0.0.0.0")
    ws_port = int(os.environ.get("WS_PORT", "8765"))
    
    print("🚀 Starting WebSocket Server & Kafka Bridge for UI...")
    
    ws_server = WebSocketServer(host=ws_host, port=ws_port)
    await ws_server.start()
    
    bridge = KafkaWebSocketBridge(
        ws_server=ws_server,
        group_id="ui_ws_bridge_async",
        bootstrap_servers="localhost:9092"
    )
    bridge.start()
    
    print(f"✅ Bridge is running on ws://{ws_host}:{ws_port}. The UI will now receive real-time updates.")
    print("Press Ctrl+C to stop.")
    
    stop_event = asyncio.Event()
    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        bridge.stop()
        await ws_server.stop()

if __name__ == "__main__":
    asyncio.run(main())
