#!/usr/bin/env python3
"""Unified Pipeline Launcher
=====================================================================
Starts the complete Adaptive Network Policy Engine pipeline:

  1.  Docker Compose  (Kafka, Zookeeper, Redis, Mongo, Prometheus,
                       mock cloud simulators, adapters)
  2.  WebSocket server  – ws://0.0.0.0:8765  (real-time UI feed)
  3.  Kafka → WebSocket bridge  – mirrors every Kafka topic into WS
                                   messages so the React UI updates
                                   in real time

Usage
-----
  python -m launcher.unified_pipeline            # start Docker + pipeline
  python -m launcher.unified_pipeline --no-docker  # skip docker compose up (stack already running)
  python -m launcher.unified_pipeline --down     # stop Docker stack on Ctrl-C

Environment overrides
---------------------
  KAFKA_BOOTSTRAP_SERVERS   default: localhost:9092
  WS_HOST                   default: 0.0.0.0
  WS_PORT                   default: 8765
  KAFKA_BRIDGE_GROUP        default: ui_ws_bridge
  KAFKA_READY_TIMEOUT       default: 90  (seconds to wait for Kafka)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

# ---------------------------------------------------------------------------
# Resolve repo root
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

from launcher.websocket_server import WebSocketServer  # noqa: E402

logger = logging.getLogger(__name__)

TOPICS: List[str] = [
    "metrics.events",
    "service.state",
    "topo.decisions",
    "policy.decisions",
    "policy.approved",
]


# ---------------------------------------------------------------------------
# Docker Compose helpers
# ---------------------------------------------------------------------------

def run_docker_compose(compose_file: str = "docker-compose.yml") -> None:
    """Bring the full Docker Compose stack up (detached)."""
    logger.info("▶  Starting Docker Compose stack …")
    result = subprocess.run(
        ["docker", "compose", "-f", compose_file, "up", "-d", "--remove-orphans"],
        cwd=str(REPO_ROOT),
    )
    if result.returncode != 0:
        raise RuntimeError(f"docker compose up failed (exit {result.returncode})")
    logger.info("✔  Docker Compose stack is up.")


def stop_docker_compose(compose_file: str = "docker-compose.yml") -> None:
    """Stop containers (leaves volumes intact)."""
    logger.info("▶  Stopping Docker Compose stack …")
    subprocess.run(
        ["docker", "compose", "-f", compose_file, "down"],
        cwd=str(REPO_ROOT),
    )
    logger.info("✔  Docker Compose stack stopped.")


# ---------------------------------------------------------------------------
# Kafka readiness probe
# ---------------------------------------------------------------------------

def _tcp_ping(host: str, port: int, timeout: float = 2.0) -> bool:
    """Return True if TCP port is accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def wait_for_kafka(
    bootstrap: str = "localhost:9092",
    timeout: int = 90,
    poll_interval: float = 3.0,
) -> None:
    """Block until Kafka is ready to accept clients or timeout is reached.

    Two-phase check:
      Phase 1 – TCP port is open (Kafka process accepting connections)
      Phase 2 – AdminClient can list topics (broker fully initialised)
    """
    host, port_str = bootstrap.rsplit(":", 1)
    port = int(port_str)
    deadline = time.monotonic() + timeout

    # ---- Phase 1: TCP -------------------------------------------------------
    logger.info("⏳  Waiting for Kafka TCP port %s:%s (up to %ss) …", host, port, timeout)
    while time.monotonic() < deadline:
        if _tcp_ping(host, port):
            logger.info("✔  Kafka TCP port is open.")
            break
        logger.debug("    Kafka TCP not ready yet – retrying in %.0fs …", poll_interval)
        time.sleep(poll_interval)
    else:
        raise TimeoutError(
            f"Kafka at {bootstrap} did not open TCP port within {timeout}s. "
            "Is Docker running and the kafka container healthy?"
        )

    # ---- Phase 2: AdminClient -----------------------------------------------
    logger.info("⏳  Waiting for Kafka broker to finish initialising …")
    from kafka import KafkaAdminClient  # type: ignore
    from kafka.errors import NoBrokersAvailable  # type: ignore

    while time.monotonic() < deadline:
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=bootstrap,
                client_id="readiness_probe",
                request_timeout_ms=5_000,
            )
            admin.list_topics()
            admin.close()
            logger.info("✔  Kafka broker is ready.")
            return
        except (NoBrokersAvailable, Exception) as exc:
            logger.debug("    Broker not ready yet (%s) – retrying in %.0fs …", exc, poll_interval)
            time.sleep(poll_interval)

    raise TimeoutError(
        f"Kafka broker at {bootstrap} did not finish initialising within {timeout}s."
    )


# ---------------------------------------------------------------------------
# Kafka → WebSocket bridge
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class KafkaWebSocketBridge(threading.Thread):
    """Poll every Kafka topic listed in TOPICS and push each message to the
    WebSocket server so the React UI receives real-time updates.

    Uses raw kafka-python ``KafkaConsumer`` (not the abstract base class) so
    we get direct access to ``poll()`` with full ``TopicPartition`` metadata.
    The bridge automatically reconnects if the connection drops.
    """

    RECONNECT_DELAY = 5.0      # seconds between reconnect attempts
    POLL_TIMEOUT_MS = 300      # how long poll() blocks per iteration

    def __init__(
        self,
        ws_server: WebSocketServer,
        group_id: str = "ui_ws_bridge",
        bootstrap_servers: str = "localhost:9092",
    ) -> None:
        super().__init__(name="KafkaWebSocketBridgeThread", daemon=True)
        self.ws_server = ws_server
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self._stop = threading.Event()
        self._consumer: Optional[object] = None   # raw KafkaConsumer

    # ------------------------------------------------------------------
    # Thread lifecycle
    # ------------------------------------------------------------------

    def run(self) -> None:
        logger.info("KafkaWebSocketBridge starting, topics=%s", TOPICS)
        while not self._stop.is_set():
            try:
                self._open_consumer()
                self._poll_loop()
            except Exception as exc:
                logger.warning(
                    "Bridge disconnected (%s). Reconnecting in %.0fs …",
                    exc, self.RECONNECT_DELAY,
                )
                self._close_consumer()
                if not self._stop.is_set():
                    time.sleep(self.RECONNECT_DELAY)
        self._close_consumer()
        logger.info("KafkaWebSocketBridge stopped.")

    def stop(self) -> None:
        self._stop.set()
        self._close_consumer()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open_consumer(self) -> None:
        """Create and configure a raw KafkaConsumer.

        Retries silently until the broker is reachable.  Any exception from
        the constructor (including ``NoBrokersAvailable`` and
        ``ConnectionResetError`` during the API-version handshake) is caught
        and retried after ``RECONNECT_DELAY`` seconds.
        """
        from kafka import KafkaConsumer  # type: ignore
        from kafka.errors import NoBrokersAvailable  # type: ignore

        while not self._stop.is_set():
            try:
                consumer = KafkaConsumer(
                    *TOPICS,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda b: (
                        json.loads(b.decode("utf-8")) if b else {}
                    ),
                    key_deserializer=lambda b: b.decode("utf-8") if b else None,
                    # Aggressive timeouts so we detect broken connections fast
                    request_timeout_ms=15_000,
                    session_timeout_ms=10_000,
                    heartbeat_interval_ms=3_000,
                    connections_max_idle_ms=30_000,
                    # Trigger the API-version check *now* so we catch errors here
                    # rather than inside poll(), by fetching metadata immediately.
                    metadata_max_age_ms=5_000,
                )
                # Force an immediate metadata fetch – this is where the API
                # version handshake happens.  If Kafka isn't ready this throws.
                consumer.topics()  # raises NoBrokersAvailable or similar
                self._consumer = consumer
                logger.info("✔  Bridge connected to Kafka (%s), subscribed to %s",
                            self.bootstrap_servers, TOPICS)
                return
            except (NoBrokersAvailable, Exception) as exc:
                logger.warning(
                    "Bridge: Kafka not ready yet (%s: %s) – retrying in %.0fs …",
                    type(exc).__name__, exc, self.RECONNECT_DELAY,
                )
                try:
                    consumer.close()
                except Exception:
                    pass
                if not self._stop.is_set():
                    time.sleep(self.RECONNECT_DELAY)

    def _poll_loop(self) -> None:
        """Block-poll and forward messages until stopped or error occurs."""
        while not self._stop.is_set():
            # poll() returns Dict[TopicPartition, List[ConsumerRecord]]
            raw = self._consumer.poll(timeout_ms=self.POLL_TIMEOUT_MS)
            if raw:
                total_records = sum(len(records) for records in raw.values())
                logger.info(
                    "Bridge poll received %s records across %s partitions",
                    total_records,
                    len(raw),
                )
            for tp, records in raw.items():
                for record in records:
                    self._forward(tp.topic, record)

    def _forward(self, topic: str, record) -> None:
        """Push one Kafka record to all WebSocket clients."""
        try:
            logger.info(
                "Bridge consumed event topic=%s partition=%s offset=%s key=%s",
                topic,
                record.partition,
                record.offset,
                record.key,
            )
            payload: dict = (
                record.value if isinstance(record.value, dict) else {}
            )
            logger.debug(
                "Bridge payload transformed topic=%s offset=%s payload_keys=%s",
                topic,
                record.offset,
                sorted(list(payload.keys())),
            )
            data = {
                **payload,
                "topic": topic,
                "partition": record.partition,
                "offset": record.offset,
                "ts": _now_iso(),
            }

            # Human-readable log entry for the UI log panel
            log_msg = (
                f"[{topic}] p={record.partition} "
                f"off={record.offset} key={record.key or '—'}"
            )
            self.ws_server.add_log("INFO", topic, log_msg)

            if topic == "metrics.events":
                self.ws_server.add_metrics(data)

            self.ws_server.add_event(topic, data)
            logger.info(
                "Bridge forwarded event topic=%s offset=%s to websocket layer",
                topic,
                record.offset,
            )

        except Exception as exc:
            logger.exception("Bridge: failed to forward record from %s: %s", topic, exc)

    def _close_consumer(self) -> None:
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:
                pass
            finally:
                self._consumer = None


# ---------------------------------------------------------------------------
# Main async entry point
# ---------------------------------------------------------------------------

async def async_main(
    stop_docker_on_exit: bool = False,
    skip_docker: bool = False,
) -> None:
    """Orchestrate the unified pipeline."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    ws_host   = os.environ.get("WS_HOST", "0.0.0.0")
    ws_port   = int(os.environ.get("WS_PORT", "8765"))
    bridge_group = os.environ.get("KAFKA_BRIDGE_GROUP", "ui_ws_bridge")
    kafka_timeout = int(os.environ.get("KAFKA_READY_TIMEOUT", "90"))

    # -- 1. Docker Compose (optional) --------------------------------------
    if not skip_docker:
        run_docker_compose()

    # -- 2. Wait for Kafka to be genuinely ready ---------------------------
    # Run in executor so we don't block the event loop
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,
        lambda: wait_for_kafka(bootstrap, timeout=kafka_timeout),
    )

    # -- 3. WebSocket server -----------------------------------------------
    ws_server = WebSocketServer(host=ws_host, port=ws_port)
    await ws_server.start()
    logger.info("✔  WebSocket server running on ws://%s:%s", ws_host, ws_port)

    # -- 4. Kafka → WebSocket bridge ---------------------------------------
    bridge = KafkaWebSocketBridge(
        ws_server=ws_server,
        group_id=bridge_group,
        bootstrap_servers=bootstrap,
    )
    bridge.start()

    # -- 5. Termination handling -------------------------------------------
    stop_event = asyncio.Event()

    def _on_signal(signame: str) -> None:
        logger.info("Signal %s received – shutting down …", signame)
        loop.call_soon_threadsafe(stop_event.set)

    for sig, name in [(signal.SIGINT, "SIGINT"), (signal.SIGTERM, "SIGTERM")]:
        try:
            loop.add_signal_handler(sig, _on_signal, name)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _on_signal(name))

    # -- 6. Status summary -------------------------------------------------
    logger.info("")
    logger.info("=" * 68)
    logger.info("  ADAPTIVE NETWORK POLICY ENGINE  –  pipeline running")
    logger.info("=" * 68)
    logger.info("  WebSocket  →  ws://%s:%s", ws_host, ws_port)
    logger.info("  Kafka      →  %s", bootstrap)
    logger.info("  Topics     →  %s", ", ".join(TOPICS))
    logger.info("  React UI   →  http://localhost:5173  (cd ui && npm run dev)")
    logger.info("  Prometheus →  http://localhost:9090")
    logger.info("  HITL API   →  http://localhost:8080")
    logger.info("=" * 68)
    logger.info("  Press Ctrl-C to stop the Python layer.")
    if stop_docker_on_exit:
        logger.info("  Docker stack will be stopped on exit (--down flag).")
    logger.info("=" * 68)
    logger.info("")

    await stop_event.wait()

    # -- 7. Graceful shutdown ----------------------------------------------
    logger.info("Shutting down bridge …")
    bridge.stop()
    bridge.join(timeout=5)

    logger.info("Shutting down WebSocket server …")
    await ws_server.stop()

    if stop_docker_on_exit:
        stop_docker_compose()
    else:
        logger.info(
            "Docker stack is still running. "
            "Use 'docker compose down' to stop it."
        )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)-30s  %(message)s",
    )
    # Suppress noisy kafka internal logs (still show WARNING+)
    logging.getLogger("kafka.conn").setLevel(logging.WARNING)
    logging.getLogger("kafka.client").setLevel(logging.WARNING)
    logging.getLogger("kafka.coordinator").setLevel(logging.WARNING)
    logging.getLogger("kafka.coordinator.heartbeat").setLevel(logging.WARNING)
    logging.getLogger("kafka.consumer").setLevel(logging.WARNING)
    logging.getLogger("kafka.consumer.fetcher").setLevel(logging.WARNING)
    logging.getLogger("websockets.server").setLevel(logging.WARNING)

    stop_docker  = "--down"      in sys.argv
    skip_docker  = "--no-docker" in sys.argv

    try:
        asyncio.run(async_main(
            stop_docker_on_exit=stop_docker,
            skip_docker=skip_docker,
        ))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logger.exception("Unified pipeline terminated with an error: %s", exc)
        if stop_docker:
            try:
                stop_docker_compose()
            except Exception:
                pass
        sys.exit(1)


if __name__ == "__main__":
    main()
