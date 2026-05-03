"""run_mocks.py — Start all three cloud simulators and block until Ctrl-C.

Usage
-----
    python run_mocks.py

Starts three Flask-based Prometheus mock services:
    AWS simulator        → http://localhost:8001/metrics
    AKS / Azure sim      → http://localhost:8002/metrics
    DigitalOcean sim     → http://localhost:8003/metrics

Each simulator runs in its own daemon thread; the main thread blocks until
Ctrl-C arrives, then exits cleanly.

No Kafka dependency — this file is intentionally self-contained so it can be
started (and restarted) independently of the pipeline runner.
"""
from __future__ import annotations

import logging
import sys
import threading
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from mocks.aws_simulator import AWSSimulator
from mocks.aks_simulator import AKSSimulator
from mocks.dodroplets_simulator import DigitalOceanSimulator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("run_mocks")

# (SimulatorClass, port, display-name, health-url)
SIMULATORS = [
    (AWSSimulator,          8001, "AWS",          "http://127.0.0.1:8001/metrics"),
    (AKSSimulator,          8002, "AKS/Azure",    "http://127.0.0.1:8002/metrics"),
    (DigitalOceanSimulator, 8003, "DigitalOcean", "http://127.0.0.1:8003/metrics"),
]

STARTUP_TIMEOUT_S = 15.0


def _start(cls, port: int, name: str) -> threading.Thread:
    sim = cls(service_port=port)
    t = threading.Thread(
        target=sim.run, kwargs={"debug": False},
        name=f"{name}Thread", daemon=True,
    )
    t.start()
    logger.info("Started %-14s thread on port %d", name, port)
    return t


def _wait_ready(url: str, name: str, timeout: float = STARTUP_TIMEOUT_S) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if requests.get(url, timeout=1.0).status_code == 200:
                logger.info("%-14s ready  %s", name, url)
                return True
        except requests.exceptions.RequestException:
            pass
        time.sleep(0.4)
    logger.error("%-14s did NOT become ready at %s within %.0f s", name, url, timeout)
    return False


def main() -> None:
    logger.info("=" * 60)
    logger.info("Starting cloud mock simulators")
    logger.info("=" * 60)

    threads = [_start(cls, port, name) for cls, port, name, _ in SIMULATORS]

    all_ready = all(
        _wait_ready(url, name)
        for _, _, name, url in SIMULATORS
    )
    if not all_ready:
        logger.error("One or more simulators failed to start — exiting.")
        sys.exit(1)

    logger.info("=" * 60)
    logger.info("All simulators running.  Press Ctrl-C to stop.")
    for _, port, name, url in SIMULATORS:
        logger.info("  %-14s  %s", name, url)
    logger.info("=" * 60)

    try:
        while True:
            dead = [t for t in threads if not t.is_alive()]
            if dead:
                logger.warning("Simulator threads died: %s", [t.name for t in dead])
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info("Ctrl-C received — stopping mock services.")


if __name__ == "__main__":
    main()