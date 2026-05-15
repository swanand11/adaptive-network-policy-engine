"""Traffic Generator — Sends HTTP requests to the load balancer proxy.

Usage
-----
    python -m runners.traffic_generator

This service simulates a single traffic source by continuously hitting the
load balancer proxy, which then routes requests to the mock CSP backends.
"""

import logging
import os
import sys
import time
from pathlib import Path

import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("traffic_generator")


def main():
    proxy_url = os.getenv("PROXY_URL", "http://load-balancer-proxy:8080")
    target_path = os.getenv("TARGET_PATH", "/")
    request_rate = float(os.getenv("REQUESTS_PER_SECOND", "2"))
    request_delay = 1.0 / max(request_rate, 1.0)

    logger.info("Starting traffic generator")
    logger.info("Proxy URL=%s target_path=%s rate=%.2f req/s", proxy_url, target_path, request_rate)

    while True:
        try:
            response = requests.get(f"{proxy_url}{target_path}", timeout=5)
            if response.ok:
                logger.debug("Request succeeded: %s", response.status_code)
            else:
                logger.warning("Request failed: %s %s", response.status_code, response.text)
        except Exception as exc:
            logger.error("Request exception: %s", exc)

        time.sleep(request_delay)


if __name__ == "__main__":
    main()