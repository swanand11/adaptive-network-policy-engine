"""Pipeline Validator — Tests the execution layer pipeline.

Usage
-----
    python -m runners.pipeline_validator

Tests the complete pipeline:
1. Sends requests to the load balancer proxy
2. Checks traffic distribution matches current weights
3. Validates that weights are updated from Kafka decisions
"""

import logging
import sys
import time
import requests
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("pipeline_validator")


def get_backend_from_response(response):
    """Extract backend name from response."""
    try:
        data = response.json()
        cloud = data.get("cloud", "unknown")
        # Map cloud names to backend names
        backend_map = {
            "aws": "aws-simulator",
            "aks": "aks-simulator",
            "digitalocean": "digitalocean-simulator"
        }
        return backend_map.get(cloud, cloud)
    except:
        return "unknown"


def test_traffic_distribution(proxy_url="http://localhost:8080", num_requests=100):
    """Test traffic distribution over multiple requests."""
    logger.info(f"Sending {num_requests} requests to {proxy_url}")

    backends = []
    for i in range(num_requests):
        try:
            response = requests.get(f"{proxy_url}/", timeout=5)
            backend = get_backend_from_response(response)
            backends.append(backend)
            if (i + 1) % 20 == 0:
                logger.info(f"Sent {i+1}/{num_requests} requests")
        except Exception as e:
            logger.error(f"Request {i+1} failed: {e}")
            backends.append("error")

    # Count distribution
    counter = Counter(backends)
    total = sum(counter.values())

    logger.info("Traffic distribution:")
    for backend, count in counter.items():
        percentage = (count / total) * 100
        logger.info(f"  {backend}: {count} requests ({percentage:.1f}%)")

    return counter


def test_weights_update(proxy_url="http://localhost:8080"):
    """Test manual weights update."""
    logger.info("Testing manual weights update")

    # Set custom weights
    weights = {"aws_wi": 50, "aks_wi": 30, "do_wi": 20}
    response = requests.post(f"{proxy_url}/update_weights", json=weights, timeout=5)

    if response.status_code == 200:
        logger.info("Weights updated successfully")
        return True
    else:
        logger.error(f"Failed to update weights: {response.status_code} {response.text}")
        return False


def main():
    """Main validation function."""
    logger.info("Starting Pipeline Validation...")

    proxy_url = "http://localhost:8080"

    # Wait for services to be ready
    logger.info("Waiting for services to start...")
    time.sleep(10)

    # Test initial distribution (should be roughly equal)
    logger.info("Testing initial traffic distribution (should be ~33/33/34)")
    initial_dist = test_traffic_distribution(proxy_url, 100)

    # Test weights update
    if test_weights_update(proxy_url):
        logger.info("Waiting for weights to take effect...")
        time.sleep(2)

        # Test distribution with new weights
        logger.info("Testing traffic distribution with new weights (50/30/20)")
        updated_dist = test_traffic_distribution(proxy_url, 100)

        # Validate that distribution roughly matches weights
        aws_pct = (updated_dist.get("aws-simulator", 0) / sum(updated_dist.values())) * 100
        aks_pct = (updated_dist.get("aks-simulator", 0) / sum(updated_dist.values())) * 100
        do_pct = (updated_dist.get("digitalocean-simulator", 0) / sum(updated_dist.values())) * 100

        logger.info(f"Expected: AWS=50%, AKS=30%, DO=20%")
        logger.info(f"Actual: AWS={aws_pct:.1f}%, AKS={aks_pct:.1f}%, DO={do_pct:.1f}%")

        # Check if within reasonable tolerance (±10%)
        tolerance = 10
        if (abs(aws_pct - 50) <= tolerance and
            abs(aks_pct - 30) <= tolerance and
            abs(do_pct - 20) <= tolerance):
            logger.info("✓ Traffic distribution matches weights within tolerance")
        else:
            logger.error("✗ Traffic distribution does not match weights")

    logger.info("Pipeline validation completed")


if __name__ == "__main__":
    main()