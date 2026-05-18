"""Traffic Generator - Generate realistic HTTP traffic to NGINX load balancer.

Sends requests at configurable rates with different traffic patterns.
NGINX distributes traffic to CSP simulators based on dynamic weights.

USAGE:
  python traffic_generator/generator.py --profile moderate --duration 300
"""

import time
import logging
import argparse
import signal
import sys
from typing import Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
except ImportError:
    requests = None

from .user_profiles import PROFILES, UserProfile

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TrafficGenerator:
    """Generate HTTP traffic to load balancer."""

    def __init__(
        self,
        target_url: str = "http://localhost:9000",
        profile: UserProfile = None,
        max_workers: int = 10
    ):
        """Initialize traffic generator.
        
        Args:
            target_url: Load balancer URL
            profile: Traffic profile
            max_workers: Maximum concurrent workers
        """
        self.target_url = target_url
        self.profile = profile or PROFILES["moderate"]
        self.max_workers = max_workers
        self._running = False
        self._stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_latency_ms": 0.0,
        }
        
        if requests is None:
            raise ImportError("requests library not installed. Install with: pip install requests")
        
        # Create session with retry logic
        self.session = self._create_session()
        
        logger.info(f"TrafficGenerator initialized: target={target_url}, profile={profile.name}")

    def _create_session(self) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def send_request(self) -> dict:
        """Send a single HTTP request.
        
        Returns:
            Dict with request result
        """
        start_time = time.time()
        
        try:
            response = self.session.get(
                self.target_url,
                timeout=10
            )
            
            latency_ms = (time.time() - start_time) * 1000
            
            self._stats["total_requests"] += 1
            
            if response.status_code == 200:
                self._stats["successful_requests"] += 1
                self._stats["total_latency_ms"] += latency_ms
                return {
                    "success": True,
                    "status_code": response.status_code,
                    "latency_ms": latency_ms
                }
            else:
                self._stats["failed_requests"] += 1
                return {
                    "success": False,
                    "status_code": response.status_code,
                    "latency_ms": latency_ms
                }
        
        except requests.exceptions.Timeout:
            self._stats["failed_requests"] += 1
            return {
                "success": False,
                "error": "timeout",
                "latency_ms": (time.time() - start_time) * 1000
            }
        
        except requests.exceptions.ConnectionError:
            self._stats["failed_requests"] += 1
            return {
                "success": False,
                "error": "connection_error",
                "latency_ms": (time.time() - start_time) * 1000
            }
        
        except Exception as e:
            self._stats["failed_requests"] += 1
            logger.error(f"Request error: {e}")
            return {
                "success": False,
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }

    def run(self, duration_seconds: Optional[int] = None) -> None:
        """Run traffic generation.
        
        Args:
            duration_seconds: Duration to run (None for infinite)
        """
        logger.info("=" * 80)
        logger.info(f"Starting traffic generation: profile={self.profile.name}")
        logger.info(f"Target: {self.target_url}")
        logger.info(f"Duration: {duration_seconds or 'infinite'} seconds")
        logger.info("=" * 80)
        
        self._running = True
        start_time = time.time()
        last_stats_time = start_time
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                while self._running:
                    elapsed = time.time() - start_time
                    
                    # Check duration
                    if duration_seconds and elapsed >= duration_seconds:
                        logger.info("Duration reached, stopping")
                        break
                    
                    # Get current RPS from profile
                    target_rps = self.profile.get_rps(int(elapsed))
                    
                    # Calculate requests to send this second
                    requests_this_second = int(target_rps)
                    
                    # Submit requests
                    futures = []
                    for _ in range(requests_this_second):
                        future = executor.submit(self.send_request)
                        futures.append(future)
                    
                    # Wait for completion (with timeout)
                    for future in as_completed(futures, timeout=2):
                        try:
                            result = future.result()
                        except Exception as e:
                            logger.error(f"Future error: {e}")
                    
                    # Print stats every 10 seconds
                    if time.time() - last_stats_time >= 10:
                        self._print_stats(elapsed)
                        last_stats_time = time.time()
                    
                    # Sleep to maintain rate (1 second intervals)
                    sleep_time = 1.0 - (time.time() - start_time - int(elapsed))
                    if sleep_time > 0:
                        time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            logger.info("Received interrupt, stopping")
        
        finally:
            self._running = False
            self._print_final_stats()

    def _print_stats(self, elapsed: float) -> None:
        """Print current statistics."""
        total = self._stats["total_requests"]
        success = self._stats["successful_requests"]
        failed = self._stats["failed_requests"]
        
        if total > 0:
            success_rate = (success / total) * 100
            avg_latency = self._stats["total_latency_ms"] / success if success > 0 else 0
            rps = total / elapsed if elapsed > 0 else 0
        else:
            success_rate = 0
            avg_latency = 0
            rps = 0
        
        logger.info(
            f"[{int(elapsed)}s] Total: {total}, Success: {success}, Failed: {failed}, "
            f"Success Rate: {success_rate:.1f}%, Avg Latency: {avg_latency:.1f}ms, "
            f"RPS: {rps:.1f}"
        )

    def _print_final_stats(self) -> None:
        """Print final statistics."""
        logger.info("=" * 80)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 80)
        
        total = self._stats["total_requests"]
        success = self._stats["successful_requests"]
        failed = self._stats["failed_requests"]
        
        if total > 0:
            success_rate = (success / total) * 100
            avg_latency = self._stats["total_latency_ms"] / success if success > 0 else 0
        else:
            success_rate = 0
            avg_latency = 0
        
        logger.info(f"Total Requests:      {total}")
        logger.info(f"Successful:          {success}")
        logger.info(f"Failed:              {failed}")
        logger.info(f"Success Rate:        {success_rate:.2f}%")
        logger.info(f"Average Latency:     {avg_latency:.2f}ms")
        logger.info("=" * 80)

    def stop(self) -> None:
        """Stop traffic generation."""
        logger.info("Stopping traffic generator")
        self._running = False


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Traffic Generator")
    parser.add_argument(
        "--target",
        default="http://localhost:9000",
        help="Load balancer URL (default: http://localhost:9000)"
    )
    parser.add_argument(
        "--profile",
        choices=list(PROFILES.keys()),
        default="moderate",
        help="Traffic profile (default: moderate)"
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Duration in seconds (default: infinite)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Max concurrent workers (default: 10)"
    )
    
    args = parser.parse_args()
    
    # Get profile
    profile = PROFILES[args.profile]
    
    # Create generator
    generator = TrafficGenerator(
        target_url=args.target,
        profile=profile,
        max_workers=args.workers
    )
    
    # Handle signals
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}")
        generator.stop()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Run generator
    generator.run(duration_seconds=args.duration)


if __name__ == "__main__":
    main()
