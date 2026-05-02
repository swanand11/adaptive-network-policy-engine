"""Prometheus Metrics Adapter

PURPOSE:
  Poll Prometheus endpoints (AWS, AKS, DigitalOcean), normalize metrics,
  and send them to Kafka via MetricsEvent schema.

FLOW:
  1. Poll 3 endpoints: aws-simulator:8001/metrics (AWS), aks-simulator:8002/metrics (AKS), digitalocen-simulator:8003/metrics (DO)
  2. Parse Prometheus text format (OpenMetrics)
  3. Normalize metrics (extract labels, calculate derived metrics like error_rate)
  4. Create MetricsEvent objects matching schema
  5. Send to Kafka using KafkaProducerTemplate
  6. Repeat at configurable interval

USAGE:
  python prometheus_metrics_adapter.py

CONFIG:
  Update ENDPOINTS, POLL_INTERVAL, and service mapping in __main__

METRICS NORMALIZED:
  - request_latency_seconds: Summary (count, sum) → avg latency
  - request_count_total: Counter
  - cpu_usage_percent: Gauge
  - memory_usage_percent: Gauge
  - error_rate_percent: Gauge

DEPENDENCIES:
  - requests (HTTP polling)
  - pydantic (schema validation)
  - kafka.schemas (MetricsEvent, MetricsEventValue)
  - kafka.producer_base (KafkaProducerTemplate)
  - prometheus_client (optional: for metric parsing - we use regex for simplicity)
"""

import time
import re
import logging
from typing import Dict, Any, Optional, Tuple
from datetime import datetime
from enum import Enum

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from .prometheus_adapter_config import (
    ENDPOINTS_CONFIG,
    POLL_INTERVAL,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    BACKOFF_FACTOR,
    KAFKA_TOPIC,
    validate_config,
)

# Import Kafka schemas and producer
try:
    from .schemas import MetricsEvent, MetricsEventValue
    from .producer_base import KafkaProducerTemplate
    from .enums import CloudProvider
except ImportError as e:
    print(f"Warning: Could not import Kafka modules: {e}")
    print("Make sure kafka package is in PYTHONPATH")


ENDPOINTS = {
    endpoint_name: {
        "url": endpoint_config["url"],
        "cloud": CloudProvider(endpoint_config["cloud"].lower()),
        "service_id": endpoint_config["service_id"],
        "partition": endpoint_config["partition"],
    }
    for endpoint_name, endpoint_config in ENDPOINTS_CONFIG.items()
}



logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PrometheusParser:
    """Parse Prometheus text format (OpenMetrics)."""
    
    # Regex patterns for parsing metrics
    METRIC_PATTERN = re.compile(
        r'^(?P<name>[\w:]+)\{?(?P<labels>[^}]*)\}?\s+(?P<value>[-+]?[.\d]+)(?:\s+(?P<timestamp>\d+))?$'
    )
    LABEL_PATTERN = re.compile(r'(\w+)="([^"]*)"')
    
    @staticmethod
    def parse_line(line: str) -> Optional[Dict[str, Any]]:
        """Parse a single Prometheus metric line.
        
        Args:
            line: Prometheus format metric line
            
        Returns:
            Dict with 'name', 'labels', 'value' or None if unparseable
        """
        line = line.strip()
        
        # Skip comments and empty lines
        if not line or line.startswith('#'):
            return None
        
        match = PrometheusParser.METRIC_PATTERN.match(line)
        if not match:
            logger.debug(f"Could not parse line: {line}")
            return None
        
        name = match.group('name')
        labels_str = match.group('labels') or ''
        value = float(match.group('value'))
        
        # Parse labels
        labels = {}
        if labels_str:
            for label_match in PrometheusParser.LABEL_PATTERN.finditer(labels_str):
                labels[label_match.group(1)] = label_match.group(2)
        
        return {
            'name': name,
            'labels': labels,
            'value': value
        }
    
    @staticmethod
    def parse_dump(dump_text: str) -> Dict[str, list]:
        """Parse entire Prometheus dump.
        
        Args:
            dump_text: Full Prometheus metrics dump
            
        Returns:
            Dict mapping metric names to list of (labels, value) tuples
        """
        metrics = {}
        
        for line in dump_text.split('\n'):
            parsed = PrometheusParser.parse_line(line)
            if parsed:
                name = parsed['name']
                if name not in metrics:
                    metrics[name] = []
                metrics[name].append({
                    'labels': parsed['labels'],
                    'value': parsed['value']
                })
        
        return metrics


class MetricsNormalizer:
    """Normalize Prometheus metrics to common schema."""
    
    @staticmethod
    def normalize(parsed_metrics: Dict[str, list]) -> Dict[str, Any]:
        """Normalize parsed Prometheus metrics.
        
        Args:
            parsed_metrics: Output from PrometheusParser.parse_dump()
            
        Returns:
            Normalized metrics dict
        """
        normalized = {
            'request_latency_ms': None,
            'request_count': None,
            'error_rate_percent': None,
            'cpu_usage_percent': None,
            'memory_usage_percent': None,
            'timestamp': datetime.now().isoformat()
        }
        
        # Extract request latency (summary metric: sum/count = avg)
        if 'request_latency_seconds_sum' in parsed_metrics and 'request_latency_seconds_count' in parsed_metrics:
            try:
                latency_sum = parsed_metrics['request_latency_seconds_sum'][0]['value']
                latency_count = parsed_metrics['request_latency_seconds_count'][0]['value']
                if latency_count > 0:
                    avg_latency_sec = latency_sum / latency_count
                    normalized['request_latency_ms'] = avg_latency_sec * 1000
            except (IndexError, KeyError, ZeroDivisionError) as e:
                logger.warning(f"Could not extract latency: {e}")
        
        # Extract request count
        if 'request_count_total' in parsed_metrics:
            try:
                normalized['request_count'] = int(parsed_metrics['request_count_total'][0]['value'])
            except (IndexError, KeyError) as e:
                logger.warning(f"Could not extract request_count: {e}")
        
        # Extract error rate from gauge
        if 'error_rate_percent' in parsed_metrics:
            try:
                normalized['error_rate_percent'] = parsed_metrics['error_rate_percent'][0]['value']
            except (IndexError, KeyError) as e:
                logger.warning(f"Could not extract error_rate_percent: {e}")
        
        # Extract CPU usage
        if 'cpu_usage_percent' in parsed_metrics:
            try:
                normalized['cpu_usage_percent'] = parsed_metrics['cpu_usage_percent'][0]['value']
            except (IndexError, KeyError) as e:
                logger.warning(f"Could not extract cpu_usage_percent: {e}")
        
        # Extract memory usage
        if 'memory_usage_percent' in parsed_metrics:
            try:
                normalized['memory_usage_percent'] = parsed_metrics['memory_usage_percent'][0]['value']
            except (IndexError, KeyError) as e:
                logger.warning(f"Could not extract memory_usage_percent: {e}")
        
        return normalized


class PrometheusPoller:
    """Poll Prometheus endpoints with retry logic."""
    
    def __init__(self, timeout: int = REQUEST_TIMEOUT, max_retries: int = MAX_RETRIES):
        """Initialize HTTP session with retries.
        
        Args:
            timeout: Request timeout in seconds
            max_retries: Max retry attempts
        """
        self.timeout = timeout
        self.session = self._create_session(max_retries)
    
    @staticmethod
    def _create_session(max_retries: int) -> requests.Session:
        """Create requests session with retry strategy."""
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=BACKOFF_FACTOR,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def poll(self, url: str) -> Optional[str]:
        """Poll a Prometheus endpoint.
        
        Args:
            url: Prometheus metrics endpoint URL
            
        Returns:
            Metrics dump text or None on failure
        """
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return response.text
        except requests.exceptions.Timeout:
            logger.error(f"Timeout polling {url}")
            return None
        except requests.exceptions.ConnectionError:
            logger.error(f"Connection error polling {url}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error polling {url}: {e}")
            return None

class PrometheusMetricsAdapter:
    """Main adapter orchestrating polling, normalization, and Kafka sending."""
    
    def __init__(self, producer: KafkaProducerTemplate, endpoints: Dict[str, Dict], poll_interval: int = 10):
        """Initialize adapter.
        
        Args:
            producer: KafkaProducerTemplate instance
            endpoints: Endpoint configuration dict
            poll_interval: Polling interval in seconds
        """
        self.producer = producer
        self.endpoints = endpoints
        self.poll_interval = poll_interval
        self.poller = PrometheusPoller()
        self.parser = PrometheusParser()
        self.normalizer = MetricsNormalizer()
        self.running = False
        logger.info(
            "metrics.events key routing enabled: using service_id as key with explicit partition assignment (service-cache-aws→0, service-db→1, service-cache→2)"
        )
    
    def poll_and_send(self) -> bool:
        """Poll all endpoints, normalize, and send to Kafka.
        
        Returns:
            True if all sends successful, False otherwise
        """
        all_success = True
        
        for endpoint_name, config in self.endpoints.items():
            try:
                # Poll endpoint
                logger.info(f"Polling {endpoint_name} endpoint: {config['url']}")
                metrics_dump = self.poller.poll(config['url'])
                
                if not metrics_dump:
                    logger.warning(f"Failed to poll {endpoint_name}, skipping")
                    all_success = False
                    continue
                
                # Parse metrics
                parsed_metrics = self.parser.parse_dump(metrics_dump)
                logger.debug(f"Parsed {len(parsed_metrics)} metric types from {endpoint_name}")
                
                # Normalize metrics
                normalized = self.normalizer.normalize(parsed_metrics)
                logger.debug(f"Normalized metrics for {endpoint_name}: {normalized}")
                
                # Use service_id@cloud as key for partition routing and agent identification
                partition_key = f"{config['service_id']}@{config['cloud'].value}"
                target_partition = config['partition']
                event = MetricsEvent(
                    key=partition_key,
                    value=MetricsEventValue(
                        service=config['service_id'],
                        cloud=config['cloud'],
                        timestamp=datetime.now(),
                        metrics=normalized,
                        correlation_id=f"prometheus-{endpoint_name}-{datetime.now().timestamp()}"
                    )
                )
                
                # Send to Kafka with explicit partition assignment
                try:
                    record_meta = self.producer.send(KAFKA_TOPIC, event, partition=target_partition)
                    logger.info(
                        "Sent metrics for %s to %s with key='%s' partition=%s: %s",
                        endpoint_name,
                        KAFKA_TOPIC,
                        partition_key,
                        target_partition,
                        record_meta,
                    )
                except Exception as e:
                    logger.error(f"Failed to send {endpoint_name} metrics to Kafka: {e}")
                    all_success = False
            
            except Exception as e:
                logger.error(f"Unexpected error processing {endpoint_name}: {e}")
                all_success = False
        
        return all_success
    
    def start(self) -> None:
        """Start continuous polling loop."""
        logger.info(f"Starting Prometheus metrics adapter (interval: {self.poll_interval}s)")
        self.running = True
        iteration = 0
        
        try:
            while self.running:
                iteration += 1
                logger.info(f"--- Poll iteration {iteration} ---")
                
                start_time = time.time()
                self.poll_and_send()
                elapsed = time.time() - start_time
                
                logger.info(f"Poll cycle completed in {elapsed:.2f}s, waiting {self.poll_interval}s")
                time.sleep(max(0, self.poll_interval - elapsed))
        
        except KeyboardInterrupt:
            logger.info("Received interrupt, shutting down")
        except Exception as e:
            logger.error(f"Unexpected error in polling loop: {e}", exc_info=True)
        finally:
            self.stop()
    
    def stop(self) -> None:
        """Stop polling and cleanup."""
        logger.info("Stopping adapter")
        self.running = False
        self.producer.flush()
        self.producer.close()
        self.poller.session.close()

def main():
    """Main entry point."""
    logger.info("Initializing Prometheus Metrics Adapter")

    if not validate_config():
        raise RuntimeError("Invalid Prometheus adapter configuration")

    try:
        # Initialize producer
        logger.info("Initializing Kafka producer")
        producer = KafkaProducerTemplate()

        # Initialize and start adapter
        adapter = PrometheusMetricsAdapter(
            producer=producer,
            endpoints=ENDPOINTS,
            poll_interval=POLL_INTERVAL
        )

        adapter.start()

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()