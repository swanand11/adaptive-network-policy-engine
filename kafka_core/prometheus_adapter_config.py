"""Prometheus Adapter Configuration

This file contains environment-specific configuration for the adapter.
Override these values using environment variables or by editing directly.

ENVIRONMENT VARIABLES (optional):
  PROMETHEUS_POLL_INTERVAL: Polling interval in seconds (default: 10)
  PROMETHEUS_TIMEOUT: Request timeout in seconds (default: 5)
  PROMETHEUS_AWS_URL: AWS metrics endpoint (default: http://localhost:8001/metrics)
  PROMETHEUS_AKS_URL: AKS metrics endpoint (default: http://localhost:8002/metrics)
  PROMETHEUS_DO_URL: DigitalOcean metrics endpoint (default: http://localhost:8003/metrics)
  KAFKA_BOOTSTRAP_SERVERS: Kafka bootstrap servers (from kafka.config)
  LOG_LEVEL: Logging level (default: INFO)
"""

import os
from typing import Dict, Any


ENDPOINTS_CONFIG = {
    "aws": {
        "url": os.getenv("PROMETHEUS_AWS_URL", "http://aws-simulator:8001/metrics"),
        "service_id": "service-cache-aws",
        "cloud": "AWS"  
    },
    "aks": {
        "url": os.getenv("PROMETHEUS_AKS_URL", "http://aks-simulator:8002/metrics"),
        "service_id": "service-cache-aks",
        "cloud": "AKS"
    },
    "droplet": {
        "url": os.getenv("PROMETHEUS_DO_URL", "http://digitalocean-simulator:8003/metrics"),
        "service_id": "service-cache-droplet",
        "cloud": "DIGITALOCEAN"
    }
}



POLL_INTERVAL = int(os.getenv("PROMETHEUS_POLL_INTERVAL", "5"))  # seconds
REQUEST_TIMEOUT = int(os.getenv("PROMETHEUS_TIMEOUT", "3"))  # seconds (shorter for 5s cycle)
MAX_RETRIES = int(os.getenv("PROMETHEUS_MAX_RETRIES", "2"))  # fewer retries for fast cycle
BACKOFF_FACTOR = float(os.getenv("PROMETHEUS_BACKOFF_FACTOR", "0.5"))


# These are passed to KafkaProducerTemplate
# See kafka.config for broker settings
KAFKA_TOPIC = "metrics.events"



LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'



# Metrics to extract (others are ignored)
METRICS_TO_EXTRACT = [
    'request_latency_seconds_sum',
    'request_latency_seconds_count',
    'request_count_total',
    'error_count_total',
    'cpu_usage_percent',
    'memory_usage_percent',
    'error_rate_percent'
]

# Metric field mappings (prometheus_name -> normalized_name)
METRIC_FIELD_MAPPING = {
    'request_latency_seconds': 'request_latency_ms',
    'request_count_total': 'request_count',
    'error_count_total': 'error_count',
    'cpu_usage_percent': 'cpu_usage_percent',
    'memory_usage_percent': 'memory_usage_percent',
    'error_rate_percent': 'error_rate_percent'
}


def validate_config() -> bool:
    """Validate configuration."""
    issues = []
    
    if POLL_INTERVAL <= 0:
        issues.append(f"POLL_INTERVAL must be positive (got {POLL_INTERVAL})")
    
    if REQUEST_TIMEOUT <= 0:
        issues.append(f"REQUEST_TIMEOUT must be positive (got {REQUEST_TIMEOUT})")
    
    if MAX_RETRIES < 0:
        issues.append(f"MAX_RETRIES must be non-negative (got {MAX_RETRIES})")
    
    for endpoint_name, config in ENDPOINTS_CONFIG.items():
        if not config.get('url'):
            issues.append(f"Endpoint {endpoint_name} missing URL")
        if not config.get('service_id'):
            issues.append(f"Endpoint {endpoint_name} missing service_id")
        if not config.get('cloud'):
            issues.append(f"Endpoint {endpoint_name} missing cloud provider")
    
    if issues:
        for issue in issues:
            print(f"CONFIG ERROR: {issue}")
        return False
    
    return True