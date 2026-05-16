"""Base Cloud Simulator - Flask server with Prometheus metrics."""

import random
import time
import logging
import threading
from typing import Dict, Any
from datetime import datetime
from abc import ABC, abstractmethod
from flask import Flask, Response
from prometheus_client import Counter, Gauge, Summary, generate_latest, CollectorRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseSimulator(ABC):
    """Base class for cloud simulators with Prometheus metrics."""

    def __init__(self, service_name: str, cloud_name: str, service_port: int, metrics_port: int):
        self.service_name = service_name
        self.cloud_name = cloud_name
        self.service_port = service_port
        self.metrics_port = metrics_port
        
        self.app = Flask(f"{cloud_name}-simulator")
        self.registry = CollectorRegistry()
        
        self.request_latency = Summary(
            'request_latency_seconds',
            'Request latency in seconds',
            ['cloud', 'service_id'],
            registry=self.registry
        )
        self.request_count = Counter(
            'request_count_total',
            'Total requests',
            ['cloud', 'service_id'],
            registry=self.registry
        )
        self.error_count = Counter(
            'error_count_total',
            'Total errors',
            ['cloud', 'service_id'],
            registry=self.registry
        )
        self.cpu_usage = Gauge(
            'cpu_usage_percent',
            'CPU usage percentage',
            ['cloud', 'service_id'],
            registry=self.registry
        )
        self.memory_usage = Gauge(
            'memory_usage_percent',
            'Memory usage percentage',
            ['cloud', 'service_id'],
            registry=self.registry
        )
        self.error_rate = Gauge(
            'error_rate_percent',
            'Error rate percentage',
            ['cloud', 'service_id'],
            registry=self.registry
        )

        self.active_requests = 0
        self._lock = threading.Lock()
        
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route("/")
        def home():
            """Realistic endpoint that scales latency based on concurrency."""
            with self._lock:
                self.active_requests += 1
            
            try:
                cloud_labels = {"cloud": self.cloud_name, "service_id": self.service_name}
                metrics_data = self.generate_metrics(self.active_requests)
                
                latency_s = metrics_data.get("latency_ms", 100) / 1000.0
                cpu = metrics_data.get("cpu", 40)
                memory = metrics_data.get("memory_percent", 50)
                error_rate = metrics_data.get("error_rate", 0.5)

                # Simulate processing time
                time.sleep(latency_s)

                # Record metrics synchronously
                self.request_count.labels(**cloud_labels).inc()
                self.request_latency.labels(**cloud_labels).observe(latency_s)
                self.cpu_usage.labels(**cloud_labels).set(cpu)
                self.memory_usage.labels(**cloud_labels).set(memory)
                self.error_rate.labels(**cloud_labels).set(error_rate)

                # Simulate errors realistically based on load
                is_error = random.random() < (error_rate / 100.0)
                
                if is_error:
                    self.error_count.labels(**cloud_labels).inc()
                    return Response('{"error": "Internal Server Error", "status": "failed"}', status=500, mimetype='application/json')
                
                return {
                    "status": "ok",
                    "service": self.service_name,
                    "cloud": self.cloud_name,
                    "active_requests": self.active_requests,
                    "latency_ms": latency_s * 1000,
                    "cpu_percent": cpu
                }
            finally:
                with self._lock:
                    self.active_requests -= 1

        @self.app.route("/metrics")
        def metrics():
            return generate_latest(self.registry), 200, {
                "Content-Type": "text/plain; version=0.0.4"
            }

    @abstractmethod
    def generate_metrics(self, active_requests: int) -> Dict[str, Any]:
        """Generate cloud-specific metrics based on current active requests. Override in subclass."""
        pass

    def run(self, debug: bool = False):
        logger.info(f"Starting {self.cloud_name.upper()} simulator for {self.service_name}")
        logger.info(f"Service running on http://0.0.0.0:{self.service_port}")
        
        # We must run threaded=True to handle concurrent requests
        self.app.run(host="0.0.0.0", port=self.service_port, debug=debug, use_reloader=False, threaded=True)