"""Base Cloud Simulator - Flask server with Prometheus metrics."""

import random
import time
import logging
from typing import Dict, Any
from datetime import datetime
from abc import ABC, abstractmethod
from flask import Flask
from prometheus_client import Counter, Gauge, Summary, generate_latest, CollectorRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseSimulator(ABC):
    """Base class for cloud simulators with Prometheus metrics."""

    def __init__(self, service_name: str, cloud_name: str, service_port: int, metrics_port: int):
        """
        Initialize simulator.
        
        Args:
            service_name: Service identifier (e.g., 'service-api')
            cloud_name: Cloud provider (aws, aks, digitalocean)
            service_port: Flask app port
            metrics_port: Prometheus metrics port (usually same as service_port)
        """
        self.service_name = service_name
        self.cloud_name = cloud_name
        self.service_port = service_port
        self.metrics_port = metrics_port
        
        # Create Flask app
        self.app = Flask(f"{cloud_name}-simulator")
        
        # Create custom registry for this service
        self.registry = CollectorRegistry()
        
        # Prometheus metrics
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
        
        self._setup_routes()

    def _setup_routes(self):
        """Setup Flask routes."""
        @self.app.route("/")
        def home():
            """Health check endpoint."""
            return {
                "status": "ok",
                "service": self.service_name,
                "cloud": self.cloud_name
            }

        @self.app.route("/metrics")
        def metrics():
            """Prometheus metrics endpoint."""
            return generate_latest(self.registry), 200, {
    "Content-Type": "text/plain; version=0.0.4"
}

        @self.app.route("/simulate", methods=["POST"])
        def simulate():
            """Simulate a request (for testing)."""
            self.simulate_request()
            return {"status": "simulated"}

    def simulate_request(self):
        """Simulate a single request with metrics."""
        # Generate metrics
        cloud_labels = {"cloud": self.cloud_name, "service_id": self.service_name}
        metrics_data = self.generate_metrics()
        
        # Record request
        self.request_count.labels(**cloud_labels).inc()
        
        # Record latency
        latency = metrics_data.get("latency_ms", 100) / 1000
        self.request_latency.labels(**cloud_labels).observe(latency)
        
        # Record CPU
        cpu = metrics_data.get("cpu", 40)
        self.cpu_usage.labels(**cloud_labels).set(cpu)
        
        # Record memory
        memory = metrics_data.get("memory_percent", 50)
        self.memory_usage.labels(**cloud_labels).set(memory)
        
        # Record error rate
        error_rate = metrics_data.get("error_rate", 0.5)
        self.error_rate.labels(**cloud_labels).set(error_rate)

    @abstractmethod
    def generate_metrics(self) -> Dict[str, Any]:
        """Generate cloud-specific metrics. Override in subclass."""
        pass

    def run(self, debug: bool = False):
        """Run Flask app."""
        logger.info(f"Starting {self.cloud_name.upper()} simulator for {self.service_name}")
        logger.info(f"Service running on http://0.0.0.0:{self.service_port}")
        logger.info(f"Metrics available at http://0.0.0.0:{self.service_port}/metrics")
        
        # Simulation thread
        def simulate_continuously():
            while True:
                self.simulate_request()
                time.sleep(2)  # Generate metrics every 2 seconds
        
        import threading
        sim_thread = threading.Thread(target=simulate_continuously, daemon=True)
        sim_thread.start()
        
        # Run Flask
        self.app.run(host="0.0.0.0", port=self.service_port, debug=debug, use_reloader=False)