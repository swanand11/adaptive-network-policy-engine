"""Base Cloud Simulator - Flask server with Prometheus metrics.

UPDATED: Traffic-driven metrics instead of random generation.
Metrics now reflect actual request load from the load balancer.
"""

import random
import time
import logging
import threading
from typing import Dict, Any
from datetime import datetime
from abc import ABC, abstractmethod
from flask import Flask, Response, request
from prometheus_client import Counter, Gauge, Summary, Histogram, generate_latest, CollectorRegistry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricsState:
    """Track real-time metrics state based on actual traffic."""
    
    def __init__(self):
        self.total_requests = 0
        self.total_errors = 0
        self.active_connections = 0
        self.queue_depth = 0
        self.latency_samples = []
        self.bandwidth_bytes = 0
        self._lock = threading.Lock()
    
    def record_request(self, latency_ms: float, is_error: bool = False, bytes_sent: int = 0):
        """Record a completed request."""
        with self._lock:
            self.total_requests += 1
            if is_error:
                self.total_errors += 1
            self.latency_samples.append(latency_ms)
            if len(self.latency_samples) > 100:
                self.latency_samples.pop(0)
            self.bandwidth_bytes += bytes_sent
    
    def get_error_rate(self) -> float:
        """Calculate current error rate percentage."""
        with self._lock:
            if self.total_requests == 0:
                return 0.0
            return (self.total_errors / self.total_requests) * 100
    
    def get_avg_latency(self) -> float:
        """Calculate average latency from recent samples."""
        with self._lock:
            if not self.latency_samples:
                return 0.0
            return sum(self.latency_samples) / len(self.latency_samples)


class BaseSimulator(ABC):
    """Base class for cloud simulators with traffic-driven Prometheus metrics."""

    def __init__(self, service_name: str, cloud_name: str, service_port: int, metrics_port: int):
        self.service_name = service_name
        self.cloud_name = cloud_name
        self.service_port = service_port
        self.metrics_port = metrics_port
        
        self.app = Flask(f"{cloud_name}-simulator")
        self.registry = CollectorRegistry()
        
        # Prometheus metrics
        self.request_latency = Gauge(
            'latency_ms',
            'Request latency in milliseconds',
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
        self.active_connections_gauge = Gauge(
            'active_connections',
            'Active connections',
            ['cloud', 'service_id'],
            registry=self.registry
        )
        self.requests_per_second = Gauge(
            'requests_per_second',
            'Requests per second',
            ['cloud', 'service_id'],
            registry=self.registry
        )

        # Traffic-driven state
        self.metrics_state = MetricsState()
        self._lock = threading.Lock()
        
        # Start background metrics updater
        self._start_metrics_updater()
        
        self._setup_routes()

    def _start_metrics_updater(self):
        """Background thread to update computed metrics."""
        def update_loop():
            last_request_count = 0
            while True:
                try:
                    cloud_labels = {"cloud": self.cloud_name, "service_id": self.service_name}
                    
                    # Calculate requests per second
                    current_requests = self.metrics_state.total_requests
                    rps = current_requests - last_request_count
                    last_request_count = current_requests
                    self.requests_per_second.labels(**cloud_labels).set(rps)
                    
                    # Update error rate
                    error_rate = self.metrics_state.get_error_rate()
                    self.error_rate.labels(**cloud_labels).set(error_rate)
                    
                    # Update active connections
                    self.active_connections_gauge.labels(**cloud_labels).set(
                        self.metrics_state.active_connections
                    )
                    
                    # Compute CPU and memory based on load
                    cpu, memory = self.compute_resource_usage(
                        self.metrics_state.active_connections,
                        rps
                    )
                    self.cpu_usage.labels(**cloud_labels).set(cpu)
                    self.memory_usage.labels(**cloud_labels).set(memory)
                    
                    time.sleep(1)  # Update every second
                except Exception as e:
                    logger.error(f"Error in metrics updater: {e}")
                    time.sleep(1)
        
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()

    def _setup_routes(self):
        @self.app.route("/")
        @self.app.route("/request")
        def handle_request():
            """Handle incoming traffic and generate realistic metrics."""
            start_time = time.time()
            
            with self._lock:
                self.metrics_state.active_connections += 1
            
            try:
                cloud_labels = {"cloud": self.cloud_name, "service_id": self.service_name}
                
                # Generate metrics based on current load
                metrics_data = self.generate_metrics(self.metrics_state.active_connections)
                
                latency_s = metrics_data.get("latency_ms", 100) / 1000.0
                error_rate = metrics_data.get("error_rate", 0.5)

                # Simulate processing time
                time.sleep(latency_s)

                # Determine if this request errors
                is_error = random.random() < (error_rate / 100.0)
                
                # Calculate actual latency
                actual_latency_ms = (time.time() - start_time) * 1000
                
                # Record metrics
                self.request_count.labels(**cloud_labels).inc()
                self.request_latency.labels(**cloud_labels).set(actual_latency_ms)
                
                # Update state
                response_size = 200 if not is_error else 100
                self.metrics_state.record_request(
                    actual_latency_ms,
                    is_error,
                    response_size
                )
                
                if is_error:
                    return Response(
                        '{"error": "Internal Server Error", "status": "failed"}',
                        status=500,
                        mimetype='application/json'
                    )
                
                return {
                    "status": "ok",
                    "service": self.service_name,
                    "cloud": self.cloud_name,
                    "active_connections": self.metrics_state.active_connections,
                    "latency_ms": actual_latency_ms,
                    "total_requests": self.metrics_state.total_requests
                }
            finally:
                with self._lock:
                    self.metrics_state.active_connections -= 1

        @self.app.route("/metrics")
        def metrics():
            return generate_latest(self.registry), 200, {
                "Content-Type": "text/plain; version=0.0.4"
            }
        
        @self.app.route("/health")
        def health():
            return {"status": "healthy", "service": self.service_name}

    @abstractmethod
    def generate_metrics(self, active_requests: int) -> Dict[str, Any]:
        """Generate cloud-specific metrics based on current active requests. Override in subclass."""
        pass
    
    @abstractmethod
    def compute_resource_usage(self, active_connections: int, rps: float) -> tuple:
        """Compute CPU and memory usage based on load. Returns (cpu_percent, memory_percent)."""
        pass

    def run(self, debug: bool = False):
        logger.info(f"Starting {self.cloud_name.upper()} simulator for {self.service_name}")
        logger.info(f"Service running on http://0.0.0.0:{self.service_port}")
        logger.info(f"Traffic-driven metrics enabled")
        
        # We must run threaded=True to handle concurrent requests
        self.app.run(host="0.0.0.0", port=self.service_port, debug=debug, use_reloader=False, threaded=True)