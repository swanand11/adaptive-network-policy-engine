"""DigitalOcean Cloud Simulator - Droplet service metrics via Prometheus.

UPDATED: Traffic-driven metrics reflecting actual load from load balancer.
DigitalOcean characteristics: Higher latency, lower performance, struggles under heavy load.
"""

import random
from mocks.base_simulator import BaseSimulator

class DigitalOceanSimulator(BaseSimulator):
    """Simulates DigitalOcean service metrics. Lower performance under load."""

    def __init__(self, service_name: str = "service-api", service_port: int = 8003):
        super().__init__(service_name, "digitalocean", service_port, service_port)

    def generate_metrics(self, active_requests: int) -> dict:
        """Generate DigitalOcean-specific metrics based on active load.
        
        DigitalOcean characteristics:
        - Higher base latency (60ms)
        - Struggles with scaling
        - Higher error rate, especially under load
        """
        # Latency scales significantly with load
        latency = 60.0 + (active_requests * 5.0) + random.gauss(0, 5)
        
        # CPU increases rapidly
        cpu = min(100.0, 30.0 + (active_requests * 2.5) + random.gauss(0, 3))
        
        # Error rate increases early
        if cpu < 60:
            error_rate = 1.0
        else:
            error_rate = 1.0 + ((cpu - 60) * 1.5)
            
        memory = min(100.0, 50.0 + (active_requests * 0.5))

        return {
            "latency_ms": max(1, latency),
            "cpu": max(1, cpu),
            "memory_percent": memory,
            "error_rate": min(100, error_rate)
        }
    
    def compute_resource_usage(self, active_connections: int, rps: float) -> tuple:
        """Compute CPU and memory based on traffic load.
        
        Args:
            active_connections: Current active connections
            rps: Requests per second
            
        Returns:
            (cpu_percent, memory_percent)
        """
        # DigitalOcean scales poorly
        base_cpu = 25.0
        cpu = min(100.0, base_cpu + (active_connections * 4.0) + (rps * 1.2))
        
        base_memory = 45.0
        memory = min(100.0, base_memory + (active_connections * 2.0) + (rps * 0.8))
        
        return (cpu, memory)

if __name__ == "__main__":
    import os
    port = int(os.getenv("SERVICE_PORT", "8003"))
    simulator = DigitalOceanSimulator(service_port=port)
    simulator.run()