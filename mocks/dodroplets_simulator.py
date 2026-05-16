"""DigitalOcean Cloud Simulator - Droplet service metrics via Prometheus."""

import random
from mocks.base_simulator import BaseSimulator

class DigitalOceanSimulator(BaseSimulator):
    """Simulates DigitalOcean service metrics. Lower performance under load."""

    def __init__(self, service_name: str = "service-api", service_port: int = 8003):
        super().__init__(service_name, "digitalocean", service_port, service_port)

    def generate_metrics(self, active_requests: int) -> dict:
        """Generate DigitalOcean-specific metrics based on active load."""
        latency = 60.0 + (active_requests * 5.0) + random.gauss(0, 5)
        cpu = min(100.0, 30.0 + (active_requests * 2.5) + random.gauss(0, 3))
        
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

if __name__ == "__main__":
    import os
    port = int(os.getenv("SERVICE_PORT", "8003"))
    simulator = DigitalOceanSimulator(service_port=port)
    simulator.run()