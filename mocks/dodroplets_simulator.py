"""DigitalOcean Simulator - Droplet metrics via Prometheus."""

import random
from mocks.base_simulator import BaseSimulator


class DigitalOceanSimulator(BaseSimulator):
    """Simulates DigitalOcean Droplet metrics."""

    def __init__(self, service_name: str = "service-cache", service_port: int = 8003):
        super().__init__(service_name, "digitalocean", service_port, service_port)
        self.latency_baseline = 55  # DO moderate-low latency
        self.cpu_baseline = 40
        self.error_rate_baseline = 0.8

    def generate_metrics(self) -> dict:
        """Generate DigitalOcean Droplet-specific metrics."""
        latency = max(10, self.latency_baseline + random.gauss(0, 12))
        cpu = max(5, min(100, self.cpu_baseline + random.gauss(0, 12)))
        error_rate = max(0, min(10, self.error_rate_baseline + random.gauss(0, 0.4)))
        memory = max(25, min(100, random.randint(25, 75)))

        # Occasional traffic bursts
        if random.random() < 0.15:
            cpu *= 1.3

        return {
            "latency_ms": latency,
            "cpu": cpu,
            "memory_percent": memory,
            "error_rate": error_rate,
            "requests_per_sec": random.randint(120, 550),
            "bandwidth_mbps": random.randint(50, 300),
            "region": random.choice(["nyc3", "sfo3", "lon1", "sgp1"]),
        }


if __name__ == "__main__":
    import os

    port = int(os.getenv("SERVICE_PORT", "8003"))
    simulator = DigitalOceanSimulator(service_port=port)
    simulator.run()