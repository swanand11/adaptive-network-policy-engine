"""AKS Cloud Simulator - Azure Kubernetes Service metrics via Prometheus."""

import random
from mocks.base_simulator import BaseSimulator


class AKSSimulator(BaseSimulator):
    """Simulates AKS (Azure Kubernetes Service) pod metrics."""

    def __init__(self, service_name: str = "service-db", service_port: int = 8002):
        super().__init__(service_name, "aks", service_port, service_port)
        self.latency_baseline = 65  # AKS typically moderate latency
        self.cpu_baseline = 45
        self.error_rate_baseline = 1.0

    def generate_metrics(self) -> dict:
        """Generate AKS-specific metrics."""
        latency = max(10, self.latency_baseline + random.gauss(0, 20))
        cpu = max(5, min(100, self.cpu_baseline + random.gauss(0, 15)))
        error_rate = max(0, min(10, self.error_rate_baseline + random.gauss(0, 0.5)))
        memory = max(30, min(100, random.randint(30, 85)))

        # Occasional spikes (pod rebalancing)
        if random.random() < 0.1:
            latency *= 1.5
            cpu *= 1.3
            error_rate *= 2

        return {
            "latency_ms": latency,
            "cpu": cpu,
            "memory_percent": memory,
            "error_rate": error_rate,
            "requests_per_sec": random.randint(80, 450),
            "pod_count": random.randint(1, 5),
            "namespace": random.choice(["default", "production", "staging"]),
        }


if __name__ == "__main__":
    import os

    port = int(os.getenv("SERVICE_PORT", "8002"))
    simulator = AKSSimulator(service_port=port)
    simulator.run()