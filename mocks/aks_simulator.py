"""AKS/Azure Cloud Simulator - Kubernetes service metrics via Prometheus."""

import random
from mocks.base_simulator import BaseSimulator

class AKSSimulator(BaseSimulator):
    """Simulates AKS/Azure service metrics. Medium performance under load."""

    def __init__(self, service_name: str = "service-api", service_port: int = 8002):
        super().__init__(service_name, "aks", service_port, service_port)

    def generate_metrics(self, active_requests: int) -> dict:
        """Generate Azure-specific metrics based on active load."""
        latency = 40.0 + (active_requests * 3.0) + random.gauss(0, 3)
        cpu = min(100.0, 25.0 + (active_requests * 1.5) + random.gauss(0, 2))
        
        if cpu < 70:
            error_rate = 0.5
        else:
            error_rate = 0.5 + ((cpu - 70) * 0.8)
            
        memory = min(100.0, 40.0 + (active_requests * 0.3))

        return {
            "latency_ms": max(1, latency),
            "cpu": max(1, cpu),
            "memory_percent": memory,
            "error_rate": min(100, error_rate)
        }

if __name__ == "__main__":
    import os
    port = int(os.getenv("SERVICE_PORT", "8002"))
    simulator = AKSSimulator(service_port=port)
    simulator.run()