"""AWS Cloud Simulator - EC2/ECS metrics via Prometheus."""

import random
from mocks.base_simulator import BaseSimulator

class AWSSimulator(BaseSimulator):
    """Simulates AWS EC2/ECS service metrics. High performance under load."""

    def __init__(self, service_name: str = "service-cache-aws", service_port: int = 8001):
        super().__init__(service_name, "aws", service_port, service_port)

    def generate_metrics(self, active_requests: int) -> dict:
        """Generate AWS-specific metrics based on active load."""
        latency = 20.0 + (active_requests * 1.5) + random.gauss(0, 2)
        cpu = min(100.0, 15.0 + (active_requests * 0.8) + random.gauss(0, 1))
        
        if cpu < 80:
            error_rate = 0.1
        else:
            error_rate = 0.1 + ((cpu - 80) * 0.5)
            
        memory = min(100.0, 30.0 + (active_requests * 0.2))

        return {
            "latency_ms": max(1, latency),
            "cpu": max(1, cpu),
            "memory_percent": memory,
            "error_rate": min(100, error_rate)
        }

if __name__ == "__main__":
    import os
    port = int(os.getenv("SERVICE_PORT", "8001"))
    simulator = AWSSimulator(service_port=port)
    simulator.run()