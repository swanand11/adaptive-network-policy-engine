"""AWS Cloud Simulator - EC2/ECS metrics via Prometheus."""

import random
from mocks.base_simulator import BaseSimulator


class AWSSimulator(BaseSimulator):
    """Simulates AWS EC2/ECS service metrics."""

    def __init__(self, service_name: str = "service-api", service_port: int = 8001):
        super().__init__(service_name, "aws", service_port, service_port)
        self.latency_baseline = 45  # AWS typically lower latency
        self.cpu_baseline = 35
        self.error_rate_baseline = 0.5

    def generate_metrics(self) -> dict:
        """Generate AWS-specific metrics."""
        latency = max(10, self.latency_baseline + random.gauss(0, 15))
        cpu = max(5, min(100, self.cpu_baseline + random.gauss(0, 10)))
        error_rate = max(0, min(10, self.error_rate_baseline + random.gauss(0, 0.3)))
        memory = max(20, min(100, random.randint(20, 80)))

        return {
            "latency_ms": latency,
            "cpu": cpu,
            "memory_percent": memory,
            "error_rate": error_rate,
            "requests_per_sec": random.randint(100, 500),
            "region": random.choice(["us-east-1", "us-west-2", "eu-west-1"]),
        }


if __name__ == "__main__":
    import os

    port = int(os.getenv("SERVICE_PORT", "8001"))
    simulator = AWSSimulator(service_port=port)
    simulator.run()