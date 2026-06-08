"""AWS Cloud Simulator - EC2/ECS metrics via Prometheus.

UPDATED: Traffic-driven metrics reflecting actual load from load balancer.
AWS characteristics: Low latency, high performance, scales well under load.
"""

import random
from mocks.base_simulator import BaseSimulator

class AWSSimulator(BaseSimulator):
    """Simulates AWS EC2/ECS service metrics. High performance under load."""

    def __init__(self, service_name: str = "service-cache-aws", service_port: int = 8001):
        super().__init__(service_name, "aws", service_port, service_port)

    def generate_metrics(self, active_requests: int) -> dict:
        """Generate AWS-specific metrics based on active load.
        
        AWS characteristics:
        - Low base latency (20ms)
        - Scales well with load
        - Low error rate until high CPU
        """
        # Latency scales gradually with load
        latency = 20.0 + (active_requests * 1.5) + random.gauss(0, 2)
        
        # CPU increases with load but AWS handles it well
        cpu = min(100.0, 15.0 + (active_requests * 0.8) + random.gauss(0, 1))
        
        # Error rate stays low until CPU is high
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
    
    def compute_resource_usage(self, active_connections: int, rps: float) -> tuple:
        """Compute CPU and memory based on traffic load.
        
        Args:
            active_connections: Current active connections
            rps: Requests per second
            
        Returns:
            (cpu_percent, memory_percent)
        """
        # AWS scales efficiently
        base_cpu = 10.0
        cpu = min(100.0, base_cpu + (active_connections * 2.0) + (rps * 0.5))
        
        base_memory = 25.0
        memory = min(100.0, base_memory + (active_connections * 1.0) + (rps * 0.3))
        
        return (cpu, memory)

if __name__ == "__main__":
    import os
    port = int(os.getenv("SERVICE_PORT", "8001"))
    simulator = AWSSimulator(service_port=port)
    simulator.run()