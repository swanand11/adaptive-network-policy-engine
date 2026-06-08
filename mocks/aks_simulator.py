"""AKS/Azure Cloud Simulator - Kubernetes service metrics via Prometheus.

UPDATED: Traffic-driven metrics reflecting actual load from load balancer.
Azure characteristics: Medium latency, moderate performance, higher error rate under load.
"""

import random
from mocks.base_simulator import BaseSimulator

class AKSSimulator(BaseSimulator):
    """Simulates AKS/Azure service metrics. Medium performance under load."""

    def __init__(self, service_name: str = "service-api", service_port: int = 8002):
        super().__init__(service_name, "aks", service_port, service_port)

    def generate_metrics(self, active_requests: int) -> dict:
        """Generate Azure-specific metrics based on active load.
        
        Azure characteristics:
        - Medium base latency (40ms)
        - Moderate scaling with load
        - Higher error rate under pressure
        """
        # Latency scales more with load than AWS
        latency = 40.0 + (active_requests * 3.0) + random.gauss(0, 3)
        
        # CPU increases faster than AWS
        cpu = min(100.0, 25.0 + (active_requests * 1.5) + random.gauss(0, 2))
        
        # Error rate increases earlier than AWS
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
    
    def compute_resource_usage(self, active_connections: int, rps: float) -> tuple:
        """Compute CPU and memory based on traffic load.
        
        Args:
            active_connections: Current active connections
            rps: Requests per second
            
        Returns:
            (cpu_percent, memory_percent)
        """
        # Azure scales moderately
        base_cpu = 20.0
        cpu = min(100.0, base_cpu + (active_connections * 3.0) + (rps * 0.8))
        
        base_memory = 35.0
        memory = min(100.0, base_memory + (active_connections * 1.5) + (rps * 0.5))
        
        return (cpu, memory)

if __name__ == "__main__":
    import os
    port = int(os.getenv("SERVICE_PORT", "8002"))
    simulator = AKSSimulator(service_port=port)
    simulator.run()