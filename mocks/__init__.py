"""Mock Cloud Services - Simulates AWS, AKS, DigitalOcean metrics for testing.

Available Simulators:
  - AWSSimulator: EC2/ECS metrics (low latency baseline: 45ms)
  - AKSSimulator: Kubernetes Pod metrics (moderate latency baseline: 65ms)  
  - DigitalOceanSimulator: Droplet metrics (moderate-low latency baseline: 55ms)
"""

from mocks.aws_simulator import AWSSimulator
from mocks.aks_simulator import AKSSimulator
from mocks.dodroplets_simulator import DigitalOceanSimulator

__all__ = ["AWSSimulator", "AKSSimulator", "DigitalOceanSimulator"]