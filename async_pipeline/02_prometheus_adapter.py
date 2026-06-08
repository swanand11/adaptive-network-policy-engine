import sys
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"
os.environ["PROMETHEUS_AWS_URL"] = "http://localhost:8001/metrics"
os.environ["PROMETHEUS_AKS_URL"] = "http://localhost:8002/metrics"
os.environ["PROMETHEUS_DO_URL"] = "http://localhost:8003/metrics"

from kafka_core.prometheus_kafka_adaptar import main

if __name__ == "__main__":
    print("🚀 Starting Prometheus Kafka Adapter...")
    main()
