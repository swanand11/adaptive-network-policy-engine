import sys
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"

from runners.topo_agent_aws_runner import main

if __name__ == "__main__":
    print("🚀 Starting Topo Agent AWS Runner...")
    main()
