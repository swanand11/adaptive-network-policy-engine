import sys
import os
import signal
import subprocess
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# ensure local kafka env used when running from this script
os.environ["KAFKA_BOOTSTRAP_SERVERS"] = "localhost:9092"

RUNNERS = [
    "runners.topo_agent_aws_runner",
    "runners.topo_agent_aks_runner",
    "runners.topo_agent_do_runner",
]


def start_all_runners():
    procs = []

    def _start(module_name: str):
        cmd = [sys.executable, "-m", module_name]
        env = os.environ.copy()
        env["PYTHONPATH"] = str(REPO_ROOT) + os.pathsep + env.get("PYTHONPATH", "")
        # inherit stdio so logs appear in this terminal
        return subprocess.Popen(cmd, env=env)

    # start subprocess for each runner
    for m in RUNNERS:
        print(f"🚀 Launching {m}...")
        p = _start(m)
        procs.append((m, p))

    # handle signals to forward shutdown to children
    def _signal_handler(signum, frame):
        print(f"Received signal {signum}, shutting down runners...")
        for name, proc in procs:
            if proc.poll() is None:
                print(f"Terminating {name} (pid={proc.pid})")
                proc.terminate()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    try:
        # wait for all children; if any exits, keep others running until signalled
        while True:
            alive = [p for _, p in procs if p.poll() is None]
            if not alive:
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        _signal_handler(signal.SIGINT, None)
    finally:
        # ensure all children are stopped
        for name, proc in procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()


if __name__ == "__main__":
    start_all_runners()
