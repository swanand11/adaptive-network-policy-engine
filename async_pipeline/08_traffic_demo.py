import asyncio
import aiohttp
import time
import sys
from collections import Counter

# ANSI colors for premium visual styling
GREEN = "\033[1;32m"
YELLOW = "\033[1;33m"
RED = "\033[1;31m"
BLUE = "\033[1;34m"
CYAN = "\033[1;36m"
BOLD = "\033[1;m"
RESET = "\033[0m"

PROXY_URL = "http://localhost:9000"

class TrafficDemo:
    def __init__(self):
        self.stats = Counter()
        self.total_baseline_sent = 0
        self.total_surge_sent = 0
        self.running = True
        self.phase = "Phase 1: Baseline Traffic"
        self.surge_target = None
        self.surge_rate = 0

    def print_header(self, text, color=CYAN):
        print(f"\n{color}{'='*60}{RESET}")
        print(f"{color}{text.center(60)}{RESET}")
        print(f"{color}{'='*60}{RESET}\n")

    async def send_baseline_request(self, session):
        """Sends normal user traffic that is load-balanced dynamically."""
        if not self.running:
            return
        try:
            start = time.time()
            async with session.get(f"{PROXY_URL}/", timeout=3) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    cloud = data.get("cloud", "unknown").lower()
                    # Map Azure to aks for display consistency
                    if cloud == "azure":
                        cloud = "aks"
                    self.stats[cloud] += 1
                    self.total_baseline_sent += 1
                else:
                    self.stats["errors"] += 1
        except Exception:
            self.stats["errors"] += 1

    async def send_surge_request(self, session, target):
        """Sends targeted high-rate traffic directly to a specific CSP to simulate overload."""
        if not self.running:
            return
        headers = {"X-CSP-Target": target}
        try:
            async with session.get(f"{PROXY_URL}/", headers=headers, timeout=3) as resp:
                # We do not count surge traffic in routing stats to observe only baseline shifting
                self.total_surge_sent += 1
        except Exception:
            pass

    async def baseline_loop(self):
        """Generates continuous background user traffic at 10 requests per second."""
        async with aiohttp.ClientSession() as session:
            while self.running:
                tasks = [self.send_baseline_request(session) for _ in range(10)]
                await asyncio.gather(*tasks)
                await asyncio.sleep(1)

    async def surge_loop(self):
        """Generates high-rate surge requests on the target CSP when enabled."""
        async with aiohttp.ClientSession() as session:
            while self.running:
                if self.surge_rate > 0 and self.surge_target:
                    tasks = [self.send_surge_request(session, self.surge_target) for _ in range(self.surge_rate)]
                    await asyncio.gather(*tasks)
                await asyncio.sleep(0.5)

    async def stats_reporter(self):
        """Periodically prints high-fidelity live distribution metrics to the terminal."""
        while self.running:
            await asyncio.sleep(3)
            total = sum(self.stats.values())
            if total == 0:
                continue

            print(f"\n📊 {BOLD}{self.phase}{RESET} | Live User Traffic Stats:")
            for csp in ["aws", "aks", "do", "errors"]:
                count = self.stats[csp]
                pct = (count / total) * 100
                bar = "█" * int(pct // 5)
                color = GREEN if csp in ["aws", "aks", "do"] else RED
                if csp == "aws":
                    color = YELLOW
                elif csp == "do":
                    color = BLUE
                
                print(f"  {csp.upper():6} [{color}{bar:<20}{RESET}] {count:4} requests ({pct:.1f}%)")
            
            print(f"  {CYAN}Total baseline sent:{RESET} {self.total_baseline_sent} | {RED}Total surge sent:{RESET} {self.total_surge_sent}")
            # Reset counters occasionally to show moving window distribution
            self.stats.clear()

    async def run_scenario(self):
        self.print_header("PROACTIVE MULTI-CLOUD POLICY ORCHESTRATION DEMO", BOLD + GREEN)
        print(f"Connecting to load balancer proxy at {PROXY_URL}...")
        
        # Test connection
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{PROXY_URL}/health", timeout=5) as resp:
                    if resp.status == 200:
                        print(f"✓ Connected to load balancer successfully!")
                    else:
                        print(f"✗ Load balancer returned status {resp.status}. Please ensure docker stack is up!")
                        return
            except Exception as e:
                print(f"✗ Connection error: {e}. Is the proxy running on port 9000?")
                return

        # Start traffic tasks
        baseline_task = asyncio.create_task(self.baseline_loop())
        surge_task = asyncio.create_task(self.surge_loop())
        reporter_task = asyncio.create_task(self.stats_reporter())

        # --- Phase 1: Baseline ---
        self.phase = "Phase 1: Baseline Load"
        self.print_header("PHASE 1: STABLE BASELINE LOAD (NORMAL OPERATION)", BLUE)
        print("Sending normal user traffic. Dynamic weights are balanced at 33/33/34.")
        print("Mocks are healthy and operating in optimal latency envelopes.")
        await asyncio.sleep(15)

        # --- Phase 2: Surge AWS ---
        self.phase = "Phase 2: Traffic Spike on AWS"
        self.print_header("PHASE 2: SIMULATING APPLICATION SURGE ON AWS", RED)
        print("Spiking load on AWS upstream to simulate high latency and resource contention...")
        self.surge_target = "aws"
        self.surge_rate = 30  # 60 requests per second
        await asyncio.sleep(25)

        # --- Phase 3: Proactive Relocation ---
        self.phase = "Phase 3: Proactive Policy Relocation"
        self.print_header("PHASE 3: DETECTED OVERLOAD - PROACTIVE ROUTING ACTIVE", GREEN)
        print("Topography Agent has solved flow optimization. Governance has approved.")
        print("Observe how baseline user traffic is actively shifted away from the overloaded AWS node!")
        await asyncio.sleep(25)

        # --- Phase 4: Recovery ---
        self.phase = "Phase 4: Cooldown & Recovery"
        self.print_header("PHASE 4: SURGE ENDED - COOLING DOWN AWS", YELLOW)
        print("Stopping AWS surge. AWS metrics will recover. Policy engine should dynamically restore balance.")
        self.surge_rate = 0
        self.surge_target = None
        await asyncio.sleep(20)

        # Teardown
        self.running = False
        baseline_task.cancel()
        surge_task.cancel()
        reporter_task.cancel()
        self.print_header("DEMO COMPLETE - POLICY ENGINE STABLE", BOLD + GREEN)

if __name__ == "__main__":
    demo = TrafficDemo()
    try:
        asyncio.run(demo.run_scenario())
    except KeyboardInterrupt:
        print("\nStopping traffic generator.")
