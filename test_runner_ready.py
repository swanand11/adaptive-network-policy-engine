#!/usr/bin/env python
"""Quick test of runner instantiation."""

from runners.service_agent_runner import ParallelServiceAgentRunner

try:
    runner = ParallelServiceAgentRunner()
    print("✓ Runner instantiated successfully")
    print(f"✓ Loaded {len(runner.config['agents'])} agents")
    print(f"✓ Group ID: {runner.group_id}")
    print(f"✓ Queue max size: {runner.config['queue_config']['max_size']}")
    print("\nAgent Configuration:")
    for agent in runner.config['agents']:
        print(f"  - {agent['service_id']}@{agent['cloud']} (alpha={agent['alpha']}, threshold={agent['latency_threshold']}ms)")
    print("\n✓✓✓ Runner ready for integration testing ✓✓✓")
except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
