#!/bin/bash
set -e

echo "========================================================="
echo " Nexus Policy Engine - Baseline Benchmark (Agents OFF)   "
echo "========================================================="

# 1. Stop any running agents to ensure no dynamic weight updating
echo "[*] Ensuring agent runners are stopped..."
pkill -f "pipeline_live_runner" || true
pkill -f "decision_producer" || true
pkill -f "execution_consumer" || true

# 2. Restart docker-compose to reset proxy weights to 33/33/34 and clear state
echo "[*] Restarting infrastructure to ensure clean state..."
sudo docker compose down
sudo docker compose up -d

echo "[*] Waiting for services to become healthy (15s)..."
sleep 15

# 3. Run Benchmark
echo "[*] Executing Load Test..."
echo "    Target: http://localhost:9000/ (Execution Proxy)"
echo "    Concurrency: 50 | Total Requests: 1000"

python3 benchmark.py --url http://localhost:9000/ --concurrency 50 --requests 1000 --output benchmark_results.csv

echo ""
echo "========================================================="
echo "[+] Benchmark Complete!"
echo "[+] Results appended to: benchmark_results.csv"
echo "========================================================="
