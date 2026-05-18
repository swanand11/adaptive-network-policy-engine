#!/bin/bash
# System Shutdown Script
# Stops all components of the Multi-Cloud Policy Orchestrator

echo "=========================================="
echo "Multi-Cloud Policy Orchestrator"
echo "System Shutdown"
echo "=========================================="
echo ""

# Stop all Python processes
echo "Stopping Python processes..."
if [ -d "pids" ]; then
    for pidfile in pids/*.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            if kill -0 "$pid" 2>/dev/null; then
                echo "Stopping $(basename $pidfile .pid) (PID: $pid)"
                kill "$pid" 2>/dev/null || true
            fi
            rm "$pidfile"
        fi
    done
fi

# Stop Docker services
echo "Stopping Docker services..."
docker compose down

echo ""
echo "System stopped successfully!"
echo ""
