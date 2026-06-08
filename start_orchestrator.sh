#!/bin/bash

# Multi-Agent Multi-Cloud Network Policy Orchestrator
# Startup Script - Activates venv and runs launcher

set -e

echo "=========================================="
echo "Multi-Cloud Policy Orchestrator"
echo "=========================================="
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check Python version
echo "Checking Python version..."
python3 --version
echo ""

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
    echo "✓ Virtual environment created"
fi

# Activate virtual environment
echo "Activating virtual environment..."
source .venv/bin/activate
echo "✓ Virtual environment activated"
echo "Using Python: $(which python3)"
echo ""

# Install/upgrade dependencies
echo "Installing dependencies..."
pip install -q --upgrade pip
pip install -q -r requirements.txt
echo "✓ Dependencies installed"
echo ""

# Check Docker
echo "Checking Docker..."
if ! command -v docker &> /dev/null; then
    echo "✗ Docker not found. Please install Docker first."
    exit 1
fi
echo "✓ Docker found"
echo ""

# Check Docker Compose
echo "Checking Docker Compose..."
if ! command -v docker &> /dev/null; then
    echo "✗ Docker Compose not found. Please install Docker Compose first."
    exit 1
fi
echo "✓ Docker Compose found"
echo ""

# Start infrastructure services
echo "Starting infrastructure services (Kafka, Redis, Prometheus)..."
docker compose up -d --remove-orphans kafka zookeeper redis prometheus
echo "✓ Infrastructure services started"
echo ""

# Wait for services to be ready
echo "Waiting for services to be ready..."
sleep 10
echo "✓ Services ready"
echo ""

# Start the orchestrator
echo "=========================================="
echo "Starting Orchestrator..."
echo "=========================================="
echo ""

# Run with sudo -E to preserve environment
sudo -E python3 -m launcher.main

# Cleanup on exit
trap 'echo ""; echo "Shutting down..."; docker compose down' EXIT

