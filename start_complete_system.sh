#!/bin/bash
# Complete System Startup Script
# Starts the entire Multi-Cloud Policy Orchestrator

set -e

echo "=========================================="
echo "Multi-Cloud Policy Orchestrator"
echo "Complete System Startup"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

step() {
    echo -e "${GREEN}▶${NC} $1"
}

info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Step 1: Start infrastructure
step "Step 1: Starting infrastructure (Kafka, Prometheus, Redis)..."
docker compose up -d kafka zookeeper prometheus redis load-balancer-proxy kafka-exporter
info "Waiting 30 seconds for services to initialize..."
sleep 30

# Verify Kafka is ready
info "Verifying Kafka is ready..."
until docker exec -it kafka_1 kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    echo "Waiting for Kafka..."
    sleep 5
done
echo "✓ Kafka is ready"

# Step 2: Start simulators
step "Step 2: Starting cloud simulators..."
python3 -m mocks.aws_simulator > logs/aws_simulator.log 2>&1 &
echo $! > pids/aws_simulator.pid
sleep 2

python3 -m mocks.aks_simulator > logs/aks_simulator.log 2>&1 &
echo $! > pids/aks_simulator.pid
sleep 2

python3 -m mocks.dodroplets_simulator > logs/do_simulator.log 2>&1 &
echo $! > pids/do_simulator.pid
sleep 2

info "Waiting for simulators to be ready..."
sleep 5

# Verify simulators
for port in 8001 8002 8003; do
    if curl -s http://localhost:$port/health > /dev/null 2>&1; then
        echo "✓ Simulator on port $port is ready"
    else
        echo "✗ Simulator on port $port failed to start"
    fi
done

# Step 3: Start Prometheus adapter
step "Step 3: Starting Prometheus adapter..."
python3 -m kafka_core.prometheus_kafka_adaptar > logs/prometheus_adapter.log 2>&1 &
echo $! > pids/prometheus_adapter.pid
sleep 5

# Step 4: Start service agents
step "Step 4: Starting service agents..."
export SERVICE_ID=service-cache-aws
python3 -m runners.service_agent_runner > logs/service_agent_1.log 2>&1 &
echo $! > pids/service_agent_1.pid
sleep 2

export SERVICE_ID=service-db
python3 -m runners.service_agent_runner > logs/service_agent_2.log 2>&1 &
echo $! > pids/service_agent_2.pid
sleep 2

export SERVICE_ID=service-cache
python3 -m runners.service_agent_runner > logs/service_agent_3.log 2>&1 &
echo $! > pids/service_agent_3.pid
sleep 2

# Step 5: Start topology agents
step "Step 5: Starting topology agents..."
python3 -m runners.topo_agent_aws_runner > logs/topo_agent_aws.log 2>&1 &
echo $! > pids/topo_agent_aws.pid
sleep 2

python3 -m runners.topo_agent_aks_runner > logs/topo_agent_aks.log 2>&1 &
echo $! > pids/topo_agent_aks.pid
sleep 2

python3 -m runners.topo_agent_do_runner > logs/topo_agent_do.log 2>&1 &
echo $! > pids/topo_agent_do.pid
sleep 2

# Step 6: Start governance agent
step "Step 6: Starting governance agent..."
python3 -m runners.governance_agent_runner > logs/governance_agent.log 2>&1 &
echo $! > pids/governance_agent.pid
sleep 5

# Step 7: Start HITL
step "Step 7: Starting HITL API..."
python3 -m hitl.api > logs/hitl.log 2>&1 &
echo $! > pids/hitl.pid
sleep 5

# Verify HITL
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "✓ HITL API is ready"
else
    echo "✗ HITL API failed to start"
fi

# Step 8: Start NGINX weight consumer
step "Step 8: Starting NGINX weight consumer..."
python3 -m consumers.nginx_weight_consumer > logs/nginx_consumer.log 2>&1 &
echo $! > pids/nginx_consumer.pid
sleep 3

# Step 9: Start Backend API
step "Step 9: Starting Backend API..."
python3 -m backend.api > logs/backend.log 2>&1 &
echo $! > pids/backend.pid
sleep 5

# Verify Backend API
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    echo "✓ Backend API is ready"
else
    echo "✗ Backend API failed to start"
fi

# Step 10: Start traffic generator (optional)
step "Step 10: Starting traffic generator..."
info "Traffic generator will run for 5 minutes with moderate load"
python3 -m traffic_generator.generator --profile moderate --duration 300 > logs/traffic_generator.log 2>&1 &
echo $! > pids/traffic_generator.pid

echo ""
echo "=========================================="
echo "System Started Successfully!"
echo "=========================================="
echo ""
echo "Access Points:"
echo "  Dashboard:        http://localhost:5173"
echo "  Backend API:      http://localhost:8000"
echo "  HITL Portal:      http://localhost:8080"
echo "  Prometheus:       http://localhost:9090"
echo "  WebSocket:        ws://localhost:8000/ws"
echo ""
echo "Simulators:"
echo "  AWS:              http://localhost:8001"
echo "  Azure:            http://localhost:8002"
echo "  DigitalOcean:     http://localhost:8003"
echo ""
echo "Logs are in: logs/"
echo "PIDs are in: pids/"
echo ""
echo "To stop the system, run: ./stop_system.sh"
echo "To test the system, run: ./test_system.sh"
echo ""
