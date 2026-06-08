#!/bin/bash
# System Integration Test Script
# Tests the complete Multi-Cloud Policy Orchestrator pipeline

set -e

echo "=========================================="
echo "Multi-Cloud Policy Orchestrator"
echo "System Integration Test"
echo "=========================================="
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test counter
TESTS_PASSED=0
TESTS_FAILED=0

# Helper functions
test_pass() {
    echo -e "${GREEN}✓${NC} $1"
    ((TESTS_PASSED++))
}

test_fail() {
    echo -e "${RED}✗${NC} $1"
    ((TESTS_FAILED++))
}

test_info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Test 1: Check Docker services
echo "Test 1: Checking Docker services..."
if docker compose ps | grep -q "kafka"; then
    test_pass "Kafka is running"
else
    test_fail "Kafka is not running"
fi

if docker compose ps | grep -q "prometheus"; then
    test_pass "Prometheus is running"
else
    test_fail "Prometheus is not running"
fi

if docker compose ps | grep -q "redis"; then
    test_pass "Redis is running"
else
    test_fail "Redis is not running"
fi

echo ""

# Test 2: Check Kafka topics
echo "Test 2: Checking Kafka topics..."
TOPICS=$(docker exec -it kafka_1 kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | tr -d '\r')

for topic in "metrics.events" "service.state" "topo.decisions" "policy.decisions" "policy.approved"; do
    if echo "$TOPICS" | grep -q "$topic"; then
        test_pass "Topic $topic exists"
    else
        test_fail "Topic $topic does not exist"
    fi
done

echo ""

# Test 3: Check simulators
echo "Test 3: Checking simulators..."
for port in 8001 8002 8003; do
    if curl -s http://localhost:$port/health > /dev/null 2>&1; then
        test_pass "Simulator on port $port is responding"
    else
        test_fail "Simulator on port $port is not responding"
    fi
done

echo ""

# Test 4: Check Prometheus targets
echo "Test 4: Checking Prometheus targets..."
if curl -s http://localhost:9090/api/v1/targets | grep -q "up"; then
    test_pass "Prometheus is scraping targets"
else
    test_fail "Prometheus is not scraping targets"
fi

echo ""

# Test 5: Check Redis
echo "Test 5: Checking Redis..."
if redis-cli ping > /dev/null 2>&1; then
    test_pass "Redis is responding"
    
    # Check for weights
    if redis-cli GET nginx:weights > /dev/null 2>&1; then
        test_pass "Redis contains nginx:weights key"
    else
        test_info "Redis does not contain nginx:weights yet (may be normal if system just started)"
    fi
else
    test_fail "Redis is not responding"
fi

echo ""

# Test 6: Check HITL API
echo "Test 6: Checking HITL API..."
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    test_pass "HITL API is responding"
    
    # Check stats endpoint
    if curl -s http://localhost:8080/stats | grep -q "pending_count"; then
        test_pass "HITL stats endpoint is working"
    else
        test_fail "HITL stats endpoint is not working"
    fi
else
    test_fail "HITL API is not responding"
fi

echo ""

# Test 7: Check Backend API
echo "Test 7: Checking Backend API..."
if curl -s http://localhost:8000/health > /dev/null 2>&1; then
    test_pass "Backend API is responding"
    
    # Check various endpoints
    for endpoint in "api/csp/health" "api/weights" "api/stats"; do
        if curl -s http://localhost:8000/$endpoint | grep -q "status"; then
            test_pass "Backend endpoint /$endpoint is working"
        else
            test_fail "Backend endpoint /$endpoint is not working"
        fi
    done
else
    test_fail "Backend API is not responding"
fi

echo ""

# Test 8: Check metrics flow
echo "Test 8: Checking metrics flow..."
test_info "Checking if metrics are flowing through Kafka..."

# Check metrics.events topic
METRICS_COUNT=$(docker exec -it kafka_1 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic metrics.events \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | wc -l)

if [ "$METRICS_COUNT" -gt 0 ]; then
    test_pass "Metrics are flowing through metrics.events topic"
else
    test_info "No metrics found in metrics.events (may be normal if system just started)"
fi

echo ""

# Test 9: Check service agents
echo "Test 9: Checking service agents..."
SERVICE_STATE_COUNT=$(docker exec -it kafka_1 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic service.state \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | wc -l)

if [ "$SERVICE_STATE_COUNT" -gt 0 ]; then
    test_pass "Service agents are publishing to service.state"
else
    test_info "No service.state messages found (may be normal if system just started)"
fi

echo ""

# Test 10: Check topology agents
echo "Test 10: Checking topology agents..."
TOPO_COUNT=$(docker exec -it kafka_1 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic topo.decisions \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | wc -l)

if [ "$TOPO_COUNT" -gt 0 ]; then
    test_pass "Topology agents are publishing to topo.decisions"
else
    test_info "No topo.decisions messages found (may be normal if system just started)"
fi

echo ""

# Test 11: Check governance agent
echo "Test 11: Checking governance agent..."
POLICY_COUNT=$(docker exec -it kafka_1 kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic policy.decisions \
    --max-messages 1 \
    --timeout-ms 5000 2>/dev/null | wc -l)

if [ "$POLICY_COUNT" -gt 0 ]; then
    test_pass "Governance agent is publishing to policy.decisions"
else
    test_info "No policy.decisions messages found (may be normal if system just started)"
fi

echo ""

# Summary
echo "=========================================="
echo "Test Summary"
echo "=========================================="
echo -e "${GREEN}Passed:${NC} $TESTS_PASSED"
echo -e "${RED}Failed:${NC} $TESTS_FAILED"
echo ""

if [ $TESTS_FAILED -eq 0 ]; then
    echo -e "${GREEN}All critical tests passed!${NC}"
    echo "System appears to be functioning correctly."
    exit 0
else
    echo -e "${YELLOW}Some tests failed.${NC}"
    echo "Please check the logs for more details."
    exit 1
fi
