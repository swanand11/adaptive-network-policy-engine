#!/bin/bash

# Configuration
KAFKA_CONTAINER="kafka_1"
BROKER="localhost:9092"

echo "Running Pipeline Validation..."
echo "------------------------------"

# 1. Kafka topics exist
echo -n "Checking Kafka topics... "
TOPICS=$(docker exec $KAFKA_CONTAINER kafka-topics --list --bootstrap-server localhost:9092 2>/dev/null)
if echo "$TOPICS" | grep -q "topo.decisions" && echo "$TOPICS" | grep -q "policy.decisions"; then
    echo "[PASS]"
else
    echo "[FAIL] Topics missing"
fi

# 2. CSP mocks respond
echo -n "Checking CSP mocks... "
if curl -s -f http://localhost:8001/metrics >/dev/null && \
   curl -s -f http://localhost:8002/metrics >/dev/null && \
   curl -s -f http://localhost:8003/metrics >/dev/null; then
    echo "[PASS]"
else
    echo "[FAIL] Mock(s) did not respond"
fi

# 3. Exec layer /health is 200
echo -n "Checking Exec layer health... "
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9000/health)
if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "[PASS]"
else
    echo "[FAIL] Status was $HTTP_STATUS"
fi

# 4. Prometheus is up
echo -n "Checking Prometheus... "
if curl -s -f http://localhost:9090/-/healthy >/dev/null; then
    echo "[PASS]"
else
    echo "[FAIL]"
fi

# 5. Governance stub is producing
echo -n "Checking Governance stub (consuming 1 msg from policy.decisions)... "
TIMEOUT=5
# timeout needs to be run inside the container if we exec into it, or wrap docker exec
MSG=$(timeout $TIMEOUT docker exec $KAFKA_CONTAINER kafka-console-consumer --bootstrap-server localhost:9092 --topic policy.decisions --max-messages 1 2>/dev/null)
if [ -n "$MSG" ]; then
    echo "[PASS]"
else
    echo "[FAIL] No message received within ${TIMEOUT}s"
fi

echo "------------------------------"
echo "Pipeline Validation Completed."
exit 0
