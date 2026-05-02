# Adaptive Network Policy Engine - Session Context Prompt

## Project Overview

**Name:** Adaptive Network Policy Engine  
**Purpose:** A Kafka-based multi-agent system that monitors cloud service metrics, computes health beliefs, derives optimal load intents, and coordinates adaptive network policies across AWS, Azure, and DigitalOcean cloud providers.

**Repository Location:** `d:\Sandesh\mini project\adaptive-network-policy-engine`  
**Current Branch:** `sk` (latest development work)  
**Last Commit:** `f611990` - "Reorganize project structure and consolidate agents/runners/tests"

---

## System Architecture

### High-Level Data Flow

```
Cloud Simulators (Prometheus Endpoints)
    ↓
Prometheus Scraper
    ↓
Prometheus Kafka Adapter (prometheus_adapter_runner.py)
    ↓
Kafka Topic: metrics.events (3 partitions)
    ↓
Service Agent (service_agent_runner.py) [×3 instances]
    ↓
Kafka Topic: service.state (2 partitions)
    ↓
[Future] Policy Coordination Agents
```

### Core Components

#### 1. **Kafka Infrastructure** (`kafka_core/`)
   - `producer_base.py`: Generic Kafka producer template with partition routing support
   - `consumer_base.py`: Generic Kafka consumer template with configurable topics
   - `topic_initializer.py`: Automatic topic creation and partition management
   - `schemas.py`: Pydantic models for all event types (MetricsEventValue, ServiceState, etc.)
   - `config.py`: Centralized Kafka configuration
   - `prometheus_kafka_adaptar.py`: Polls Prometheus, normalizes metrics, sends to Kafka with partition routing
   - `enums.py`: CloudProvider enum (aws, azure, digitalocean)

#### 2. **Agents** (`agents/service_agent/`)
   - `service_agent.py`: Core ServiceAgent class (200+ lines)
     * Extends KafkaConsumerTemplate
     * Processes metrics.events by service_id filter
     * Computes EWMA latency, trend, confidence, status
     * Builds belief (latency_ewma, trend, confidence, status)
     * Builds intent (current_load, optimal_load)
     * Publishes ServiceState to service.state topic
   - `README.md`: Comprehensive 300+ line documentation

#### 3. **Runners** (`runners/`)
   - `prometheus_adapter_runner.py`: Entrypoint for Prometheus metrics collection
   - `service_agent_runner.py`: Entrypoint for ServiceAgent process (reads SERVICE_ID from env)

#### 4. **Tests** (`tests/`)
   - `e2e_pipeline_test.py`: 4-stage end-to-end validation pipeline (ALL PASSING)
     * Stage 1: Kafka Readiness - Broker connectivity, topic verification
     * Stage 2: Send Events - Send 3 test events to metrics.events
     * Stage 3: Partition Routing - Verify events route to correct partitions by service name
     * Stage 4: Service State Publishing - Verify agents publish to service.state

#### 5. **Cloud Simulators** (`mocks/`)
   - `aws_simulator.py`: AWS service simulator with Prometheus metrics endpoint
   - `aks_simulator.py`: Azure Kubernetes Service simulator
   - `dodroplets_simulator.py`: DigitalOcean Droplets simulator
   - `base_simulator.py`: Base class for all simulators
   - Each simulator exposes Prometheus metrics on port 8001-8003

#### 6. **Docker Orchestration** (`docker-compose.yml`)
   - 10 services total:
     * `zookeeper`: Kafka coordination
     * `kafka`: Message broker (cp-kafka 2.5.0)
     * `prometheus`: Metrics scraper
     * `prometheus-adapter`: Kafka publishing adapter
     * `aws-simulator`, `aks-simulator`, `dodroplets-simulator`: Cloud mocks
     * `redis`, `mongo`: Data stores
     * `service-agent`: ServiceAgent instance (SERVICE_ID=service-cache-aws)

---

## Completed Work (Commit f611990)

### 1. Partition Routing Strategy
- **Implementation:** Explicit hardcoded partition mapping
- **Mapping:**
  - Partition 0 = Azure (aks)
  - Partition 1 = AWS
  - Partition 2 = DigitalOcean
- **Code Location:** `kafka_core/prometheus_kafka_adaptar.py` (ENDPOINTS config with partition field)
- **Producer Support:** `kafka_core/producer_base.py` - `send()` method accepts optional partition parameter

### 2. ServiceAgent Implementation
- **Location:** `agents/service_agent/service_agent.py`
- **Key Features:**
  - EWMA latency smoothing (alpha=0.3 default)
  - Sliding window trend analysis (window_size=5)
  - Confidence scoring based on metric spread
  - 3-state health classification: healthy, stressed, overloaded
  - Belief: {latency_ewma, trend, confidence, status}
  - Intent: {current_load, optimal_load}
- **Deployment:** 3 instances via docker-compose (one per cloud provider)
- **Environment Variable:** SERVICE_ID (required, e.g., "service-cache-aws")

### 3. Kafka Topic Configuration
- **metrics.events:** 3 partitions (hardcoded requirement)
- **service.state:** 2 partitions
- **system.audit.log:** 1 partition
- **policy.decisions:** 2 partitions
- **policy.executions:** 2 partitions
- **Implementation:** `kafka_core/topic_initializer.py` with partition validation

### 4. Project Reorganization
- **agents/** subdirectory structure
  ```
  agents/
  ├── __init__.py (exports ServiceAgent)
  └── service_agent/
      ├── __init__.py
      ├── service_agent.py
      └── README.md
  ```
- **runners/** created for entrypoint scripts
- **tests/** created with e2e validation

### 5. Docker Integration
- All services configured in `docker-compose.yml`
- Network isolation with internal Kafka broker (kafka:9092)
- Service agent runs with SERVICE_ID environment variable
- Prometheus adapter polls simulators and publishes to Kafka

### 6. End-to-End Test Suite
- **Location:** `tests/e2e/e2e_pipeline_test.py`
- **All 4 Stages PASSING:**
  - Kafka broker reachable, topics exist with correct partitions
  - Test events successfully sent to metrics.events
  - Partition routing verified (events in correct partitions)
  - ServiceAgent instances publish service.state messages
- **Execution:** `python tests/e2e/e2e_pipeline_test.py`
- **Docker Wrapper:** Test runs inside container with kafka network access

---

## How to Test the System

### Setup

1. **Install Dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

2. **Start Docker Services:**
   ```powershell
   cd "d:\Sandesh\mini project\adaptive-network-policy-engine"
   docker-compose up -d
   ```
   This starts: zookeeper, kafka, prometheus, prometheus-adapter, simulators, redis, mongo, and all service agents.

3. **Wait for Services:**
   ```powershell
   # Check broker readiness
   docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

### Test Scenarios

#### Scenario 1: Verify E2E Pipeline (FASTEST)
```powershell
python tests/e2e/e2e_pipeline_test.py
```
- Expected: All 4 stages pass
- Output: JSON and HTML reports in `tests/reports/`
- Duration: ~4-5 seconds

#### Scenario 2: Check Partition Routing
```powershell
# See messages in metrics.events
docker run -it --rm --network container:kafka_1 confluentinc/cp-kafka:latest `
  kafka-console-consumer --bootstrap-server localhost:9092 `
  --topic metrics.events --from-beginning --max-messages 3

# Verify partition distribution
docker run -it --rm --network container:kafka_1 confluentinc/cp-kafka:latest `
  kafka-topics --bootstrap-server localhost:9092 --describe --topic metrics.events
```
- Expected: 3 partitions, one message per partition (aws, azure, do)

#### Scenario 3: Monitor Service Agent Output
```powershell
# Watch service agent logs in real-time
docker logs -f service-agent | Select-String "Published|Skipping|ERROR"

# Or from separate terminal, send a test event
python tests/kafka/send_test_event.py
```
- Expected: Service agent processes metrics and publishes service.state

#### Scenario 4: Inspect service.state Topic
```powershell
docker run -it --rm --network container:kafka_1 confluentinc/cp-kafka:latest `
  kafka-console-consumer --bootstrap-server localhost:9092 `
  --topic service.state --from-beginning --max-messages 3 --value-deserializer org.apache.kafka.common.serialization.StringDeserializer
```
- Expected: JSON messages with belief (latency_ewma, confidence, status) and intent (load values)

#### Scenario 5: Test Prometheus Adapter
```powershell
docker logs prometheus-adapter | Select-String "Polling|Sent metrics|ERROR" | Select-Object -Last 20
```
- Expected: Periodic polling output showing metrics collection from simulators

#### Scenario 6: Single Service Agent Standalone
```powershell
$env:SERVICE_ID = "service-cache-aws"
$env:KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
python runners/service_agent_runner.py
```
- Expected: Agent starts, consumes from metrics.events, publishes to service.state

---

## Technical Details

### Kafka Configuration
- **Broker:** Confluent cp-kafka 2.5.0
- **Client:** kafka-python 2.0.2
- **Bootstrap Servers (Docker):** kafka:9092
- **Bootstrap Servers (Host):** localhost:9092

### Python Dependencies
```
flask==2.3.0
prometheus-client==0.17.0
kafka-python==2.0.2
six>=1.16.0
python-dotenv==1.0.0
requests>=2.28.0
pydantic>=1.10.0
```

### ServiceAgent Parameters
```python
ServiceAgent(
    service_id="service-cache-aws",          # Required
    alpha=0.3,                                # EWMA smoothing factor
    latency_threshold=200.0,                  # ms threshold for health
    error_threshold=0.05,                     # 5% error rate threshold
    cpu_threshold=0.8,                        # 80% CPU threshold
    window_size=5,                            # Sliding window for trend
    group_id="service_agent"                  # Kafka consumer group
)
```

### EWMA Formula
```
EWMA_t = alpha * current_latency + (1 - alpha) * EWMA_{t-1}
```
- With alpha=0.3: new data weighted 30%, historical 70%
- Balances responsiveness with noise smoothing

### Load Estimation Formula
```
penalty = 0.0
if latency > threshold:
    penalty += min((latency - threshold) / (2 * threshold), 0.5)
else:
    penalty += max((latency - 0.5 * threshold) / threshold, 0.0) * -0.1

penalty += min(error_rate * 2.0, 0.5)
penalty += max((cpu - 0.5) * 0.4, 0.0)

current_load = 1.0 - penalty
optimal_load = max(min(1.0 - r/2.0, 0.95), 0.05)  # where r = ewma / latency_threshold
```

### Event Schemas

**MetricsEventValue (Input to ServiceAgent):**
```json
{
  "service": "service-cache-aws",
  "cloud": "aws",
  "timestamp": "2026-05-01T10:30:00",
  "metrics": {
    "request_latency_ms": 45.08,
    "request_count": 147,
    "error_rate_percent": 0.78,
    "cpu_usage_percent": 32.19,
    "memory_usage_percent": 59.0
  },
  "correlation_id": "prometheus-aws-...",
  "parent_event_id": null
}
```

**ServiceStateValue (Output from ServiceAgent):**
```json
{
  "service": "service-cache-aws",
  "cloud": "aws",
  "timestamp": "2026-05-01T10:30:02",
  "belief": {
    "latency_ewma": 48.5,
    "trend": "+3.42ms",
    "confidence": 0.85,
    "status": "healthy"
  },
  "intent": {
    "current_load": 0.72,
    "optimal_load": 0.76
  },
  "metadata": {...},
  "correlation_id": "prometheus-aws-...",
  "parent_event_id": null
}
```

---

## Known Issues & Gotchas

### Issue 1: kafka-python 2.0.2 six.moves Import
- **Symptom:** ImportError on six.moves
- **Cause:** kafka-python vendors an outdated six module
- **Solution:** Inline compatibility shim in test files (already applied)

### Issue 2: Docker Network vs Host Network
- **Symptom:** kafka:9092 unreachable from host
- **Cause:** Different network namespaces
- **Solution:** Tests run inside kafka container network using Docker wrapper

### Issue 3: Multiple Agents Same Consumer Group
- **Symptom:** Partitions distributed across agents, some miss events
- **Cause:** Kafka rebalancing with shared consumer group
- **Solution:** Use unique group_id per agent or use single-agent deployment

### Issue 4: Topic Not Found (TopicAlreadyExistsError)
- **Symptom:** Noisy logs but system still works
- **Cause:** Topics created on first initialization
- **Solution:** Idempotent - safe to ignore on subsequent runs

---

## File Structure Summary

```
adaptive-network-policy-engine/
├── agents/
│   ├── __init__.py
│   └── service_agent/
│       ├── __init__.py
│       ├── service_agent.py (200+ lines, core logic)
│       └── README.md (300+ lines, comprehensive docs)
├── runners/
│   ├── __init__.py
│   ├── prometheus_adapter_runner.py
│   └── service_agent_runner.py
├── tests/
│   ├── __init__.py
│   ├── e2e/
│   │   ├── __init__.py
│   │   └── e2e_pipeline_test.py (4-stage validation)
│   ├── kafka/
│   │   ├── __init__.py
│   │   └── send_test_event.py
│   └── reports/ (generated JSON/HTML test reports)
├── kafka_core/
│   ├── __init__.py
│   ├── config.py
│   ├── producer_base.py (with partition routing)
│   ├── consumer_base.py
│   ├── topic_initializer.py (enforces 3 partitions)
│   ├── schemas.py (Pydantic models)
│   ├── enums.py (CloudProvider)
│   ├── prometheus_kafka_adaptar.py (partition routing config)
│   └── exceptions.py
├── mocks/
│   ├── __init__.py
│   ├── base_simulator.py
│   ├── aws_simulator.py
│   ├── aks_simulator.py
│   └── dodroplets_simulator.py
├── conversations/ (local, not committed)
├── docker-compose.yml (10 services)
├── Dockerfile (adapter)
├── requirements.txt
├── config.py (root config)
├── context.md (session notes)
├── session_context.py (helper for updating context)
└── README.md (project README)
```

---

## Testing Checklist

Use this to systematically test the system:

- [ ] **Setup:** Docker compose up, all services running
- [ ] **Stage 1:** E2E test stage 1 passes (Kafka ready, topics exist)
- [ ] **Stage 2:** E2E test stage 2 passes (3 events sent)
- [ ] **Stage 3:** E2E test stage 3 passes (partition routing verified)
- [ ] **Stage 4:** E2E test stage 4 passes (service.state published)
- [ ] **Manual Check:** Kafka topics exist with correct partition count
- [ ] **Manual Check:** Messages appear in metrics.events with correct partitions
- [ ] **Manual Check:** Service agent logs show processing and publishing
- [ ] **Manual Check:** Messages appear in service.state with belief/intent
- [ ] **Prometheus:** Adapter logs show periodic polling and message sending
- [ ] **Standalone:** Service agent can run standalone with SERVICE_ID env var

---

## Git Information

**Current Branch:** `sk`  
**Latest Commit:** `f611990` - "Reorganize project structure and consolidate agents/runners/tests"

**Commit includes:**
- 22 files changed
- 1555 insertions(+), 276 deletions
- Agents subdirectory reorganization
- Runners directory creation
- Tests directory creation
- Updated .gitignore with local patterns

**Local files (NOT committed):**
- `reports/` - Generated test output
- `conversations/` - Local conversation artifacts
- `.venv/` - Virtual environment
- `__pycache__/` - Python cache

---

## Next Steps / Future Work

1. **Policy Coordination Agent:** Consume service.state, decide on network policies
2. **Multi-Agent Orchestration:** Coordinate policies across multiple services
3. **Persistence Layer:** Store metrics and policies in mongo/redis
4. **Advanced ML:** Replace rule-based health with learned models
5. **Observability Dashboard:** Real-time metrics visualization
6. **Multi-Region Support:** Policy coordination across geographic regions

---

## Key Contacts / References

- **ServiceAgent Documentation:** `agents/service_agent/README.md`
- **E2E Test Reports:** `tests/reports/` (JSON and HTML)
- **Docker Configuration:** `docker-compose.yml`
- **Kafka Schemas:** `kafka_core/schemas.py`
- **Session Context:** `context.md`

---

## Quick Commands Reference

```powershell
# Start system
docker-compose up -d

# Run full e2e test
python tests/e2e/e2e_pipeline_test.py

# Watch service agent logs
docker logs -f service-agent

# Check metrics.events topic
docker run -it --rm --network container:kafka_1 confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:9092 --topic metrics.events --from-beginning --max-messages 3

# Check service.state topic
docker run -it --rm --network container:kafka_1 confluentinc/cp-kafka:latest kafka-console-consumer --bootstrap-server localhost:9092 --topic service.state --from-beginning --max-messages 3

# Describe metrics.events partitions
docker run -it --rm --network container:kafka_1 confluentinc/cp-kafka:latest kafka-topics --bootstrap-server localhost:9092 --describe --topic metrics.events

# Stop all services
docker-compose down

# View git log
git log --oneline -10
```

---

**Last Updated:** 2026-05-01  
**Status:** Production Ready (E2E validated, all 4 stages passing)
