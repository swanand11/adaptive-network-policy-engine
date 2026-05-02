# Session Context — Adaptive Network Policy Engine

> Last updated: 2026-05-03
> Purpose: Preserve complete working context across sessions.

---

## 1. Project Vision

A **distributed, event-driven, multi-agent traffic orchestration system** — NOT a simple pipeline.
It is a control system designed for stability, reliability, and adaptive traffic distribution
across multiple cloud service providers (CSPs).

### Core Design Principles

| Principle | Description |
|-----------|-------------|
| No global state | Global behaviour emerges from distributed signals |
| Absolute desired state | Use target values, NOT delta changes |
| Traffic conservation | `sum(distribution) = 1` always |
| Stateless decisions | No drift accumulation |
| EWMA + Sliding window | Stability under noise |
| Deterministic | Same inputs → same output |
| AP system | Tolerates Kafka lag, partitioning, partial failure |

---

## 2. Full Data Flow

```
metrics.events (Kafka)
        ↓
Service Agent  [per service, local state estimator]
        ↓
service.state (Kafka)
        ↓
Topography Agent  [global reconciliation + candidate generation]  ← NOT YET IMPLEMENTED
        ↓
policy.candidates (Kafka)                                         ← NOT YET CREATED
        ↓
Governance Agent  [risk validation + final decision]              ← NOT YET IMPLEMENTED
        ↓
policy.decisions (Kafka)
        ↓
Execution Layer  [NGINX / Envoy dynamic weights]                  ← NOT YET IMPLEMENTED
        ↓
Traffic redistribution
        ↓
system.audit.log (Kafka)
```

---

## 3. Repository Structure

```
adaptive-network-policy-engine/
├── agents/
│   ├── agent_registry.py          # Thread-safe registry of ServiceAgent instances
│   ├── partition_worker.py        # One worker thread per Kafka partition
│   └── service_agent/
│       ├── service_agent.py       # ✅ FULLY IMPLEMENTED (local state estimator)
│       └── README.md
│
├── kafka_core/
│   ├── config.py                  # KafkaConfig, TopicConfig — all topic definitions
│   ├── schemas.py                 # Pydantic event models for all topics
│   ├── enums.py                   # CloudProvider, PolicyStatus, ExecutionStatus, RiskLevel
│   ├── consumer_base.py           # KafkaConsumerTemplate base class
│   ├── producer_base.py           # KafkaProducerTemplate base class
│   ├── producer_registry.py       # CloudProducerRegistry (one producer per cloud)
│   ├── dispatcher.py              # Non-blocking message dispatcher to partition queues
│   ├── partition_queue_manager.py # Per-partition queue management
│   ├── topic_initializer.py       # Auto-creates all Kafka topics on startup
│   ├── prometheus_kafka_adaptar.py # Polls Prometheus → normalizes → publishes to metrics.events
│   ├── prometheus_adapter_config.py # Endpoint/interval config for the adapter
│   ├── utils.py                   # Key parsing utilities (extract_key_parts)
│   └── exceptions.py              # Custom exception classes
│
├── runners/
│   ├── service_agent_runner.py    # ✅ ParallelServiceAgentRunner — main entrypoint
│   └── prometheus_adapter_runner.py
│
├── mocks/
│   ├── base_simulator.py          # Shared Flask + Prometheus metrics server
│   ├── aws_simulator.py           # AWS EC2/ECS mock (port 8001)
│   ├── aks_simulator.py           # Azure AKS mock (port 8002)
│   └── dodroplets_simulator.py    # DigitalOcean Droplets mock (port 8003)
│
├── config/
│   ├── agents.yml                 # Agent config (service_id, cloud, thresholds, alpha)
│   └── agent_config_loader.py     # Loads agents.yml, supports ENV override
│
├── docker-compose.yml             # Full stack: Kafka, Zookeeper, mocks, adapter, agents
├── prometheus.yml                 # Prometheus scrape config for all mock services
├── Dockerfile                     # App image (adapter + runner)
├── requirements.txt
└── context.md                     # ← this file
```

---

## 4. Kafka Topics

| Topic | Partitions | Key | Producer | Consumer |
|-------|-----------|-----|----------|---------|
| `metrics.events` | 3 | `service_id@cloud` | Prometheus adapter | Service Agent |
| `service.state` | 2 | `service_id@cloud` (UUID key) | Service Agent | Topography Agent (TODO) |
| `policy.candidates` | — | — | Topography Agent (TODO) | Governance Agent (TODO) |
| `policy.decisions` | 2 | `decision_id` | Governance Agent (TODO) | Execution Layer (TODO) |
| `policy.executions` | 2 | `execution_id` | Execution Layer (TODO) | Audit consumer (TODO) |
| `system.audit.log` | 1 | `event_id` | Any actor | Monitoring (TODO) |

> `policy.candidates` topic is not yet registered in `kafka_core/config.py`.

---

## 5. Service Agent — FULLY IMPLEMENTED ✅

### File: `agents/service_agent/service_agent.py`

**Consumes:** `metrics.events`  
**Produces:** `service.state`

### Input shape (from Prometheus adapter → `metrics.events`)

```json
{
  "service": "service-cache-aws",
  "cloud": "aws",
  "timestamp": "2026-05-03T...",
  "metrics": {
    "request_latency_ms": 145.3,
    "cpu_usage_percent": 35.0,
    "memory_usage_percent": 52.0,
    "error_rate_percent": 0.5,
    "request_count": 420
  },
  "correlation_id": "prometheus-aws-1234567890.0"
}
```

> Adapter normalizes raw simulator fields to `*_ms`, `*_percent` naming.
> Service agent also accepts raw simulator names as fallbacks:
> - `cpu` → fallback for `cpu_percent`
> - `requests_per_sec` → fallback for `throughput`
> - `memory` → fallback for `memory_percent`

### Internal calculations

| Calculation | Logic | Formula |
|-------------|-------|---------|
| EWMA latency | Exponential smoothing | `alpha * current + (1-alpha) * prev_ewma`, alpha=0.3 |
| Trend | Delta of last two latency samples | `window[-1] - window[-2]` |
| Confidence | Variance-based | `1.0 - latency_spread/100 - error_spread/0.1`, clamped to [0.5, 0.95] |
| Status | Threshold rules | `overloaded / stressed / healthy` |
| Current load | Pressure penalty | `1.0 - penalty`, clamped [0.05, 0.95] |
| Optimal load | EWMA-derived target | `1.0 - (ewma/threshold)/2`, clamped [0.05, 0.95] |

### Status thresholds (defaults)

| Status | Latency | Error Rate | CPU |
|--------|---------|------------|-----|
| overloaded | > 200ms | ≥ 5% (0.05) | ≥ 80% (0.8) |
| stressed | > 160ms | ≥ 3% (0.03) | ≥ 60% (0.6) |
| healthy | otherwise | — | — |

### Output shape → `service.state`

```json
{
  "key": "<uuid>",
  "value": {
    "service": "service-cache-aws",
    "cloud": "aws",
    "timestamp": "2026-05-03T...",
    "belief": {
      "latency_ewma": 143.20,
      "trend": "+2.10ms",
      "confidence": 0.87,
      "status": "healthy"
    },
    "intent": {
      "current_load": 0.72,
      "optimal_load": 0.64
    },
    "metadata": {
      "source_event_key": "service-cache-aws@aws",
      "metrics": {...},
      "throughput": 320,
      "memory_percent": 52.0,
      "normalized_cpu": 0.35,
      "normalized_error_rate": 0.005
    },
    "correlation_id": "prometheus-aws-...",
    "parent_event_id": null
  }
}
```

> **NOTE:** The master prompt uses `desired_load` in intent. Current implementation uses `optimal_load`.
> These are semantically equivalent. A rename may be done when the Topography Agent is implemented.

---

## 6. Runner Architecture — FULLY IMPLEMENTED ✅

### File: `runners/service_agent_runner.py` → `ParallelServiceAgentRunner`

**Design:**
- Main thread: polls `metrics.events` consumer, dispatches to partition queues
- Worker threads: one per assigned Kafka partition
- Each worker: `PartitionWorker` → pulls from queue → routes to `ServiceAgent` → commits offset after success
- Manual offset commit (no auto-commit) → exactly-once processing guarantee

**Key components:**

| Component | File | Role |
|-----------|------|------|
| `ParallelServiceAgentRunner` | `runners/service_agent_runner.py` | Orchestrates consumer + workers |
| `PartitionWorker` | `agents/partition_worker.py` | One thread per partition; processes messages |
| `AgentRegistry` | `agents/agent_registry.py` | Thread-safe lazy registry of `ServiceAgent` instances by `(service_id, cloud)` |
| `CloudProducerRegistry` | `kafka_core/producer_registry.py` | One Kafka producer per cloud (shared by agents) |
| `Dispatcher` | `kafka_core/dispatcher.py` | Non-blocking dispatch to partition queues; drops on full |
| `PartitionQueueManager` | `kafka_core/partition_queue_manager.py` | Creates/manages per-partition queues |
| `AgentConfigLoader` | `config/agent_config_loader.py` | Loads `agents.yml`, supports `AGENTS_CONFIG_PATH` ENV |

**Partition key format:**  
`service_id@cloud` (e.g. `service-cache-aws@aws`)  
This ensures same service always routes to same partition → ordering guarantee.

---

## 7. Mock Services

All mocks expose Prometheus metrics at `http://localhost:<port>/metrics`.

| Service | Port | Cloud | service_id |
|---------|------|-------|------------|
| `aws_simulator.py` | 8001 | aws | service-cache-aws |
| `aks_simulator.py` | 8002 | azure | service-db |
| `dodroplets_simulator.py` | 8003 | digitalocean | service-cache |

**Raw metric keys from simulators:**

```python
{
    "latency_ms": float,       # ms, e.g. 45.0
    "cpu": float,              # 0-100 (percent scale)
    "memory_percent": float,   # 0-100
    "error_rate": float,       # 0-10 (percent scale)
    "requests_per_sec": int,   # e.g. 320
    "region": str,             # e.g. "us-east-1"
}
```

**After Prometheus adapter normalization** (`MetricsNormalizer`):

```python
{
    "request_latency_ms": float,    # derived: sum/count * 1000
    "cpu_usage_percent": float,     # same as cpu
    "memory_usage_percent": float,  # same as memory_percent
    "error_rate_percent": float,    # same as error_rate
    "request_count": int,           # request_count_total
}
```

**Base simulator** (`mocks/base_simulator.py`):
- Flask + prometheus_client
- Calls `generate_metrics()` every 2 seconds to update Prometheus gauges
- Exposes `/metrics` endpoint

---

## 8. Prometheus Adapter

### File: `kafka_core/prometheus_kafka_adaptar.py`

**Flow:**
1. Polls each mock endpoint every 5 seconds (`PROMETHEUS_POLL_INTERVAL`)
2. Parses Prometheus text format via regex
3. Normalizes to `MetricsEventValue` schema
4. Sends to `metrics.events` with:
   - `key = service_id` (e.g. `service-cache-aws`)
   - explicit partition assignment (0 = aws, 1 = db, 2 = cache)

**Config file:** `kafka_core/prometheus_adapter_config.py`  
**ENV overrides:** `PROMETHEUS_AWS_URL`, `PROMETHEUS_AKS_URL`, `PROMETHEUS_DO_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `PROMETHEUS_POLL_INTERVAL`

---

## 9. Schemas (Pydantic)

All schemas are defined in `kafka_core/schemas.py`.

| Model | Topic | Direction |
|-------|-------|-----------|
| `MetricsEventValue` | `metrics.events` | Adapter → Agent |
| `MetricsEvent` | `metrics.events` | (deprecated wrapped form) |
| `ServiceStateBelief` | `service.state` | Agent output |
| `ServiceStateIntent` | `service.state` | Agent output |
| `ServiceStateValue` | `service.state` | Agent output |
| `ServiceState` | `service.state` | Agent output (with key) |
| `AuditLogEventValue` | `system.audit.log` | Any actor |
| `AuditLogEvent` | `system.audit.log` | Any actor |
| `PolicyDecisionValue` | `policy.decisions` | Governance Agent (TODO) |
| `PolicyDecision` | `policy.decisions` | Governance Agent (TODO) |
| `PolicyExecutionValue` | `policy.executions` | Execution Layer (TODO) |
| `PolicyExecution` | `policy.executions` | Execution Layer (TODO) |

> **Missing schemas (TODO):** `PolicyCandidateValue`, `PolicyCandidate` for `policy.candidates` topic.

---

## 10. Docker Compose Services

| Service | Image | Port | Role |
|---------|-------|------|------|
| `zookeeper` | confluentinc/cp-zookeeper:7.5.0 | 2181 | Kafka coordination |
| `kafka` | confluentinc/cp-kafka:7.5.0 | 9092 | Message broker |
| `redis` | redis:7 | 6379 | (reserved for future use) |
| `mongo` | mongo:7 | 27017 | (reserved for future use) |
| `aws-simulator` | mocks/Dockerfile | 8001 | AWS metrics mock |
| `aks-simulator` | mocks/Dockerfile | 8002 | Azure AKS metrics mock |
| `digitalocean-simulator` | mocks/Dockerfile | 8003 | DigitalOcean metrics mock |
| `prometheus` | prom/prometheus | 9090 | Scrapes mock endpoints |
| `prometheus-adapter` | Dockerfile | — | Polls Prometheus → Kafka |
| `service-agent` | Dockerfile | — | service-cache-aws@aws |
| `service-agent-2` | Dockerfile | — | service-db@azure |
| `service-agent-3` | Dockerfile | — | service-cache@digitalocean |

**Note:** `kafka` advertises on `kafka:9092` (internal Docker network only).  
External connections (host machine testing) require `localhost:9092` mapped port.

---

## 11. Testing Commands

### Start full stack
```bash
docker compose up -d
```

### Watch service agent output
```bash
docker compose logs -f service-agent
```

### Consume metrics.events (raw input)
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic metrics.events \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 30000
```

### Consume service.state (processed output)
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic service.state \
  --from-beginning \
  --max-messages 10 \
  --timeout-ms 30000
```

### Side-by-side comparison (two terminals)
**Terminal 1 — raw metrics:**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic metrics.events --from-beginning --max-messages 10 \
  --timeout-ms 30000
```
**Terminal 2 — processed state:**
```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic service.state --from-beginning --max-messages 10 \
  --timeout-ms 30000
```

### Syntax validation
```bash
python -m py_compile kafka_core/config.py kafka_core/schemas.py \
  agents/service_agent/service_agent.py runners/service_agent_runner.py
```

---

## 12. Current Implementation Status

| Layer | Component | Status |
|-------|-----------|--------|
| Data Sources | AWS / AKS / DigitalOcean simulators | ✅ Done |
| Ingestion | Prometheus adapter → `metrics.events` | ✅ Done |
| Layer 1 | Service Agent → `service.state` | ✅ Done |
| Runtime | ParallelServiceAgentRunner (partitioned workers) | ✅ Done |
| Registry | AgentRegistry + CloudProducerRegistry | ✅ Done |
| Layer 2 | Topography Agent → `policy.candidates` | ❌ TODO |
| Layer 3 | Governance Agent → `policy.decisions` | ❌ TODO |
| Execution | Load balancer weight updater | ❌ TODO |
| Schemas | `PolicyCandidateValue` / `PolicyCandidate` | ❌ TODO |
| Topics | `policy.candidates` in `KafkaConfig.TOPICS` | ❌ TODO |
| Audit | Structured audit log producer | ❌ TODO |

---

## 13. Next Steps (Ordered)

### Step 1 — Add `policy.candidates` topic
- Register in `kafka_core/config.py` → `KafkaConfig.TOPICS`
- Add `PolicyCandidateValue` + `PolicyCandidate` Pydantic models in `kafka_core/schemas.py`

### Step 2 — Implement Topography Agent
**File:** `agents/topography_agent/topography_agent.py`

Key behaviours:
- Consume `service.state` from ALL services
- Build a temporary in-memory snapshot (short window only, no persistent global state)
- Reconcile `optimal_load` values: normalize so `sum = 1.0`
- Apply capacity penalty/boost based on latency and cpu
- Generate multiple candidate distributions (`+/- 5%` variations)
- Run lightweight what-if latency/risk estimation per candidate
- Publish list of `PolicyCandidate` events to `policy.candidates`

### Step 3 — Implement Governance Agent
**File:** `agents/governance_agent/governance_agent.py`

Key behaviours:
- Consume `policy.candidates`
- Apply hysteresis (compare to last applied distribution)
- Apply cooldown period (reject change if last decision is too recent)
- Evaluate risk score per candidate
- Select safest high-impact candidate
- Classify mode: `canary` (partial) or `full` (complete shift)
- Publish `PolicyDecision` to `policy.decisions`
- Write to `system.audit.log`

### Step 4 — Implement Execution Layer
**File:** `agents/execution_layer/executor.py`

Key behaviours:
- Consume `policy.decisions`
- Convert `target_distribution` to integer weights (sum = 100)
- Update NGINX/Envoy via dynamic config or API
- Must be idempotent
- Publish `PolicyExecution` to `policy.executions`

### Step 5 — Add `policy.candidates` runner
- Create `runners/topography_agent_runner.py`
- Create `runners/governance_agent_runner.py`
- Add new containers to `docker-compose.yml`

---

## 14. Key Known Issues / Notes

- **`desired_load` vs `optimal_load`:** Master prompt uses `desired_load` in `ServiceStateIntent`. Current schema and code use `optimal_load`. Rename when implementing Topography Agent to avoid downstream confusion.
- **`classify_status()` is not currently called** in `build_belief()`. The `status` field in `ServiceStateBelief` requires this — it is currently hardcoded to a default. Verify in `service_agent.py` and add the call if missing.
- **Kafka internal vs external address:** `kafka:9092` is the Docker-internal hostname. For local host testing without Docker, use `localhost:9092`.
- **`kafka-python` package:** Must be installed (`pip install kafka-python`). Not installed by default in some environments.
- **No `policy.candidates` topic:** The topic is referenced in design but not yet registered in `KafkaConfig.TOPICS` or in `topic_initializer.py`.
- **Metadata field in `ServiceStateIntent`:** Currently named `optimal_load`, not `desired_load`. The Topography Agent should consume both field names for compatibility.

---

## 15. Session History

| Date | Summary |
|------|---------|
| 2026-04-29 | Created context.md and session_context.py helper |
| 2026-04-29 | Updated service_agent_runner.py to read `SERVICE_ID` from ENV |
| 2026-04-29 | docker-compose.yml updated with `SERVICE_ID: service-cache-aws` |
| 2026-05-01 | EWMA & Load Calculation testing guide added |
| 2026-05-01 | Refactored ServiceAgent to produce `service.state` instead of `policy.decisions` |
| 2026-05-01 | Added `ServiceStateBelief`, `ServiceStateIntent`, `ServiceStateValue` schemas |
| 2026-05-01 | Added `service.state` topic to `KafkaConfig.TOPICS` |
| 2026-05-01 | Fixed metric key mapping: `cpu` → `cpu_percent`, `requests_per_sec` → `throughput` |
| 2026-05-01 | Added `_normalize_percentage` helper; percent metrics normalized to [0,1] |
| 2026-05-01 | Migrated ServiceAgent to `agents/service_agent/` directory |
| 2026-05-01 | Created `AgentRegistry`, `CloudProducerRegistry`, `PartitionWorker` |
| 2026-05-01 | Created `ParallelServiceAgentRunner` with per-partition worker threads |
| 2026-05-01 | Installed `kafka-python==2.3.1` |
| 2026-05-03 | Full context.md rewrite with complete system documentation |
