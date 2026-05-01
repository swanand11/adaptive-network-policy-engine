# Session Context

This file records the current workspace session context so that work is preserved across commits and closed sessions.

## Purpose

- Capture high-level goals, changes, and decisions made during the current session.
- Preserve context in the repository so it is not lost when the session ends.
- Provide a place for future automation to append session summaries.

## Update Process

Use `session_context.py` to append new session notes in a consistent format.

Example:

```bash
python session_context.py --title "Service Agent Implementation" --summary "Added ServiceAgent, runner, and Docker Compose service."
```

## Current Session Notes

- Created `kafka_core/agents/service_agent.py`
- Added `service_agent_runner.py`
- Added `service-agent` service to `docker-compose.yml`
- Updated `docker-compose.yml` to set `SERVICE_ID: "service-cache-aws"` for the service agent
- Verified `service_agent_runner.py` reads `SERVICE_ID` from the environment
- Attempted Docker Compose startup but host environment lacks the `docker` CLI

## History

- 2026-04-29: Added context file and session update helper.
- 2026-04-29: Updated session notes with service agent topic and Compose environment alignment.

---

## 2026-05-01: EWMA & Load Calculation Testing Guide

### Overview
Comprehensive guide for testing how the system processes continuous metrics and calculates EWMA (Exponential Weighted Moving Average), current load, and optimal load. The system is **already performing all calculations** - this guide helps verify them in action.

### Complete Data Flow

```
┌─────────────────────┐
│  Mock Services      │
│ (AWS, AKS, DO)      │  Generate metrics every 2 seconds
└──────────┬──────────┘
           │ :8001, :8002, :8003 /metrics endpoints
           ↓
┌─────────────────────┐
│   Prometheus        │  Scrapes metrics from endpoints
│  (Prometheus)       │  (default interval ~15s)
└──────────┬──────────┘
           │
           ↓
┌─────────────────────────────────────┐
│ Prometheus Adapter (Docker service) │  Polls Prometheus every 5 seconds
│                                     │  (PROMETHEUS_POLL_INTERVAL: "5")
└──────────┬──────────────────────────┘
           │
           ↓ SENDS TO KAFKA
┌──────────────────────────────┐
│  KAFKA TOPIC: metrics.events │  ← ALL messages stored here permanently
└──────────┬───────────────────┘     (retention: 7 days)
           │
           ├─→ Service Agent 1 consumes
           ├─→ Service Agent 2 consumes
           └─→ Service Agent 3 consumes
                ↓
          ┌─────────────────────────────┐
          │ KAFKA TOPIC: service.state  │  ← Agents' beliefs & intents
          │                             │    (retention: 7 days)
          └─────────────────────────────┘
```

### Message Flow Rate

| Component | Frequency | Messages/Minute |
|-----------|-----------|-----------------|
| Mock Services | Every 2 seconds | 30 per service (90 total) |
| Prometheus Scrape | ~15s interval | ~4 per service (12 total) |
| Prometheus Adapter | Every 5 seconds | 12 per service (36 total) |
| Service Agents | As they process | ~12 per service (36 total to service.state) |

### What Gets Calculated (ServiceAgent Implementation)

#### 1. EWMA Calculation
**Location:** `agents/service_agent/service_agent.py` lines 45-53

```python
ewma = 0.3 * current_latency + 0.7 * previous_ewma
```

- **Alpha = 0.3**: 30% weight on current value, 70% on historical average
- **Purpose**: Smooth out temporary spikes, capture real trends
- **Example**: If latencies are [50, 60, 55, 65, 58] → EWMA gradually rises then falls

#### 2. Confidence Score Calculation
**Location:** `agents/service_agent/service_agent.py` lines 70-77

```python
confidence = 1.0 
  - min(latency_spread / 100, 0.5)      # Penalize high variance
  - min(error_spread / 0.1, 0.3)        # Penalize unstable errors
# Range: [0.5, 0.95] (minimum 0.5, maximum 0.95)
```

- **High spread** (inconsistent metrics) → **Low confidence**
- **Stable metrics** → **High confidence**
- Used to indicate how trustworthy the state estimate is

#### 3. Current Load Estimation
**Location:** `agents/service_agent/service_agent.py` lines 105-124

```python
penalty = 0.0
if latency > 200ms:
    penalty += min((latency - 200) / 400, 0.5)      # Latency penalty
penalty += min(error_rate * 2.0, 0.5)               # Error penalty
penalty += max((cpu - 0.5) * 0.4, 0.0)              # CPU penalty

current_load = 1.0 - penalty  # Range: [0.05, 0.95]
```

- **High latency/errors/CPU** → **Low current_load** (less capacity)
- **Good metrics** → **High current_load** (more capacity)
- Represents current traffic-bearing capacity

#### 4. Optimal Load Calculation
**Location:** `agents/service_agent/service_agent.py` lines 151-159

```python
r = ewma_latency / latency_threshold  # Ratio to threshold
optimal_load = 1.0 - (r / 2.0)        # Range: [0.05, 0.95]
```

- **Derived from EWMA latency only** (not current metrics)
- **Purpose**: Where the system *should* be sending traffic
- **Logic**: If EWMA approaches threshold, reduce optimal load proportionally

#### 5. Service Health Status
**Location:** `agents/service_agent/service_agent.py` lines 79-103

```
OVERLOADED: if latency > 200ms OR error_rate >= 5% OR cpu >= 80%
STRESSED:   if latency > 160ms OR error_rate >= 3% OR cpu >= 60%
HEALTHY:    otherwise
```

### Test Execution Commands

#### **Test 1: Capture Raw Metrics**
```bash
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic metrics.events \
  --from-beginning \
  --max-messages 20 \
  --timeout-ms 60000 | jq '.metrics | {latency_ms, error_rate_percent, cpu_usage_percent}'
```

**What you'll see:**
- Raw latency values bouncing (e.g., 45ms, 68ms, 52ms, 71ms)
- Error rates changing (e.g., 0.5%, 0.7%, 0.4%)
- CPU usage fluctuating (e.g., 35%, 52%, 48%)

---

#### **Test 2: Capture Processed Service State**
```bash
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic service.state \
  --from-beginning \
  --max-messages 20 \
  --timeout-ms 60000 | jq '{service: .service, belief: .belief, intent: .intent}'
```

**What you'll see:**
```json
{
  "service": "service-cache",
  "belief": {
    "latency_ewma": 145.23,
    "trend": "+5.20ms",
    "confidence": 0.82,
    "status": "healthy"
  },
  "intent": {
    "current_load": 0.72,
    "optimal_load": 0.65
  }
}
```

---

#### **Test 3: View Service Agent Logs**
```bash
docker-compose logs -f service-agent --tail=50
```

**What you'll see:**
```
Published service state XXX for service-cache@digitalocean: optimal_load=0.65
belief={'latency_ewma': 145.23, 'trend': '+5.20ms', 'confidence': 0.82, 'status': 'healthy'}
intent={'current_load': 0.72, 'optimal_load': 0.65}
```

---

#### **Test 4: Side-by-Side Comparison**
Run in one terminal:
```bash
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic metrics.events --from-beginning --max-messages 10 --timeout-ms 30000 | \
  jq '{service: .service, latency_ms: .metrics.latency_ms, error_rate: .metrics.error_rate_percent}'
```

Run in another terminal:
```bash
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic service.state --from-beginning --max-messages 10 --timeout-ms 30000 | \
  jq '{service: .service, latency_ewma: .belief.latency_ewma, optimal_load: .intent.optimal_load}'
```

**Compare**: Raw latency bounces around but EWMA smooths it out. As EWMA rises, optimal_load drops.

### What to Observe

✅ **EWMA Smoothing**: Raw latencies are noisy (50-70ms) but EWMA stays smooth (~55-60ms)

✅ **Load Correlation**: 
- When EWMA latency rises → optimal_load decreases
- When EWMA latency falls → optimal_load increases

✅ **Confidence Dynamics**:
- First few metrics: confidence = 0.5 (no history)
- Repeating patterns: confidence rises to 0.7-0.95
- Sudden spikes: confidence drops (high variance)

✅ **Status Transitions**:
- Stable low latency (45-55ms) → "healthy"
- Rising latency (100-150ms) → "stressed"
- High latency (200+ms) + high error → "overloaded"

### Key Thresholds (from ServiceAgent defaults)

| Metric | Threshold | Notes |
|--------|-----------|-------|
| latency_threshold | 200ms | Above = overloaded |
| error_threshold | 5% (0.05) | Above = overloaded |
| cpu_threshold | 80% (0.8) | Above = overloaded |
| alpha (EWMA) | 0.3 | 30% current, 70% history |
| window_size | 5 | Last 5 observations tracked |

### Files Involved

- **Metrics Generation**: `mocks/base_simulator.py`
- **Metrics Collection**: `kafka_core/prometheus_kafka_adaptar.py`
- **Processing Logic**: `agents/service_agent/service_agent.py`
- **Runner**: `runners/service_agent_runner.py`
- **Configuration**: `kafka_core/config.py`, `kafka_core/prometheus_adapter_config.py`

### Notes

- All calculations are **stateful per service agent instance** (separate EWMA per service_id)
- Messages are **continuously** flowing (not snapshots)
- Kafka retains all messages for **7 days** by default
- Service agents process messages in order (offset management)
- Each service is monitored independently with its own EWMA state

---
