# Service Agent Documentation

## 1. Purpose and Scope

The Service Agent in [agents/service_agent/service_agent.py](agents/service_agent/service_agent.py) is a Kafka consumer-producer component that:

1. Consumes service metrics from topic `metrics.events`.
2. Builds a local health belief for one target service.
3. Derives a load intent (`current_load`, `optimal_load`) from that belief.
4. Publishes the result to topic `service.state`.

This is the first decision stage in the larger multi-agent flow.

## 2. Runtime Position in the Pipeline

End-to-end flow around Service Agent:

1. Mock cloud simulators expose Prometheus metrics endpoints.
2. Prometheus Kafka adapter polls and normalizes metrics.
3. Adapter publishes normalized events to `metrics.events`.
4. Service Agent consumes `metrics.events`.
5. Service Agent filters events by configured `service_id`.
6. Service Agent computes belief and intent.
7. Service Agent publishes `ServiceState` events to `service.state`.

## 3. Inputs and Outputs

### 3.1 Input Topic

- Topic: `metrics.events`
- Consumed via: `KafkaConsumerTemplate`
- Consumer group default: `service_agent`

### 3.2 Input Event Shape (canonical)

The agent expects a single canonical input format (Shape B) — the flat payload produced by the Prometheus adapter.

Example (Shape B):

```json
{
  "service": "service-cache-aws",
  "cloud": "aws",
  "timestamp": "2026-04-30 06:30:00.996773",
  "metrics": {
    "request_latency_ms": 45.08,
    "request_count": 147,
    "error_count": 2,
    "error_rate_percent": 0.78,
    "cpu_usage_percent": 32.19,
    "memory_usage_percent": 59.0,
    "timestamp": "2026-04-30T06:30:00.996753"
  },
  "correlation_id": "prometheus-aws-...",
  "parent_event_id": null
}
```

For incoming flat messages the agent constructs a synthetic partition key `service@cloud` and proceeds to validate the payload against the `MetricsEventValue` schema. The agent no longer accepts a wrapped `{key,value}` envelope — Shape B is the only supported input.

### 3.3 Output Topic

- Topic: `service.state`
- Produced schema: `ServiceState`
- Value schema: `ServiceStateValue`

Output contains:

- `service`, `cloud`, `timestamp`
- `belief`: `latency_ewma`, `trend`, `confidence`, `status`
- `intent`: `current_load`, `optimal_load`
- `metadata`: source event key and selected metrics
- `correlation_id`, `parent_event_id`

## 4. Constructor Parameters and Hardcoded Defaults

`ServiceAgent.__init__(...)` defaults:

- `service_id` (required): target service filter
- `alpha=0.3`: EWMA smoothing factor
- `latency_threshold=200.0` (ms): main health threshold
- `error_threshold=0.05`: treated as fraction (5%)
- `cpu_threshold=0.8`: treated as fraction (80%)
- `window_size=5`: sliding history depth for trend/confidence
- `group_id="service_agent"`: Kafka consumer group

### Why these defaults

1. `alpha=0.3`
- Gives moderate responsiveness.
- New data influences estimate, but historical signal is not discarded.

2. `latency_threshold=200ms`
- Practical upper bound for many interactive APIs.
- Used as anchor for overloaded/stressed decisions.

3. `error_threshold=5%`, `cpu_threshold=80%`
- Common operational thresholds to flag service instability.

4. `window_size=5`
- Enough short-term history for trend and spread without over-smoothing.

5. `group_id="service_agent"`
- Works for single-agent local tests.
- For multi-agent-per-service deployments, unique group IDs are recommended.

## 5. Metric Key Mapping and Normalization

The agent accepts multiple key aliases to handle upstream schema variations.

### 5.1 Latency

Looks for first present key in order:

- `latency_ms`
- `request_latency_ms`

### 5.2 Error rate

Looks for:

- `error_rate`
- `error_rate_percent`

Then normalizes via `_normalize_percentage`:

- if value > 1.0, divide by 100
- clamp to [0.0, 1.0]
- round to 3 decimals

This supports both `0.34` and `34.0` styles.

### 5.3 CPU

Looks for:

- `cpu_percent`
- `cpu`
- `cpu_usage_percent`

Then normalized same as error rate.

### 5.4 Throughput and Memory (metadata only)

Throughput key order:

- `throughput`
- `requests_per_sec`
- `request_count`

Memory key order:

- `memory_percent`
- `memory`
- `memory_usage_percent`

## 6. Function-by-Function Behavior

### `update_ewma(current_latency)`

Computes exponentially weighted moving average:

- first point initializes EWMA
- later points use:

EWMA_t = alpha * x_t + (1 - alpha) * EWMA_{t-1}

Purpose:
- smooth noisy latency while keeping recent sensitivity.

### `update_window(current_latency, error_rate)`

Maintains bounded sliding windows:

- `latency_window` always updated
- `error_window` updated when error exists
- each capped at `window_size`

Purpose:
- trend and spread calculations on recent history only.

### `compute_trend()`

Returns point-to-point latency delta:

trend = latency_window[-1] - latency_window[-2]

If less than 2 points, returns `0.0`.

### `compute_confidence()`

Measures stability using spreads:

- `latency_spread = max(latency_window) - min(latency_window)`
- `error_spread = max(error_window) - min(error_window)` or 0.0

Raw score:

score = 1.0 - min(latency_spread/100, 0.5) - min(error_spread/0.1, 0.3)

Final confidence:

- clamp to [0.5, 0.95]
- round to 2 decimals

Interpretation:
- low spread => high confidence
- high variability => lower confidence

### `classify_status(latency, error_rate, cpu)`

3-state classification:

1. `overloaded` if any:
- latency > `latency_threshold`
- error_rate >= `error_threshold`
- cpu >= `cpu_threshold`

2. `stressed` if not overloaded and any:
- latency > 0.8 * `latency_threshold`
- error_rate >= 0.6 * `error_threshold`
- cpu >= 0.75 * `cpu_threshold`

3. otherwise `healthy`

Design choice:
- boolean condition sets provide simple, explainable health logic.

### `estimate_current_load(latency, error_rate, cpu)`

Computes synthetic current load estimate using penalties.

Start:

- `penalty = 0.0`

Latency contribution:

- if latency > threshold:
  penalty += min((latency - threshold)/(2*threshold), 0.5)
- else:
  penalty += max((latency - 0.5*threshold)/threshold, 0.0) * -0.1

Error contribution:

- penalty += min(error_rate * 2.0, 0.5)

CPU contribution:

- penalty += max((cpu - 0.5) * 0.4, 0.0)

Then:

- `current_load = 1.0 - penalty`
- clamp to [0.05, 0.95]
- round to 2 decimals

Interpretation:
- larger latency/error/cpu increases penalty and decreases load confidence.

### `derive_optimal_load(ewma_latency)`

Maps the EWMA latency to a single `optimal_load` scalar the service can optimally handle.

- Let `r = ewma_latency / latency_threshold`
- `optimal_load = clamp(1.0 - r/2.0, 0.05, 0.95)`
- Round to 2 decimals

Interpretation:
- `ewma` near 0 -> `optimal_load` close to 1.0 (service can take full load)
- `ewma` ~= `latency_threshold` -> `optimal_load` ~= 0.5
- Higher `ewma` reduces `optimal_load` towards the lower clamp

### `build_belief(...)`

Orchestrates belief fields:

1. update EWMA
2. update windows
3. compute trend
4. compute confidence
5. classify status

Returns:

- `latency_ewma`
- `trend` as signed string with `ms`
- `confidence`
- `status`

### `build_intent(...)`

Uses belief and normalized metrics to compute:

- `current_load`
- `optimal_load`

### `_extract_metric(metrics, *keys)`

Utility to fetch first available numeric metric by alias order.

### `_normalize_percentage(value)`

Utility to normalize fraction/percent ambiguity.

### `_parse_metrics_event(message)`

Compatibility parser:

- wrapped envelope -> `MetricsEvent(**message)`
- flat payload -> `MetricsEventValue(**message)` then wrap

### `process_message(topic, message)`

Core execution path:

1. parse message shape
2. filter by configured `service_id`
3. extract and normalize metrics
4. skip if no latency found
5. build belief and intent
6. build `ServiceState`
7. publish to `service.state`
8. log result

Return value behavior:

- `True` on processed/ignored/skip cases where commit is okay
- `False` only on invalid event parse failure

### `close()`

Graceful shutdown for consumer and producer.

## 7. Workflow and Dataflow Inside the Agent

For each consumed message:

1. `process_message` receives deserialized dict from Kafka consumer.
2. `_parse_metrics_event` creates canonical `MetricsEvent` representation.
3. Service filter compares incoming service vs configured `service_id`.
4. Metric extraction maps alias keys and normalizes error/cpu.
5. Belief is built (EWMA, trend, confidence, status).
6. Intent is built (current and desired load).
7. `ServiceState` object is created and validated.
8. Event is sent by `KafkaProducerTemplate` to `service.state`.

## 8. Why These Concepts and Formulas

### EWMA over simple average

- EWMA emphasizes recency while keeping history memory.
- Better for adaptive control under fluctuating traffic.

### Spread-based confidence

- Spread is cheap and robust for local online estimation.
- High spread implies instability; confidence should drop.

### Rule-based status thresholds

- Transparent and easy to debug during early system phases.
- Avoids black-box behavior before enough training data exists.

### Penalty-to-load model

- Converts multi-metric health into single actionable scalar.
- Works as a bridge signal for downstream orchestration agents.

### Discrete desired-load shifts

- Controlled deterministic behavior for early-stage policy tuning.
- Easier to reason about than continuous PID-like controllers.

## 9. Running the Agent

### 9.1 Through Docker Compose

From repo root:

```powershell
docker-compose up -d zookeeper kafka aws-simulator aks-simulator digitalocean-simulator prometheus prometheus-adapter service-agent
```

Current `service-agent` compose service uses:

- `SERVICE_ID=service-cache-aws`
- broker `kafka:9092`

### 9.2 Through Python Runner

```powershell
$env:SERVICE_ID="service-cache-aws"
python runners/service_agent_runner.py
```

## 10. Validation Commands

Consume one input event:

```powershell
docker exec kafka_1 bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic metrics.events --from-beginning --max-messages 1"
```

Consume one output event:

```powershell
docker exec kafka_1 bash -lc "kafka-console-consumer --bootstrap-server localhost:9092 --topic service.state --from-beginning --max-messages 1"
```

Check agent logs:

```powershell
docker logs service-agent | Select-String "Published service state|Skipping metrics event|Ignoring metrics|Invalid metrics|ERROR" | Select-Object -Last 40
```

Success signal:

- log line containing `Published service state ...`
- visible JSON message on `service.state` with `belief` and `intent`

## 11. Known Caveats and Troubleshooting

### 11.1 TopicAlreadyExistsError at startup

Observed when topics already exist.

- This is noisy but non-fatal for agent processing.
- The consumer and producer can still run.

### 11.2 Event shape mismatch

If adapter emits flat payload but parser expects wrapped format, parsing fails.

Current implementation already handles both shapes.

### 11.3 Missing latency key

If metrics do not include either:

- `latency_ms`
- `request_latency_ms`

agent logs:

- `Skipping metrics event without latency_ms`

### 11.4 Service filter mismatch

If `SERVICE_ID` does not match incoming service values, events are ignored.

Check incoming service names in `metrics.events` and set `SERVICE_ID` accordingly.

### 11.5 Multi-agent scaling behavior

If multiple agents share one consumer group:

- Kafka partitions are load-balanced across instances.
- Agents may not each observe all services.

For one-agent-per-service deployment, use unique `group_id` per agent instance.

## 12. Code Reference

- Agent implementation: [agents/service_agent/service_agent.py](agents/service_agent/service_agent.py)
- Agent module: [agents/service_agent/](agents/service_agent/)
- Runner: [runners/service_agent_runner.py](runners/service_agent_runner.py)
- Schemas: [kafka_core/schemas.py](kafka_core/schemas.py)
- Upstream adapter: [kafka_core/prometheus_kafka_adaptar.py](kafka_core/prometheus_kafka_adaptar.py)
- Runtime wiring: [docker-compose.yml](docker-compose.yml)
