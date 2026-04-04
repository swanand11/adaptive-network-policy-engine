# feat: Add Prometheus Metrics Adapter for Event Production Pipeline

## Changes

### New Files
- `prometheus_kafka_adaptar.py`: Main adapter orchestrating polling, normalization, and Kafka event production
  - `PrometheusParser`: Parses OpenMetrics text format with regex
  - `MetricsNormalizer`: Normalizes Prometheus metrics to common schema (latency, request count, error rate, CPU, memory)
  - `PrometheusPoller`: HTTP polling with retry logic (3 retries, 0.5s backoff)
  - `PrometheusMetricsAdapter`: Main orchestrator—polls 3 endpoints (AWS, AKS, DigitalOcean) every 5 seconds, sends to Kafka
  
- `prometheus_adapter_config.py`: Environment-specific configuration with validation
  - Endpoint URLs configurable via env vars
  - Poll interval, timeout, retry settings
  - Metric field mappings for normalization

### Modified Files
- **Refactored `kafka/` → `kafka_core/`**: Renamed to avoid naming collision with `kafka` PyPI library (was causing pydantic export issues)
- **Relative imports**: Updated all imports from absolute (`from kafka.x`) to relative (`from .x`) per Python standards

## How It Works

1. **Polling**: Continuously polls 3 Prometheus endpoints every 5 seconds
2. **Parsing**: Extracts metrics using regex patterns (name, labels, value)
3. **Normalization**: Maps Prometheus metrics to normalized schema:
   - `request_latency_seconds_sum/count` → `request_latency_ms` (avg)
   - `request_count_total` → `request_count`
   - `error_count_total` → `error_count`
   - Auto-calculates `error_rate_percent` if needed
   - Extracts `cpu_usage_percent`, `memory_usage_percent`
4. **Event Creation**: Wraps normalized metrics in `MetricsEvent` with correlation ID and cloud provider
5. **Kafka Send**: Publishes to `metrics.events` topic with partition key (cloud:service_id)
6. **Resilience**: Retries failed requests (max 3 attempts, exponential backoff 0.5s)

## Configuration

Environment variables (optional):
- `PROMETHEUS_POLL_INTERVAL`: Polling interval (default: 5s)
- `PROMETHEUS_TIMEOUT`: Request timeout (default: 3s)
- `PROMETHEUS_AWS_URL`, `PROMETHEUS_AKS_URL`, `PROMETHEUS_DO_URL`: Endpoint URLs
- `LOG_LEVEL`: Logging level (default: INFO)

## Usage

```bash
docker-compose up --build
docker-compose logs -f prometheus-adapter
```

## Implementation Notes
- Inherits from `KafkaProducerTemplate` for schema-driven production
- Handles partial failures gracefully (one endpoint down doesn't block others)
- Configurable partition key ensures ordering per service/cloud
- Production-ready error handling and logging