# Latest Changes

## 🔁 Pipeline Consistency Fix
- Standardized the full data flow:
  `/metrics -> metrics.events -> service.state -> topo.decisions`
- Added a shared pipeline map for AWS, AKS, and DigitalOcean services:
  - AWS -> partition `0`
  - AKS -> partition `1`
  - DigitalOcean -> partition `2`
- Ensured all main pipeline topics use 3 partitions:
  - `metrics.events`
  - `service.state`
  - `topo.decisions`

## ⚙️ Kafka Flow Updates
- Prometheus adapter now sends each service metric event to the canonical partition.
- Service agents now consume `metrics.events` and publish `service.state` with stable `service@cloud` keys.
- Topography agents now consume `service.state` instead of `metrics.events`.
- Topography decisions now publish to `topo.decisions` with 3-partition routing.
- Consumer base now supports explicit partition assignment for deterministic pipeline runners.
- Producer base now supports explicit Kafka `key` and `partition` on send.

## 👀 Live Pipeline Visualizer
- Added live runner:
  `runners/pipeline_live_runner.py`
- The runner starts:
  - adapter polling loop
  - 3 service-agent threads
  - 3 topography-agent threads
  - Kafka observers for all pipeline topics
  - Flask/SSE UI visualizer
- UI shows live events moving through:
  `metrics.events -> service.state -> topo.decisions`

## ▶️ Commands To Run

Install dependencies if needed:

```bash
python3 -m pip install -r requirements.txt
```

Start Kafka:

```bash
docker compose up -d zookeeper kafka
```

Start mock `/metrics` services:

```bash
python3 -m runners.mocks_runner
```

Start live pipeline runner and visualizer:

```bash
python3 -m runners.pipeline_live_runner --bootstrap-servers localhost:9092 --interval 5
```

Open the UI:

```text
http://127.0.0.1:8088
```

## 🧪 Verification
- Python compile check passed for the changed pipeline files.
- Local topography-agent smoke test passed.
- Full live validation requires Kafka, Python dependencies, and mock services running locally.
