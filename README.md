# adaptive-network-policy-engine
multi agent networ policy management for cloud native distributed systems

## Live pipeline runner

Start Kafka and the mock `/metrics` services, then run the full observable flow:

```bash
python3 -m runners.mocks_runner
python3 -m runners.pipeline_live_runner --bootstrap-servers localhost:9092 --interval 5
```

Open `http://127.0.0.1:8088` to watch events move through:

```text
/metrics -> metrics.events[0..2] -> service.state[0..2] -> topo.decisions[0..2]
```
