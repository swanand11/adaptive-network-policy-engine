# Asynchronous Pipeline Debugging Setup

This directory contains standalone wrappers for each stage of the data pipeline. Since they are separated out, you can run them in distinct terminals. This completely decouples the pipeline and allows you to observe the inputs/outputs (or failures) of each individual component asynchronously!

## Prerequisites
Ensure that your mock servers (AWS, AKS, DO) and Kafka (via docker-compose) are already running. You can start the infrastructure with:
```bash
docker compose up -d zookeeper kafka redis mongo aws-simulator aks-simulator digitalocean-simulator load-balancer-proxy
```

## Running the Stages

You can either run the auto-launcher script which pops open a new terminal for each stage (requires `gnome-terminal`):
```bash
chmod +x run_all_terminals.sh
./run_all_terminals.sh
```

Or you can manually open 6 terminals and run these sequentially:

**Terminal 1 (Test if mocks are producing events):**
```bash
python3 01_test_mocks.py
```
*This continuously polls the mock endpoints and prints if they are returning metrics successfully.*

**Terminal 2 (Prometheus Adapter):**
```bash
python3 02_prometheus_adapter.py
```
*This scrapes the mocks and publishes `metrics.events` to Kafka.*

**Terminal 3 (Service Agents):**
```bash
python3 03_service_agents.py
```
*This consumes `metrics.events` and publishes `service.state`.*

**Terminal 4 (Topology Agent):**
```bash
python3 04_topo_agent_aws.py
```
*This consumes `service.state` and publishes `topo.decisions`.*

**Terminal 5 (Governance Agent):**
```bash
python3 05_governance_agent.py
```
*This consumes `topo.decisions` and publishes `policy.decisions`.*

**Terminal 6 (Execution Consumer):**
```bash
python3 06_execution_consumer.py
```
*This consumes `policy.decisions` and executes them against the load balancer.*

**Terminal 7 (WebSocket UI Bridge):**
```bash
python3 07_websocket_bridge.py
```
*This streams all the Kafka events directly to your React Frontend so everything reflects in the UI.*
