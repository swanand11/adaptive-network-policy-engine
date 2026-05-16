# Proactive Agentic Multi-Cloud Pipeline Walkthrough

This document outlines how to execute the entire **Adaptive Network Policy Engine** pipeline from scratch and how to validate it using the newly updated Nexus Dashboard.

## 1. Start Core Infrastructure and Services

The core infrastructure includes Kafka, Zookeeper, Redis, MongoDB, the Cloud Mock Simulators (AWS, AKS, DO), the NGINX Execution Proxy, the Prometheus Metrics Server, and the Traffic Generator.

Run the following command in the project root:

```bash
docker-compose up -d
```

> [!TIP]  
> You can run `docker-compose ps` to verify that all containers (like `kafka_1`, `aws-simulator`, `prometheus`, etc.) are up and running.

## 2. Start the Multi-Agent System

With the infrastructure running, Prometheus will automatically begin scraping metrics from the execution proxy and the mock simulators.

To start the multi-agent system (which includes the Service Agents, Topography Agents, and the Prometheus-to-Kafka adapter), run the live pipeline runner:

```bash
python -m runners.pipeline_live_runner --bootstrap-servers localhost:9092
```

> [!NOTE]  
> The live runner also spins up an internal visualizer on `http://127.0.0.1:8088` where you can see the event logs moving between topics (`metrics.events` -> `service.state` -> `topo.decisions`).

## 3. Start the Governance & Execution Agents

To complete the end-to-end loop (where Topography decisions are validated and then applied to the load balancer proxy), you must run the Governance Stub and the Execution Consumer.

Open two new terminal windows and run:

**Terminal 1 (Governance Stub):**
```bash
python -m runners.decision_producer
```

**Terminal 2 (Execution Consumer):**
```bash
python -m runners.execution_consumer
```

> [!IMPORTANT]  
> The Execution Consumer connects to the `load-balancer-proxy` to update traffic weights dynamically via the `/update_weights` endpoint based on the policy decisions.

## 4. Validate with the Nexus Dashboard

The dashboard has been updated to provide a premium, agentic, multi-cloud visualization of your pipeline.

1. Open `dashboard/index.html` in your web browser.
2. The dashboard will automatically start polling the Prometheus API (`http://localhost:9090`).
3. **What to look for:**
   - **Pipeline Telemetry:** You will see the visual flow of data from Traffic Ingress through to the Governance Stub.
   - **CSP Metrics Matrix:** Live, individualized metrics for AWS, AKS, and DigitalOcean will appear, including CPU Usage, Latency, Error Rates, and the total requests routed to them by the proxy.
   - **Event Stream:** Keep an eye on the side-panel event stream. It will log active polling, warn you of high CPU loads, and alert you if an error rate spikes.

## Pipeline Flow Summary

1. `traffic_generator` hits the `load-balancer-proxy`.
2. `load-balancer-proxy` routes to `aws-simulator`, `aks-simulator`, `digitalocean-simulator`.
3. `prometheus` scrapes metrics from the proxy and the simulators.
4. `prometheus-adapter` reads from Prometheus and produces to `metrics.events`.
5. `ServiceAgent` reads `metrics.events` and produces to `service.state`.
6. `TopographyAgent` reads `service.state` and produces to `topo.decisions`.
7. `decision_producer` (Governance Stub) reads `topo.decisions` and produces to `policy.decisions`.
8. `execution_consumer` reads `policy.decisions` and updates the `load-balancer-proxy` weights via HTTP.

