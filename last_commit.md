# Nexus Policy Engine - End-to-End Governance & UI Integration

This document outlines the recent architectural improvements, bug fixes, and instructions on how to run and verify the complete closed-loop system.

---

## 🛠️ Summary of Changes

### 1. UI Crash Fix (`Uncaught TypeError` in `<ApprovalCard>`)
- **Issue**: The React UI's `<ApprovalCard>` component failed with `Cannot read properties of undefined (reading 'service')` because the Flask REST API served a flat pending approval payload missing the nested `"decision"` sub-object defined in the UI's types.
- **Fix**: Upgraded `_maybe_enqueue_highrisk()` in `backend/governance_flask_api.py` to populate the expected `"decision"` sub-object (containing `service`, `decision`, `risk_level`, `status`, and `metadata`) when storing pending approvals in the cache.

### 2. Topography & Governance Nomenclature Bridge
- **Issue**: Topography agents emitted overloaded/underload actions with service names (`service-cache`, `service-db`, `service-cache-aws`), whereas the Governance Agent computed traffic redirection using standard CSP codes (`["aws", "aks", "do"]`). This nomenclature mismatch caused the agent to drop all actions and output a static uniform distribution (`33% / 34% / 33%`).
- **Fix**: Implemented a mapping helper (`map_to_csp()`) in `agents/governance_agent/governance_logic.py` to dynamically bridge naming patterns from the topology agents to standard CSP codes. Governance now acts on and propagates load-balancing decisions in real-time.

### 3. Closed-Loop Human Approval Propagation
- **Issue**: Approving high-risk policies in the UI published events to the isolated topic `governance.approved`, while NGINX and local execution consumers listened on `policy.approved` or `policy.decisions`. Weight overrides were never applied.
- **Fix**: Upgraded `publish_decision()` in `backend/governance_flask_api.py` to wrap approved decisions in standard `PolicyDecision` events and broadcast them to both `policy.approved` and `policy.decisions`. Approving a policy in the UI now instantly overrides/shifts weights in the load balancer.

### 4. Runner & Pipeline Optimization
- **Fix**: Computed absolute config paths for `03_service_agents.py` and forwarded `PYTHONPATH` context to topology subprocesses in `04_topo_agent.py` to eliminate relative path loading errors.
- **Clean**: Removed the redundant FastAPI backend (`backend/api.py`) from Terminal 7 in the `run_all_terminals.sh` runner since the UI uses the polling REST API on port `5000` exclusively.

---

## 🚀 Execution & Verification Commands

Follow these steps to launch the complete end-to-end multi-agent orchestration loop:

### Step 1: Spin Up Docker Infrastructure
Ensure the supporting message broker and mock microservices are running:
```bash
docker compose up -d zookeeper kafka redis mongo aws-simulator aks-simulator digitalocean-simulator load-balancer-proxy prometheus
```

### Step 2: Start the Active Pipeline
Run the unified terminal launcher inside the `async_pipeline` directory. This spins up all service/topology/governance agents, the Flask backend, and the traffic generator:
```bash
cd async_pipeline
./run_all_terminals.sh
```

### Step 3: Run the React UI Dashboard
In a separate terminal, launch the frontend server:
```bash
cd ui
npm run dev
```

### Step 4: Open and Verify
- Open your browser to [http://localhost:5173](http://localhost:5173).
- **Observe**: The traffic generator simulates load spikes on `aws`, prompting the Topology Agent to suggest redistribution plans. High-risk decisions appear as interactive cards in the **Governance Queue** page. Approving them immediately updates the **Active Upstream Weights** panel via OpenResty!
