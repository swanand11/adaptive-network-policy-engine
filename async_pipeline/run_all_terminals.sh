#!/bin/bash
# This script opens a new terminal window for each pipeline stage.
# It uses gnome-terminal (default on many Linux distros).

cd "$(dirname "$0")"

echo "Launching 01_test_mocks..."
gnome-terminal --title="01_Test_Mocks" -- bash -c "python3 01_test_mocks.py; exec bash"

echo "Launching 02_prometheus_adapter..."
gnome-terminal --title="02_Prometheus_Adapter" -- bash -c "python3 02_prometheus_adapter.py; exec bash"

echo "Launching 03_service_agents..."
gnome-terminal --title="03_Service_Agents" -- bash -c "python3 03_service_agents.py; exec bash"

echo "Launching 04_topo_agent..."
gnome-terminal --title="04_Topo_Agent" -- bash -c "python3 04_topo_agent.py; exec bash"

echo "Launching 05_governance_agent..."
gnome-terminal --title="05_Governance_Agent" -- bash -c "python3 05_governance_agent.py; exec bash"

echo "Launching 06_execution_consumer..."
gnome-terminal --title="06_Execution_Consumer" -- bash -c "python3 06_execution_consumer.py; exec bash"

echo "Launching 08_traffic_demo..."
gnome-terminal --title="08_Traffic_Demo" -- bash -c "python3 08_traffic_demo.py; exec bash"

echo "Launching 09_governance_flask_api (REST API for UI)..."
gnome-terminal --title="09_Governance_Flask_API" -- bash -c "cd .. && python3 -m backend.governance_flask_api; exec bash"

echo "All pipeline stages, backend APIs, and traffic generator launched in separate terminals!"
