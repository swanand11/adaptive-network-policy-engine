# Latest Changes

## 🔄 Dependency Update
- Replaced `kafka-python` with `kafka-python-ng`
- Fixes compatibility issues with newer Python versions (3.12+)
- Resolves `kafka.vendor.six.moves` import errors

## ⚙️ Topography Agent Implementation
- Implemented `TopographyAgent` per service
- Each agent independently:
  - Consumes normalized load signals
  - Computes pressure (`P_i = L_opt_i - L_i`)
  - Identifies overloaded and underloaded services
  - Generates redistribution actions
- Designed for distributed, event-driven execution

## 🧪 Pipeline Runner
- Added `topo_test.py` as a manual pipeline runner
- Integrates:
  - `NormalizedLoadSimulator`
  - `TopographyAgent`
- Allows local testing without full Kafka setup (when configured accordingly)

## 🧠 Purpose
- Validate system behavior and action generation
- Observe redistribution logic in simulated environments
- Enable rapid iteration without full infrastructure dependency