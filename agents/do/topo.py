"""Topography Agent for AWS

Responsible for computing system-level actions to move the system toward optimal normalized load signals.
Operates in a distributed, event-driven architecture using Kafka as shared state layer.

Core Logic:
1. Consume global state of normalized loads from all services
2. Compute pressure per service: P_i = L_opt_i - L_i
3. Partition into overloaded (P_i < 0) and underloaded (P_i > 0) sets
4. Solve convex QP to find optimal redistribution flows f_ij
5. Produce action signals: {from, to, intensity}

Optimization Problem:
---------------------
Variables : f_ij >= 0  for i ∈ Overloaded, j ∈ Underloaded

Objective :
  min_f   Σ_i (P_i + Σ_j f_ij)²          ← overloaded residuals (want outflow = |P_i|)
        + Σ_j (P_j - Σ_i f_ij)²          ← underloaded residuals (want inflow = P_j)
        + β · Σ_ij f_ij²                  ← Tikhonov: penalise large flows
        - T · Σ_ij H(f_ij)               ← entropy barrier: smooth/unique solution

Constraints (projected after each gradient step):
  Σ_j f_ij  ≤  α · |P_i|    ∀ i ∈ Overloaded
  Σ_i f_ij  ≤  α · P_j      ∀ j ∈ Underloaded
  f_ij      ≥  0

Architecture:
- No direct inter-agent communication
- Kafka = shared state layer
- Each agent independently computes same result
- Eventual convergence expected
"""

import logging
import math
import time
import json
from typing import Dict, List, Optional
from datetime import datetime
import config
from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import PolicyDecision, PolicyDecisionValue
from kafka_core.enums import PolicyStatus, RiskLevel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_ENTROPY_EPS = 1e-9
_FLOW_THRESH = 1e-3


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _zeros(n: int, m: int) -> List[List[float]]:
    return [[0.0] * m for _ in range(n)]


def _clip(x: float, lo: float = 0.0, hi: float = float("inf")) -> float:
    return max(lo, min(hi, x))


# ---------------------------------------------------------------------------
# Convex QP solver — projected gradient + Armijo line search
# ---------------------------------------------------------------------------

class ConvexFlowSolver:
    """
    Solves the load-redistribution QP in a single call.

    Objective:
        F(f) = Σ_i (P_i + Σ_j f_ij)²        overloaded residuals
             + Σ_j (P_j - Σ_i f_ij)²        underloaded residuals
             + β · Σ_ij f_ij²               Tikhonov regularisation
             + T · Σ_ij f_ij·ln(f_ij + ε)  entropy barrier  (−T·H, strictly convex)

    Constraints (Dykstra projection):
        Σ_j f_ij  ≤  α · |P_i|   row capacity
        Σ_i f_ij  ≤  α ·  P_j   col capacity
        f_ij      ≥  0
    """

    def __init__(
        self,
        alpha: float,
        beta: float,
        temperature: float,
        max_iters: int = 300,
        tol: float = 1e-6,
        armijo_c: float = 1e-4,
        armijo_rho: float = 0.5,
    ):
        self.alpha       = alpha
        self.beta        = beta
        self.temperature = temperature
        self.max_iters   = max_iters
        self.tol         = tol
        self.armijo_c    = armijo_c
        self.armijo_rho  = armijo_rho

    def solve(
        self,
        overloaded:  List[str],
        underloaded: List[str],
        pressures:   Dict[str, float],
        confidences: Dict[str, float],
    ) -> List[Dict]:
        """
        Returns optimal flows as action list: [{from, to, intensity}, ...]
        """
        n_o = len(overloaded)
        n_u = len(underloaded)

        cap_row = [self.alpha * abs(pressures[s]) for s in overloaded]
        cap_col = [self.alpha * pressures[s]      for s in underloaded]

        # Warm start: confidence-weighted proportional allocation (greedy heuristic)
        f = _zeros(n_o, n_u)
        for i, s_i in enumerate(overloaded):
            for j, s_j in enumerate(underloaded):
                f[i][j] = confidences.get(s_i, 1.0) * confidences.get(s_j, 1.0)
        f = self._project(f, cap_row, cap_col)

        # Projected gradient descent with Armijo line search
        prev_obj = float("inf")
        for iteration in range(self.max_iters):
            obj  = self._objective(f, overloaded, underloaded, pressures, n_o, n_u)
            grad = self._gradient(f, overloaded, underloaded, pressures, n_o, n_u)

            step = 1.0
            for _ in range(50):
                f_new = self._project(
                    [[_clip(f[i][j] - step * grad[i][j]) for j in range(n_u)]
                     for i in range(n_o)],
                    cap_row, cap_col
                )
                obj_new  = self._objective(f_new, overloaded, underloaded, pressures, n_o, n_u)
                grad_dot = sum(
                    grad[i][j] * (f_new[i][j] - f[i][j])
                    for i in range(n_o) for j in range(n_u)
                )
                if obj_new <= obj + self.armijo_c * grad_dot:
                    break
                step *= self.armijo_rho

            f = f_new

            if abs(prev_obj - obj_new) < self.tol:
                logger.debug(f"Solver converged at iteration {iteration}")
                break
            prev_obj = obj_new

        # Emit result
        actions = [
            {"from": overloaded[i], "to": underloaded[j], "intensity": round(f[i][j], 4)}
            for i in range(n_o)
            for j in range(n_u)
            if f[i][j] > _FLOW_THRESH
        ]
        actions.sort(key=lambda a: a["intensity"], reverse=True)
        return actions

    # ------------------------------------------------------------------
    # Objective  F(f)
    # ------------------------------------------------------------------

    def _objective(self, f, overloaded, underloaded, pressures, n_o, n_u) -> float:
        residual = 0.0
        for i in range(n_o):
            residual += (pressures[overloaded[i]] + sum(f[i])) ** 2
        for j in range(n_u):
            absorbed = sum(f[i][j] for i in range(n_o))
            residual += (pressures[underloaded[j]] - absorbed) ** 2

        tikhonov = sum(f[i][j] ** 2      for i in range(n_o) for j in range(n_u))
        entropy  = sum(
            f[i][j] * math.log(f[i][j] + _ENTROPY_EPS)
            for i in range(n_o) for j in range(n_u)
        )
        return residual + self.beta * tikhonov + self.temperature * entropy

    # ------------------------------------------------------------------
    # Gradient  ∂F/∂f_ij
    # ------------------------------------------------------------------

    def _gradient(self, f, overloaded, underloaded, pressures, n_o, n_u) -> List[List[float]]:
        res_o = [pressures[overloaded[i]] + sum(f[i])                          for i in range(n_o)]
        res_u = [pressures[underloaded[j]] - sum(f[i][j] for i in range(n_o)) for j in range(n_u)]

        grad = _zeros(n_o, n_u)
        for i in range(n_o):
            for j in range(n_u):
                v = f[i][j]
                grad[i][j] = (
                    2.0 * res_o[i]                                            # ∂ overloaded residual²
                    - 2.0 * res_u[j]                                          # ∂ underloaded residual²
                    + 2.0 * self.beta * v                                     # ∂ Tikhonov
                    + self.temperature * (math.log(v + _ENTROPY_EPS) + 1.0)  # ∂ entropy barrier
                )
        return grad

    # ------------------------------------------------------------------
    # Dykstra projection onto {f≥0, row sums ≤ cap_row, col sums ≤ cap_col}
    # ------------------------------------------------------------------

    def _project(self, f, cap_row, cap_col, iters: int = 30) -> List[List[float]]:
        n_o = len(cap_row)
        n_u = len(cap_col)
        f   = [[_clip(f[i][j]) for j in range(n_u)] for i in range(n_o)]

        p = _zeros(n_o, n_u)  # Dykstra increments — row constraints
        q = _zeros(n_o, n_u)  # Dykstra increments — col constraints

        for _ in range(iters):
            prev = [row[:] for row in f]
            for i in range(n_o):
                y = [_clip(f[i][j] + p[i][j]) for j in range(n_u)]
                s = sum(y)
                if s > cap_row[i] > 0:
                    y = [v * cap_row[i] / s for v in y]
                for j in range(n_u):
                    p[i][j] = prev[i][j] + p[i][j] - y[j]
                    f[i][j] = _clip(y[j])

            prev = [row[:] for row in f]
            for j in range(n_u):
                y = [_clip(f[i][j] + q[i][j]) for i in range(n_o)]
                s = sum(y)
                if s > cap_col[j] > 0:
                    y = [v * cap_col[j] / s for v in y]
                for i in range(n_o):
                    q[i][j] = prev[i][j] + q[i][j] - y[i]
                    f[i][j] = _clip(y[i])

        return f


# ---------------------------------------------------------------------------
# TopographyAgent
# ---------------------------------------------------------------------------

class TopographyAgent(KafkaConsumerTemplate):
    """Topography Agent — computes load redistribution flows via convex QP."""

    def __init__(
        self,
        service_id: str = "do",
        consumer: Optional[KafkaConsumerTemplate] = None,
        producer: Optional[KafkaProducerTemplate] = None,
    ):
        if consumer:
            self.consumer = consumer
        else:
            super().__init__(
                topics=["metrics.events"],
                group_id=f"topography-{service_id}"
            )

        self.service_id = service_id
        self.producer   = producer or KafkaProducerTemplate()

        # Global state: service_id → {L_i, L_opt_i, confidence}
        self.global_state: Dict[str, Dict] = {}

        # Hyper-parameters
        self.alpha       = 0.35   # fraction of |pressure| available for redistribution
        self.beta        = 0.18   # Tikhonov weight (penalises large individual flows)
        self.temperature = 1.2    # entropy barrier scale (anneals across decisions)
        self.gamma       = 0.05   # temperature decay rate per published decision

        self.iteration_count       = 0
        self.last_computation_time = 0.0
        self.computation_interval  = 5.0

        logger.info(f"Topography Agent initialised for service {service_id}")

    # ------------------------------------------------------------------
    # Kafka ingestion
    # ------------------------------------------------------------------

    def process_message(self, topic: str, message: dict) -> bool:
        try:
            if "value" in message and "service" in message["value"]:
                service_id = message["value"]["service"]
                metrics    = message["value"].get("metrics", {})

                if "L_i" in metrics and "L_opt_i" in metrics:
                    self.global_state[service_id] = {
                        "L_i":        metrics.get("L_i",        0.0),
                        "L_opt_i":    metrics.get("L_opt_i",    0.0),
                        "confidence": metrics.get("confidence", 0.0),
                    }
                    logger.debug(f"Updated state for {service_id}: {self.global_state[service_id]}")

                    current_time = time.time()
                    if current_time - self.last_computation_time >= self.computation_interval:
                        self._compute_and_publish()
                        self.last_computation_time = current_time
                return True

            logger.warning(f"Invalid message format: {message}")
            return False

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            return False

    # ------------------------------------------------------------------
    # Compute → publish
    # ------------------------------------------------------------------

    def _compute_and_publish(self):
        if len(self.global_state) < 2:
            logger.info("Not enough services for redistribution")
            return
        try:
            actions = self._compute_redistribution_actions()
            if actions:
                self._publish_actions(actions)
            else:
                logger.info("No redistribution actions needed")
        except Exception as e:
            logger.error(f"Error computing actions: {e}")

    def _compute_redistribution_actions(self) -> List[Dict]:
        """
        1. Compute pressures  P_i = L_opt_i - L_i
        2. Partition overloaded (P<0) / underloaded (P≥0)
        3. Solve convex QP → [{from, to, intensity}, ...]
        """
        pressures   = {s: st["L_opt_i"] - st["L_i"] for s, st in self.global_state.items()}
        overloaded  = [s for s, p in pressures.items() if p <  0]
        underloaded = [s for s, p in pressures.items() if p >= 0]

        logger.info(f"Overloaded: {overloaded}  Underloaded: {underloaded}")

        if not overloaded or not underloaded:
            return []

        confidences  = {s: self.global_state[s]["confidence"] for s in self.global_state}
        current_temp = self.temperature * ((1 - self.gamma) ** self.iteration_count)

        return ConvexFlowSolver(
            alpha=self.alpha,
            beta=self.beta,
            temperature=current_temp,
        ).solve(overloaded, underloaded, pressures, confidences)

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------

    def _publish_actions(self, actions: List[Dict]):
        decision_id = f"topo-{self.service_id}-{int(time.time())}"

        decision_value = PolicyDecisionValue(
            service    = self.service_id,
            decision   = json.dumps({"actions": actions}),
            risk_level = self._assess_risk(actions),
            status     = PolicyStatus.PENDING,
            timestamp  = datetime.now(),
            metadata   = {
                "agent_type":        "topography",
                "solver":            "convex_qp_entropy_regularised",
                "iteration":         self.iteration_count,
                "global_state_size": len(self.global_state),
                "temperature":       self.temperature * ((1 - self.gamma) ** self.iteration_count),
                "alpha":             self.alpha,
                "beta":              self.beta,
                "gamma":             self.gamma,
            },
        )

        success = self.producer.send(
            "policy.decisions",
            PolicyDecision(key=decision_id, value=decision_value)
        )

        if success:
            logger.info(f"Published {decision_id} — {len(actions)} actions")
            self.iteration_count += 1
        else:
            logger.error(f"Failed to publish {decision_id}")

    def _assess_risk(self, actions: List[Dict]) -> RiskLevel:
        if not actions:
            return RiskLevel.LOW
        total = sum(a["intensity"] for a in actions)
        peak  = max(a["intensity"] for a in actions)
        if total > 0.8 or peak > 0.5:
            return RiskLevel.HIGH
        if total > 0.5 or peak > 0.3:
            return RiskLevel.MEDIUM
        return RiskLevel.LOW

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self):
        self.producer.close()
        super().close()


if __name__ == "__main__":
    import sys
    service_id = sys.argv[1] if len(sys.argv) > 1 else "aks"
    agent = TopographyAgent(service_id=service_id)
    try:
        logger.info(f"Starting Topography Agent for {service_id}")
        agent.start()
    except KeyboardInterrupt:
        logger.info("Shutting down Topography Agent")
        agent.close()