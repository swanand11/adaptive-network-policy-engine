import logging
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import uuid4

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import (
    MetricsEvent,
    ServiceState,
    ServiceStateValue,
    ServiceStateBelief,
    ServiceStateIntent,
)

logger = logging.getLogger(__name__)


class ServiceAgent(KafkaConsumerTemplate):
    """Service Agent consumes metrics.events and produces service.state events."""

    def __init__(
        self,
        service_id: str,
        alpha: float = 0.3,
        latency_threshold: float = 200.0,
        error_threshold: float = 0.05,
        cpu_threshold: float = 0.8,
        window_size: int = 5,
        group_id: str = "service_agent",
    ):
        super().__init__(topics=["metrics.events"], group_id=group_id)
        self.service_id = service_id
        self.alpha = alpha
        self.latency_threshold = latency_threshold
        self.error_threshold = error_threshold
        self.cpu_threshold = cpu_threshold
        self.ewma_latency: Optional[float] = None
        self.latency_window: list[float] = []
        self.error_window: list[float] = []
        self.window_size = window_size
        self.producer = KafkaProducerTemplate()

    def update_ewma(self, current_latency: float) -> float:
        if self.ewma_latency is None:
            self.ewma_latency = current_latency
        else:
            self.ewma_latency = (
                self.alpha * current_latency
                + (1 - self.alpha) * self.ewma_latency
            )
        return self.ewma_latency

    def update_window(self, current_latency: float, error_rate: Optional[float]) -> None:
        self.latency_window.append(current_latency)
        if len(self.latency_window) > self.window_size:
            self.latency_window.pop(0)

        if error_rate is not None:
            self.error_window.append(error_rate)
            if len(self.error_window) > self.window_size:
                self.error_window.pop(0)

    def compute_trend(self) -> float:
        if len(self.latency_window) < 2:
            return 0.0
        return self.latency_window[-1] - self.latency_window[-2]

    def compute_confidence(self) -> float:
        if len(self.latency_window) < 2:
            return 0.5

        latency_spread = max(self.latency_window) - min(self.latency_window)
        error_spread = max(self.error_window) - min(self.error_window) if self.error_window else 0.0
        score = 1.0 - min((latency_spread / 100.0), 0.5) - min((error_spread / 0.1), 0.3)
        return round(max(min(score, 0.95), 0.5), 2)

    def classify_status(
        self,
        latency: float,
        error_rate: Optional[float],
        cpu: Optional[float],
    ) -> str:
        overloaded_conditions = [latency > self.latency_threshold]
        if error_rate is not None:
            overloaded_conditions.append(error_rate >= self.error_threshold)
        if cpu is not None:
            overloaded_conditions.append(cpu >= self.cpu_threshold)

        if any(overloaded_conditions):
            return "overloaded"

        stressed_conditions = [latency > self.latency_threshold * 0.8]
        if error_rate is not None:
            stressed_conditions.append(error_rate >= self.error_threshold * 0.6)
        if cpu is not None:
            stressed_conditions.append(cpu >= self.cpu_threshold * 0.75)

        if any(stressed_conditions):
            return "stressed"

        return "healthy"

    def estimate_current_load(
        self,
        latency: float,
        error_rate: Optional[float],
        cpu: Optional[float],
    ) -> float:
        penalty = 0.0
        if latency > self.latency_threshold:
            penalty += min((latency - self.latency_threshold) / (self.latency_threshold * 2), 0.5)
        else:
            penalty += max((latency - self.latency_threshold * 0.5) / self.latency_threshold, 0.0) * -0.1

        if error_rate is not None:
            penalty += min(error_rate * 2.0, 0.5)

        if cpu is not None:
            penalty += max((cpu - 0.5) * 0.4, 0.0)

        current_load = 1.0 - penalty
        return round(max(min(current_load, 0.95), 0.05), 2)

    def derive_desired_load(self, current_load: float, status: str) -> float:
        if status == "overloaded":
            return round(max(current_load - 0.25, 0.0), 2)
        if status == "stressed":
            return round(max(current_load - 0.1, 0.0), 2)
        return round(min(current_load + 0.05, 1.0), 2)

    def build_belief(
        self,
        current_latency: float,
        current_error: Optional[float],
        current_cpu: Optional[float],
    ) -> Dict[str, Any]:
        ewma = self.update_ewma(current_latency)
        self.update_window(current_latency, current_error)
        trend_value = self.compute_trend()
        confidence = self.compute_confidence()
        status = self.classify_status(current_latency, current_error, current_cpu)

        return {
            "latency_ewma": round(ewma, 2),
            "trend": f"{'+ ' if trend_value >= 0 else ''}{round(trend_value, 2)}ms".replace('+ ', '+'),
            "confidence": confidence,
            "status": status,
        }

    def build_intent(
        self,
        belief: Dict[str, Any],
        current_error: Optional[float],
        current_cpu: Optional[float],
    ) -> Dict[str, float]:
        current_load = self.estimate_current_load(
            latency=belief["latency_ewma"],
            error_rate=current_error,
            cpu=current_cpu,
        )
        desired_load = self.derive_desired_load(current_load, belief["status"])

        return {
            "current_load": current_load,
            "desired_load": desired_load,
        }

    def process_message(self, topic: str, message: Dict[str, Any]) -> bool:
        try:
            event = MetricsEvent(**message)
        except Exception as exc:
            logger.error(f"Invalid metrics event: {exc} | message={message}")
            return False

        if event.value.service != self.service_id:
            logger.debug(
                "Ignoring metrics for service '%s' (expecting '%s')",
                event.value.service,
                self.service_id,
            )
            return True

        metrics = event.value.metrics
        current_latency = self._extract_metric(metrics, "latency_ms")
        if current_latency is None:
            logger.warning("Skipping metrics event without latency_ms")
            return True

        raw_error = self._extract_metric(metrics, "error_rate")
        raw_cpu = self._extract_metric(metrics, "cpu_percent", "cpu")
        current_error = self._normalize_percentage(raw_error)
        current_cpu = self._normalize_percentage(raw_cpu)
        current_throughput = self._extract_metric(metrics, "throughput", "requests_per_sec")
        memory_percent = self._extract_metric(metrics, "memory_percent", "memory")

        belief = self.build_belief(current_latency, current_error, current_cpu)
        intent = self.build_intent(belief, current_error, current_cpu)

        service_state = ServiceState(
            key=str(uuid4()),
            value=ServiceStateValue(
                service=event.value.service,
                cloud=event.value.cloud,
                timestamp=datetime.utcnow(),
                belief=ServiceStateBelief(**belief),
                intent=ServiceStateIntent(**intent),
                metadata={
                    "source_event_key": event.key,
                    "metrics": metrics,
                    "throughput": current_throughput,
                    "memory_percent": memory_percent,
                    "normalized_cpu": current_cpu,
                    "normalized_error_rate": current_error,
                },
                correlation_id=event.value.correlation_id,
                parent_event_id=event.value.parent_event_id,
            ),
        )

        self.producer.send("service.state", service_state)
        logger.info(
            "Published service state %s for %s@%s: desired_load=%s",
            service_state.key,
            event.value.service,
            event.value.cloud.value,
            intent["desired_load"],
        )
        logger.debug("belief=%s intent=%s", belief, intent)
        return True

    @staticmethod
    def _extract_metric(metrics: Dict[str, Any], *keys: str) -> Optional[float]:
        for key in keys:
            value = metrics.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _normalize_percentage(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        normalized = float(value)
        if normalized > 1.0:
            normalized = normalized / 100.0
        return round(max(min(normalized, 1.0), 0.0), 3)

    def close(self) -> None:
        super().close()
        if self.producer:
            self.producer.close()
