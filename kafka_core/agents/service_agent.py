import logging
from datetime import datetime
from typing import Dict, Any, Optional
from uuid import uuid4

from kafka_core.consumer_base import KafkaConsumerTemplate
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import MetricsEvent, PolicyDecision, PolicyDecisionValue
from kafka_core.enums import PolicyStatus, RiskLevel

logger = logging.getLogger(__name__)


class ServiceAgent(KafkaConsumerTemplate):
    """Service Agent that consumes metrics events and produces policy decisions."""

    def __init__(
        self,
        service_id: str,
        alpha: float = 0.3,
        latency_threshold: float = 200.0,
        group_id: str = "service_agent",
    ):
        super().__init__(topics=["metrics.events"], group_id=group_id)
        self.service_id = service_id
        self.alpha = alpha
        self.latency_threshold = latency_threshold
        self.ewma_latency: Optional[float] = None
        self.latency_window: list[float] = []
        self.window_size = 5
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

    def update_window(self, current_latency: float) -> None:
        self.latency_window.append(current_latency)
        if len(self.latency_window) > self.window_size:
            self.latency_window.pop(0)

    def compute_trend(self) -> float:
        if len(self.latency_window) < 2:
            return 0.0
        return self.latency_window[-1] - self.latency_window[-2]

    def compute_confidence(self) -> float:
        if len(self.latency_window) < 2:
            return 0.5
        variance = max(self.latency_window) - min(self.latency_window)
        if variance < 20:
            return 0.9
        if variance < 50:
            return 0.75
        return 0.6

    def build_belief(self, current_latency: float) -> Dict[str, Any]:
        ewma = self.update_ewma(current_latency)
        self.update_window(current_latency)
        trend_value = self.compute_trend()
        confidence = self.compute_confidence()

        return {
            "latency": round(ewma, 2),
            "trend": f"{'+ ' if trend_value >= 0 else ''}{round(trend_value, 2)}ms".replace('+ ', '+'),
            "confidence": round(confidence, 2),
        }

    def build_intent(self, belief: Dict[str, Any]) -> Dict[str, Any]:
        latency = belief["latency"]
        trend = belief["trend"]

        if latency > self.latency_threshold:
            return {"action": "shift_traffic_out", "magnitude": 0.1}
        if trend.startswith("+") and trend != "+0.0ms":
            return {"action": "watch_closely", "magnitude": 0.05}
        return {"action": "hold", "magnitude": 0.0}

    def classify_risk(self, belief: Dict[str, Any], intent: Dict[str, Any]) -> RiskLevel:
        confidence = belief["confidence"]
        magnitude = intent["magnitude"]

        if intent["action"] == "hold":
            return RiskLevel.LOW
        if confidence >= 0.85 and magnitude <= 0.05:
            return RiskLevel.LOW
        if confidence >= 0.7 and magnitude <= 0.1:
            return RiskLevel.MEDIUM
        if magnitude > 0.1:
            return RiskLevel.HIGH
        return RiskLevel.MEDIUM

    def format_decision_text(self, intent: Dict[str, Any], cloud: str) -> str:
        if intent["action"] == "shift_traffic_out":
            return f"shift {int(intent['magnitude'] * 100)}% traffic away from {cloud.upper()}"
        if intent["action"] == "watch_closely":
            return f"monitor {cloud.upper()} latency and keep current weights"
        return "hold current traffic distribution"

    def process_message(self, topic: str, message: Dict[str, Any]) -> bool:
        try:
            event = MetricsEvent(**message)
        except Exception as exc:
            logger.error(f"Invalid metrics event: {exc} | message={message}")
            return False

        current_latency = self._extract_latency(event.value.metrics)
        if current_latency is None:
            logger.warning("Skipping metrics event without latency_ms")
            return True

        if event.value.service != self.service_id:
            logger.debug(
                "Ignoring metrics for service '%s' (expecting '%s')",
                event.value.service,
                self.service_id,
            )
            return True

        belief = self.build_belief(current_latency)
        intent = self.build_intent(belief)
        risk = self.classify_risk(belief, intent)
        decision_text = self.format_decision_text(intent, event.value.cloud.value)

        policy = PolicyDecision(
            key=str(uuid4()),
            value=PolicyDecisionValue(
                service=event.value.service,
                decision=decision_text,
                risk_level=risk,
                status=PolicyStatus.PENDING,
                timestamp=datetime.utcnow(),
                metadata={
                    "cloud": event.value.cloud.value,
                    "belief": belief,
                    "intent": intent,
                    "source_event_key": event.key,
                },
                correlation_id=event.value.correlation_id,
                parent_event_id=event.value.parent_event_id,
            ),
        )

        result = self.producer.send("policy.decisions", policy)
        logger.info(
            "Published policy decision %s for %s@%s: %s",
            policy.key,
            event.value.service,
            event.value.cloud.value,
            decision_text,
        )
        logger.debug("belief=%s intent=%s risk=%s", belief, intent, risk)
        return True

    @staticmethod
    def _extract_latency(metrics: Dict[str, Any]) -> Optional[float]:
        latency = metrics.get("latency_ms")
        if latency is None:
            return None
        try:
            return float(latency)
        except (TypeError, ValueError):
            return None

    def close(self) -> None:
        super().close()
        if self.producer:
            self.producer.close()
