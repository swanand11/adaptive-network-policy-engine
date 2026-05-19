"""Enterprise polling backend for Nexus Policy Engine.

Architecture:
Kafka -> consumer thread -> in-memory cache -> Flask REST -> React polling
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Any, Deque, Dict, List, Optional
from uuid import uuid4

from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
try:
    from prometheus_client.parser import text_string_to_metric_families
except Exception:  # pragma: no cover
    text_string_to_metric_families = None

try:
    from kafka import KafkaConsumer, KafkaProducer
except Exception:  # pragma: no cover
    KafkaConsumer = None
    KafkaProducer = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("governance_flask_api")

TOPICS = [
    "metrics.events",
    "policy.decisions",
    "policy.approved",
    "topo.decisions",
    "service.state",
]

APPROVAL_RESULT_TOPICS = {
    "APPROVED": "governance.approved",
    "REJECTED": "governance.rejected",
}
CLOUDS = ["aws", "aks", "digitalocean"]
DISPLAY_CLOUD = {"digitalocean": "do"}
PROMETHEUS_CLOUD_ALIASES = {
    "aws": ["aws", "AWS"],
    "aks": ["aks", "AKS"],
    "digitalocean": ["digitalocean", "do", "DO"],
}
SCRAPE_TARGETS = {
    "aws": os.getenv("PROMETHEUS_AWS_URL", "http://localhost:8001/metrics"),
    "aks": os.getenv("PROMETHEUS_AKS_URL", "http://localhost:8002/metrics"),
    "digitalocean": os.getenv("PROMETHEUS_DO_URL", "http://localhost:8003/metrics"),
}


class TopicCacheManager:
    def __init__(self, max_records: int = 100):
        self.max_records = max_records
        self._lock = threading.RLock()
        self.cache: Dict[str, Deque[Dict[str, Any]]] = {
            topic: deque(maxlen=max_records) for topic in TOPICS
        }
        self.pending_approvals: Dict[str, Dict[str, Any]] = {}
        self.approval_history: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.audit_trail: Deque[Dict[str, Any]] = deque(maxlen=5000)
        self.execution_results: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.prometheus_url = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
        self._prom_series: Dict[str, Dict[str, Deque[float]]] = defaultdict(
            lambda: defaultdict(lambda: deque(maxlen=20))
        )

    def append_topic(self, topic: str, event: Dict[str, Any]) -> None:
        with self._lock:
            if topic not in self.cache:
                self.cache[topic] = deque(maxlen=self.max_records)
            self.cache[topic].appendleft(event)

    def add_pending_approval(self, approval: Dict[str, Any]) -> None:
        with self._lock:
            self.pending_approvals[approval["id"]] = approval
            self._audit("PENDING", approval["id"], "system", approval)

    def decide(self, approval_id: str, decision: str, approver: str, note: str = "") -> Optional[Dict[str, Any]]:
        with self._lock:
            item = self.pending_approvals.get(approval_id)
            if not item:
                return None
            item = dict(item)
            item["human_decision"] = decision
            item["approver"] = approver
            item["approval_time"] = datetime.now(timezone.utc).isoformat()
            item["status"] = decision
            if note:
                item["reasoning"] = f"{item.get('reasoning', '')}\nOperator note: {note}".strip()

            self.pending_approvals.pop(approval_id, None)
            self.approval_history.appendleft(item)
            self._audit(decision, approval_id, approver, item)
            return item

    def expire_old(self, ttl_minutes: int = 30) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=ttl_minutes)
        to_expire = []
        with self._lock:
            for aid, item in self.pending_approvals.items():
                ts = _parse_ts(item.get("timestamp"))
                if ts and ts < cutoff:
                    to_expire.append(aid)

            for aid in to_expire:
                item = self.pending_approvals.pop(aid)
                item = dict(item)
                item["status"] = "EXPIRED"
                item["human_decision"] = "EXPIRED"
                item["approval_time"] = datetime.now(timezone.utc).isoformat()
                self.approval_history.appendleft(item)
                self._audit("EXPIRED", aid, "system-expirer", item)

    def get_topics(self) -> Dict[str, int]:
        with self._lock:
            return {topic: len(records) for topic, records in self.cache.items()}

    def get_logs(self, topic: str, limit: int = 100) -> List[Dict[str, Any]]:
        with self._lock:
            if topic not in self.cache:
                return []
            return list(self.cache[topic])[:limit]

    def get_pending(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.pending_approvals.values())

    def get_history(self, limit: int = 200) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.approval_history)[:limit]

    def get_audit(self, limit: int = 300, search: str = "") -> List[Dict[str, Any]]:
        with self._lock:
            rows = list(self.audit_trail)
            if search:
                s = search.lower()
                rows = [r for r in rows if s in json.dumps(r).lower()]
            return rows[:limit]

    def get_metrics_summary(self) -> Dict[str, Any]:
        with self._lock:
            by_cloud = defaultdict(
                lambda: {
                    "count": 0,
                    "avg_risk": 0.0,
                    "latency_ms": 0.0,
                    "error_rate_percent": 0.0,
                    "cpu_usage_percent": 0.0,
                    "memory_usage_percent": 0.0,
                    "active_connections": 0.0,
                    "requests_per_second": 0.0,
                    "request_count_total": 0.0,
                    "series": {
                        "latency_ms": [],
                        "error_rate_percent": [],
                        "cpu_usage_percent": [],
                        "memory_usage_percent": [],
                        "active_connections": [],
                        "requests_per_second": [],
                        "request_count_total": [],
                    },
                }
            )
            for cloud in CLOUDS:
                by_cloud[cloud]["count"] = len(
                    [
                        r
                        for r in self.cache.get("metrics.events", [])
                        if (r.get("cloud") or "").lower() in {cloud, DISPLAY_CLOUD.get(cloud, cloud)}
                    ]
                )
                by_cloud[cloud]["avg_risk"] = 0.0
                values = self._live_prometheus_metrics(cloud)
                for metric_key, value in values.items():
                    by_cloud[cloud][metric_key] = round(value, 3)
                    self._prom_series[cloud][metric_key].append(value)
                    by_cloud[cloud]["series"][metric_key] = list(self._prom_series[cloud][metric_key])
            normalized = {}
            for cloud, row in dict(by_cloud).items():
                normalized[DISPLAY_CLOUD.get(cloud, cloud)] = row
            return {
                "kafka_ingestion_health": "healthy",
                "topic_depth": self.get_topics(),
                "cloud_summary": normalized,
                "pending_approvals": len(self.pending_approvals),
                "approval_stats": self.get_approval_stats(),
            }

    def _query_prometheus(self, promql: str) -> float:
        try:
            resp = requests.get(
                f"{self.prometheus_url}/api/v1/query",
                params={"query": promql},
                timeout=2.5,
            )
            data = resp.json()
            value = data.get("data", {}).get("result", [])
            if value and len(value[0].get("value", [])) > 1:
                return float(value[0]["value"][1])
        except Exception:
            logger.debug("Prometheus query failed: %s", promql, exc_info=True)
        return 0.0

    def _live_prometheus_metrics(self, cloud: str) -> Dict[str, float]:
        aliases = PROMETHEUS_CLOUD_ALIASES.get(cloud, [cloud])
        latency_ms_val = self._query_prometheus_any("latency_ms", aliases)
        values = {
            "latency_ms": latency_ms_val,
            "error_rate_percent": self._query_prometheus_any("error_rate_percent", aliases),
            "cpu_usage_percent": self._query_prometheus_any("cpu_usage_percent", aliases),
            "memory_usage_percent": self._query_prometheus_any("memory_usage_percent", aliases),
            "active_connections": self._query_prometheus_any("active_connections", aliases),
            "requests_per_second": self._query_prometheus_any("requests_per_second", aliases),
            "request_count_total": self._query_prometheus_any("request_count_total", aliases),
        }
        visible = [
            values["latency_ms"],
            values["error_rate_percent"],
            values["cpu_usage_percent"],
            values["memory_usage_percent"],
        ]
        if any(value > 0 for value in visible):
            return values
        return self._scrape_mock_metrics(cloud)

    def _query_prometheus_any(self, metric: str, cloud_aliases: List[str]) -> float:
        for alias in cloud_aliases:
            value = self._query_prometheus(f'{metric}{{cloud="{alias}"}}')
            if value != 0:
                return value
        return 0.0

    def _scrape_mock_metrics(self, cloud: str) -> Dict[str, float]:
        values = {
            "latency_ms": 0.0,
            "error_rate_percent": 0.0,
            "cpu_usage_percent": 0.0,
            "memory_usage_percent": 0.0,
            "active_connections": 0.0,
            "requests_per_second": 0.0,
            "request_count_total": 0.0,
        }
        if text_string_to_metric_families is None:
            return values
        try:
            resp = requests.get(SCRAPE_TARGETS[cloud], timeout=2.5)
            families = list(text_string_to_metric_families(resp.text))
            samples = {sample.name: float(sample.value) for family in families for sample in family.samples}
            values["latency_ms"] = samples.get("latency_ms", 0.0)
            values["error_rate_percent"] = samples.get("error_rate_percent", 0.0)
            values["cpu_usage_percent"] = samples.get("cpu_usage_percent", 0.0)
            values["memory_usage_percent"] = samples.get("memory_usage_percent", 0.0)
            values["active_connections"] = samples.get("active_connections", 0.0)
            values["requests_per_second"] = samples.get("requests_per_second", 0.0)
            values["request_count_total"] = samples.get("request_count_total", 0.0)
        except Exception:
            logger.debug("Direct scrape fallback failed for %s", cloud, exc_info=True)
        return values

    def add_execution_result(self, event: Dict[str, Any]) -> None:
        with self._lock:
            result = {
                "id": str(uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "EXECUTED",
                "details": event,
            }
            self.execution_results.appendleft(result)
            self._audit("EXECUTED", result["id"], "execution-engine", result)

    def get_approval_stats(self) -> Dict[str, int]:
        with self._lock:
            approved = sum(1 for r in self.approval_history if r.get("status") == "APPROVED")
            rejected = sum(1 for r in self.approval_history if r.get("status") == "REJECTED")
            expired = sum(1 for r in self.approval_history if r.get("status") == "EXPIRED")
            executed = len(self.execution_results)
            pending = len(self.pending_approvals)
            return {
                "PENDING": pending,
                "APPROVED": approved,
                "REJECTED": rejected,
                "EXPIRED": expired,
                "EXECUTED": executed,
            }

    def _audit(self, action: str, entity_id: str, actor: str, payload: Dict[str, Any]) -> None:
        self.audit_trail.appendleft(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": action,
                "entity_id": entity_id,
                "actor": actor,
                "payload": payload,
            }
        )


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def normalize_event(topic: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    return {
        "timestamp": payload.get("timestamp") or datetime.now(timezone.utc).isoformat(),
        "topic": topic,
        "action": payload.get("decision") or metadata.get("action_type") or "observe",
        "service": payload.get("service") or metadata.get("service") or "unknown",
        "cloud": payload.get("cloud") or metadata.get("cloud") or "unknown",
        "severity": payload.get("risk_level") or metadata.get("severity") or "low",
        "risk_score": metadata.get("risk_score") or payload.get("risk_score") or 0,
        "payload": payload,
        "status": payload.get("status") or metadata.get("status") or "new",
    }


class KafkaPollingConsumer(threading.Thread):
    daemon = True

    def __init__(self, cache: TopicCacheManager, bootstrap_servers: str):
        super().__init__(name="GovernanceKafkaConsumer")
        self.cache = cache
        self.bootstrap_servers = bootstrap_servers
        self._stop_event = threading.Event()

    def run(self) -> None:
        if KafkaConsumer is None:
            logger.error("kafka-python not available")
            return

        while not self._stop_event.is_set():
            consumer = None
            try:
                consumer = KafkaConsumer(
                    *TOPICS,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id="governance_flask_consumer",
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else {},
                    consumer_timeout_ms=1500,
                )
                logger.info("Kafka consumer connected: %s", TOPICS)

                while not self._stop_event.is_set():
                    batch = consumer.poll(timeout_ms=500)
                    for tp, records in batch.items():
                        for rec in records:
                            payload = rec.value if isinstance(rec.value, dict) else {}
                            event = normalize_event(tp.topic, payload)
                            self.cache.append_topic(tp.topic, event)
                            self._maybe_enqueue_highrisk(event)
                            if tp.topic == "policy.approved":
                                self.cache.add_execution_result(event)
                    self.cache.expire_old()

            except Exception:
                logger.exception("Kafka consumer loop failure; retrying in 2s")
                time.sleep(2)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception:
                        pass

    def stop(self) -> None:
        self._stop_event.set()

    def _maybe_enqueue_highrisk(self, event: Dict[str, Any]) -> None:
        sev = str(event.get("severity", "")).lower()
        action = str(event.get("action", "")).lower()
        high_risk_keywords = [
            "delete", "rotate", "shutdown", "failover", "override",
            "escalation", "firewall", "public", "credential", "iam",
        ]
        risk_score = float(event.get("risk_score") or 0)
        is_high = sev in {"high", "critical"} or risk_score >= 0.8 or any(k in action for k in high_risk_keywords)
        if not is_high:
            return

        approval_id = str(uuid4())
        self.cache.add_pending_approval(
            {
                "id": approval_id,
                "timestamp": event["timestamp"],
                "requested_by": event.get("service", "ai-governor"),
                "action_type": event.get("action", "unknown"),
                "cloud_provider": event.get("cloud", "unknown"),
                "target_resource": event.get("payload", {}).get("target_resource", "unknown"),
                "severity": event.get("severity", "high").upper(),
                "risk_score": event.get("risk_score", 0),
                "reasoning": event.get("payload", {}).get("decision", "High-risk governance action detected"),
                "ai_recommendation": "Human approval required",
                "human_decision": "PENDING",
                "approver": None,
                "approval_time": None,
                "status": "PENDING",
                "payload": event.get("payload", {}),
                "decision": {
                    "service": event.get("service", "unknown"),
                    "decision": event.get("payload", {}).get("decision") or event.get("action", "unknown"),
                    "risk_level": str(event.get("severity", "high")).lower(),
                    "status": "pending",
                    "metadata": event.get("payload", {}).get("metadata") or {},
                }
            }
        )


class GovernanceProducer:
    def __init__(self, bootstrap_servers: str):
        self.bootstrap_servers = bootstrap_servers
        self._producer = None
        if KafkaProducer is not None:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
            except Exception:
                logger.exception("Failed to initialize KafkaProducer")

    def publish_decision(self, decision: str, item: Dict[str, Any]) -> None:
        topic = APPROVAL_RESULT_TOPICS.get(decision)
        if not topic or self._producer is None:
            return
        try:
            # Publish legacy format to governance.approved
            self._producer.send(topic, item)
            
            if decision == "APPROVED":
                # Extract original decision value from payload
                orig_payload = item.get("payload") or {}
                if orig_payload:
                    # Update status to APPROVED and add approval metadata
                    orig_payload = dict(orig_payload)
                    orig_payload["status"] = "APPROVED"
                    if "metadata" not in orig_payload:
                        orig_payload["metadata"] = {}
                    orig_payload["metadata"] = dict(orig_payload["metadata"])
                    orig_payload["metadata"]["approved_by"] = item.get("approver", "operator")
                    orig_payload["metadata"]["approval_note"] = item.get("reasoning", "")
                    orig_payload["metadata"]["approved_at"] = item.get("approval_time")
                    
                    # Wrap in PolicyDecision structure (key/value) for standard consumers
                    decision_id = orig_payload.get("decision_id") or item.get("id") or "unknown"
                    wrapped = {
                        "key": decision_id,
                        "value": orig_payload
                    }
                    
                    logger.info("Publishing approved decision to policy.approved and policy.decisions: %s", decision_id)
                    self._producer.send("policy.approved", wrapped)
                    self._producer.send("policy.decisions", wrapped)
            
            self._producer.flush(timeout=2)
        except Exception:
            logger.exception("Failed publishing governance decision to %s", topic)


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    cache = TopicCacheManager(max_records=100)
    producer = GovernanceProducer(bootstrap)
    consumer = KafkaPollingConsumer(cache, bootstrap)
    consumer.start()

    @app.get("/health")
    def health():
        return jsonify({"status": "healthy", "service": "governance-flask-api"})

    @app.get("/api/metrics")
    def metrics():
        return jsonify(cache.get_metrics_summary())

    @app.get("/api/highrisk/stats")
    def highrisk_stats():
        return jsonify(cache.get_approval_stats())

    @app.get("/api/highrisk/pending")
    def highrisk_pending():
        rows = cache.get_pending()
        return jsonify({"count": len(rows), "requests": rows})

    @app.get("/api/highrisk/history")
    def highrisk_history():
        limit = int(request.args.get("limit", 200))
        rows = cache.get_history(limit)
        return jsonify({"count": len(rows), "requests": rows})

    @app.get("/api/audit")
    def audit():
        limit = int(request.args.get("limit", 300))
        search = request.args.get("search", "")
        rows = cache.get_audit(limit, search)
        return jsonify({"count": len(rows), "entries": rows})

    @app.post("/api/highrisk/approve/<approval_id>")
    def approve(approval_id: str):
        body = request.get_json(silent=True) or {}
        approver = body.get("approver", "operator")
        note = body.get("note", "")
        item = cache.decide(approval_id, "APPROVED", approver, note)
        if item is None:
            return jsonify({"error": "not_found"}), 404
        producer.publish_decision("APPROVED", item)
        return jsonify({"status": "APPROVED", "id": approval_id, "item": item})

    @app.post("/api/highrisk/reject/<approval_id>")
    def reject(approval_id: str):
        body = request.get_json(silent=True) or {}
        approver = body.get("approver", "operator")
        note = body.get("note", "")
        item = cache.decide(approval_id, "REJECTED", approver, note)
        if item is None:
            return jsonify({"error": "not_found"}), 404
        producer.publish_decision("REJECTED", item)
        return jsonify({"status": "REJECTED", "id": approval_id, "item": item})

    @app.teardown_appcontext
    def _shutdown(_exc):
        pass

    app.consumer_thread = consumer  # type: ignore[attr-defined]
    return app


if __name__ == "__main__":
    app = create_app()
    port = int(os.getenv("BACKEND_PORT", "5000"))
    host = os.getenv("BACKEND_HOST", "0.0.0.0")
    app.run(host=host, port=port, debug=False, use_reloader=False)
