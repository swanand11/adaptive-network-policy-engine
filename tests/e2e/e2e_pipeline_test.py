from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import subprocess
import time
import types
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

if not os.getenv("PIPELINE_TEST_DOCKERIZED"):
    docker_command = [
        "docker",
        "run",
        "--rm",
        "--network",
        "container:kafka_1",
        "-e",
        "PIPELINE_TEST_DOCKERIZED=1",
        "-e",
        "KAFKA_BOOTSTRAP_SERVERS=kafka:9092",
        "-v",
        f"{REPO_ROOT}:/work",
        "-w",
        "/work",
        "python:3.11-slim",
        "sh",
        "-lc",
        "python -m pip install --quiet --no-cache-dir -r requirements.txt && python tests/e2e/e2e_pipeline_test.py "
        + " ".join(json.dumps(arg) for arg in sys.argv[1:]),
    ]
    raise SystemExit(subprocess.run(docker_command).returncode)

try:
    import six  # type: ignore
    from six import moves as six_moves  # type: ignore
except Exception:  # pragma: no cover - only used as a compatibility shim
    six = None
    six_moves = None
else:
    if six_moves is None:
        six_moves = types.ModuleType("kafka.vendor.six.moves")
        six_moves.range = range  # type: ignore[attr-defined]
    sys.modules.setdefault("kafka.vendor.six", six)
    sys.modules.setdefault("kafka.vendor.six.moves", six_moves)

from kafka import KafkaConsumer
from kafka.structs import TopicPartition

from kafka_core.config import KafkaConfig
from kafka_core.enums import CloudProvider
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.schemas import MetricsEvent, MetricsEventValue, ServiceStateValue
from kafka_core.topic_initializer import TopicInitializer


logger = logging.getLogger(__name__)


SERVICE_MAPPING = [
    {"partition": 0, "service": "service-db", "cloud": CloudProvider.AZURE, "latency_ms": 220.0},
    {"partition": 1, "service": "service-cache-aws", "cloud": CloudProvider.AWS, "latency_ms": 42.0},
    {"partition": 2, "service": "service-cache", "cloud": CloudProvider.DIGITALOCEAN, "latency_ms": 115.0},
]


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class StageResult:
    name: str
    passed: bool
    checks: List[CheckResult] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TestReport:
    started_at: str
    bootstrap_servers: List[str]
    run_id: str
    stages: List[StageResult] = field(default_factory=list)
    failures: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def passed(self) -> bool:
        return not self.failures and all(stage.passed for stage in self.stages)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "started_at": self.started_at,
            "bootstrap_servers": self.bootstrap_servers,
            "run_id": self.run_id,
            "duration_seconds": round(self.duration_seconds, 3),
            "passed": self.passed,
            "stages": [
                {
                    "name": stage.name,
                    "passed": stage.passed,
                    "checks": [asdict(check) for check in stage.checks],
                    "details": stage.details,
                }
                for stage in self.stages
            ],
            "failures": self.failures,
        }


class Reporter:
    def __init__(self, report_dir: Path, run_id: str):
        self.report_dir = report_dir
        self.run_id = run_id
        self.report_dir.mkdir(parents=True, exist_ok=True)

    def write(self, report: TestReport) -> Dict[str, Path]:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        json_path = self.report_dir / f"pipeline_test_{timestamp}_{self.run_id}.json"
        html_path = self.report_dir / f"pipeline_test_{timestamp}_{self.run_id}.html"

        payload = report.to_dict()
        json_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        html_path.write_text(self._render_html(payload), encoding="utf-8")
        return {"json": json_path, "html": html_path}

    def _render_html(self, payload: Dict[str, Any]) -> str:
        stages_html = []
        for stage in payload["stages"]:
            status = "PASSED" if stage["passed"] else "FAILED"
            checks_html = "".join(
                f"<li><strong>{self._escape(check['name'])}</strong>: {('PASS' if check['passed'] else 'FAIL')}"
                f"{(' - ' + self._escape(check['error'])) if check.get('error') else ''}"
                f"{(' <pre>' + self._escape(json.dumps(check.get('details', {}), indent=2, default=str)) + '</pre>') if check.get('details') else ''}"
                f"</li>"
                for check in stage["checks"]
            )
            details = self._escape(json.dumps(stage.get("details", {}), indent=2, default=str))
            stages_html.append(
                f"<section class='stage {status.lower()}'>"
                f"<h2>{self._escape(stage['name'])} - {status}</h2>"
                f"<ul>{checks_html}</ul>"
                f"<pre>{details}</pre>"
                f"</section>"
            )

        failures = "".join(f"<li>{self._escape(failure)}</li>" for failure in payload["failures"])
        return f"""<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <title>Pipeline Test Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; background: #f7f7f9; color: #1d1d1f; }}
    h1 {{ margin-bottom: 0.2rem; }}
    .meta {{ color: #555; margin-bottom: 1rem; }}
    .stage {{ background: #fff; border-radius: 10px; padding: 16px 18px; margin-bottom: 14px; box-shadow: 0 1px 3px rgba(0,0,0,0.08); }}
    .passed {{ border-left: 6px solid #1f8f4a; }}
    .failed {{ border-left: 6px solid #c0392b; }}
    ul {{ margin-top: 0.5rem; }}
    pre {{ background: #f0f2f5; padding: 12px; border-radius: 8px; overflow: auto; }}
    .summary {{ background: #fff; padding: 16px 18px; border-radius: 10px; margin-bottom: 16px; }}
  </style>
</head>
<body>
  <h1>Pipeline Test Report</h1>
  <div class='meta'>Run ID: {self._escape(payload['run_id'])} | Started: {self._escape(payload['started_at'])} | Duration: {payload['duration_seconds']}s | Result: {('PASSED' if payload['passed'] else 'FAILED')}</div>
  <div class='summary'>
    <h2>Failures</h2>
    <ul>{failures or '<li>None</li>'}</ul>
  </div>
  {''.join(stages_html)}
</body>
</html>"""

    @staticmethod
    def _escape(text: str) -> str:
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def topic_partition_count(topic_name: str) -> Optional[int]:
    with TopicInitializer() as initializer:
        info = initializer.get_topic_info(topic_name)
    if not info:
        return None
    partitions = info.get("partitions", []) if isinstance(info, dict) else []
    return len(partitions)


def assert_topic_ready(topic_name: str, expected_partitions: Optional[int] = None) -> CheckResult:
    try:
        count = topic_partition_count(topic_name)
        if count is None:
            return CheckResult(name=f"{topic_name} exists", passed=False, error="topic metadata unavailable")
        details = {"partitions": count}
        if expected_partitions is not None:
            details["expected_partitions"] = expected_partitions
            if count != expected_partitions:
                return CheckResult(
                    name=f"{topic_name} partition count",
                    passed=False,
                    details=details,
                    error=f"expected {expected_partitions}, found {count}",
                )
        return CheckResult(name=f"{topic_name} ready", passed=True, details=details)
    except Exception as exc:
        return CheckResult(name=f"{topic_name} ready", passed=False, error=str(exc))


def assert_broker_reachable() -> CheckResult:
    try:
        initializer = TopicInitializer()
        initializer.close()
        return CheckResult(name="Broker reachable", passed=True, details={"bootstrap_servers": KafkaConfig.BOOTSTRAP_SERVERS})
    except Exception as exc:
        return CheckResult(name="Broker reachable", passed=False, error=str(exc), details={"bootstrap_servers": KafkaConfig.BOOTSTRAP_SERVERS})


def make_metrics_event(service: str, cloud: CloudProvider, latency_ms: float, run_id: str, partition: int) -> MetricsEvent:
    value = MetricsEventValue(
        service=service,
        cloud=cloud,
        timestamp=datetime.now(timezone.utc),
        metrics={
            "latency_ms": latency_ms,
            "request_latency_ms": latency_ms,
            "request_count": 1,
            "error_rate_percent": 1.0 if latency_ms > 200 else 0.0,
            "cpu_percent": 55.0 + partition * 5,
            "memory_percent": 60.0 + partition * 3,
        },
        correlation_id=run_id,
        parent_event_id=f"stage2-{run_id}",
    )
    return MetricsEvent(key=f"{service}@{cloud.value}", value=value)


def capture_end_offsets(topic_name: str, partition_count: int) -> Dict[int, int]:
    config = KafkaConfig.get_consumer_config(group_id=f"pipeline_probe_{uuid4().hex}")
    config["enable_auto_commit"] = False
    consumer = KafkaConsumer(
        **config,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
    )
    partitions = [TopicPartition(topic_name, index) for index in range(partition_count)]
    consumer.assign(partitions)
    consumer.poll(timeout_ms=250)
    end_offsets = consumer.end_offsets(partitions)
    offsets = {partition.partition: offset for partition, offset in end_offsets.items()}
    consumer.close()
    return offsets


def consume_matching_messages(
    topic_name: str,
    partition_count: int,
    start_offsets: Dict[int, int],
    timeout_seconds: float,
    match_fn: Callable[[Dict[str, Any]], bool],
) -> List[Dict[str, Any]]:
    config = KafkaConfig.get_consumer_config(group_id=f"pipeline_probe_{uuid4().hex}")
    config["enable_auto_commit"] = False
    consumer = KafkaConsumer(
        **config,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        key_deserializer=lambda m: m.decode("utf-8") if m else None,
    )
    partitions = [TopicPartition(topic_name, index) for index in range(partition_count)]
    consumer.assign(partitions)
    consumer.poll(timeout_ms=250)

    for partition in partitions:
        consumer.seek(partition, start_offsets.get(partition.partition, 0))

    deadline = time.monotonic() + timeout_seconds
    matches: List[Dict[str, Any]] = []

    try:
        while time.monotonic() < deadline:
            records = consumer.poll(timeout_ms=500, max_records=100)
            if not records:
                continue
            for _, topic_records in records.items():
                for record in topic_records:
                    payload = record.value or {}
                    if match_fn(payload):
                        matches.append(
                            {
                                "topic": record.topic,
                                "partition": record.partition,
                                "offset": record.offset,
                                "key": record.key,
                                "value": payload,
                            }
                        )
            if matches:
                break
    finally:
        consumer.close()

    return matches


def stage_1_readiness() -> StageResult:
    checks = [
        assert_broker_reachable(),
        assert_topic_ready("metrics.events", expected_partitions=3),
        assert_topic_ready("service.state"),
    ]

    passed = all(check.passed for check in checks)
    details = {
        "metrics_events_partitions": topic_partition_count("metrics.events"),
        "service_state_partitions": topic_partition_count("service.state"),
    }
    return StageResult(name="Stage 1 - Kafka Readiness", passed=passed, checks=checks, details=details)


def stage_2_send_events(producer: KafkaProducerTemplate, run_id: str) -> StageResult:
    checks: List[CheckResult] = []
    sent: List[Dict[str, Any]] = []

    for item in SERVICE_MAPPING:
        event = make_metrics_event(
            service=item["service"],
            cloud=item["cloud"],
            latency_ms=item["latency_ms"],
            run_id=run_id,
            partition=item["partition"],
        )
        try:
            record = producer.send("metrics.events", event, partition=item["partition"])
            checks.append(
                CheckResult(
                    name=f"Send partition {item['partition']}",
                    passed=True,
                    details={"record": record, "service": item["service"], "cloud": item["cloud"].value},
                )
            )
            sent.append(
                {
                    "partition": item["partition"],
                    "service": item["service"],
                    "cloud": item["cloud"].value,
                    "latency_ms": item["latency_ms"],
                    "record": record,
                }
            )
        except Exception as exc:
            checks.append(
                CheckResult(
                    name=f"Send partition {item['partition']}",
                    passed=False,
                    details={"service": item["service"], "cloud": item["cloud"].value},
                    error=str(exc),
                )
            )

    passed = all(check.passed for check in checks)
    return StageResult(name="Stage 2 - Send Metrics Events", passed=passed, checks=checks, details={"sent": sent})


def stage_3_verify_partition_routing(run_id: str, start_offsets: Dict[int, int]) -> StageResult:
    partition_count = 3
    checks: List[CheckResult] = []
    verified: List[Dict[str, Any]] = []

    expected_by_partition = {item["partition"]: item for item in SERVICE_MAPPING}
    for partition, expected in expected_by_partition.items():
        matches = consume_matching_messages(
            topic_name="metrics.events",
            partition_count=partition_count,
            start_offsets=start_offsets,
            timeout_seconds=8.0,
            match_fn=lambda payload, expected=expected, run_id=run_id: payload.get("correlation_id") == run_id and payload.get("service") == expected["service"],
        )
        passed = bool(matches)
        checks.append(
            CheckResult(
                name=f"Partition {partition}",
                passed=passed,
                details={"expected_service": expected["service"], "matches": len(matches)},
                error=None if passed else "expected event not found",
            )
        )
        if matches:
            verified.append({"partition": partition, "service": expected["service"], "offset": matches[0]["offset"]})

    return StageResult(name="Stage 3 - Partition Routing", passed=all(check.passed for check in checks), checks=checks, details={"verified": verified})


def stage_4_verify_service_state(run_id: str, start_offsets: Dict[int, int]) -> StageResult:
    partition_count = 2
    checks: List[CheckResult] = []
    service_hits: Dict[str, List[Dict[str, Any]]] = {item["service"]: [] for item in SERVICE_MAPPING}

    matches = consume_matching_messages(
        topic_name="service.state",
        partition_count=partition_count,
        start_offsets=start_offsets,
        timeout_seconds=12.0,
        match_fn=lambda payload: payload.get("correlation_id") == run_id,
    )

    for match in matches:
        try:
            parsed = ServiceStateValue(**match["value"])
            service_hits.setdefault(parsed.service, []).append(
                {
                    "partition": match["partition"],
                    "offset": match["offset"],
                    "cloud": parsed.cloud.value if hasattr(parsed.cloud, "value") else str(parsed.cloud),
                    "status": parsed.belief.status,
                    "confidence": parsed.belief.confidence,
                    "optimal_load": parsed.intent.optimal_load,
                }
            )
        except Exception as exc:
            checks.append(CheckResult(name="Parse service.state payload", passed=False, error=str(exc)))

    for item in SERVICE_MAPPING:
        hits = service_hits.get(item["service"], [])
        checks.append(
            CheckResult(
                name=f"service.state for {item['service']}",
                passed=bool(hits),
                details={"messages": hits},
                error=None if hits else "no published service.state message found",
            )
        )

    details = {service: len(messages) for service, messages in service_hits.items()}
    return StageResult(name="Stage 4 - Service State Publishing", passed=all(check.passed for check in checks), checks=checks, details=details)


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage-by-stage end-to-end Kafka pipeline test")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--report-dir", default="tests/reports")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    setup_logging(args.verbose)

    bootstrap_servers = [server.strip() for server in args.bootstrap_servers.split(",") if server.strip()]
    KafkaConfig.BOOTSTRAP_SERVERS = bootstrap_servers
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ",".join(bootstrap_servers)

    run_id = uuid4().hex[:12]
    report = TestReport(
        started_at=datetime.now(timezone.utc).isoformat(),
        bootstrap_servers=bootstrap_servers,
        run_id=run_id,
    )

    start_time = time.monotonic()
    reporter = Reporter(report_dir=Path(args.report_dir), run_id=run_id)
    producer = None

    try:
        logger.info("Starting pipeline test run_id=%s", run_id)

        stage1 = stage_1_readiness()
        report.stages.append(stage1)
        if not stage1.passed:
            report.failures.append("Stage 1 failed")
            return finalize(report, reporter, start_time)

        metrics_offsets = capture_end_offsets("metrics.events", 3)
        service_state_offsets = capture_end_offsets("service.state", 2)

        producer = KafkaProducerTemplate()
        stage2 = stage_2_send_events(producer, run_id)
        report.stages.append(stage2)
        if not stage2.passed:
            report.failures.append("Stage 2 failed")

        stage3 = stage_3_verify_partition_routing(run_id, metrics_offsets)
        report.stages.append(stage3)
        if not stage3.passed:
            report.failures.append("Stage 3 failed")

        stage4 = stage_4_verify_service_state(run_id, service_state_offsets)
        report.stages.append(stage4)
        if not stage4.passed:
            report.failures.append("Stage 4 failed")

        return finalize(report, reporter, start_time)
    except Exception as exc:
        logger.exception("Unexpected failure in pipeline test: %s", exc)
        report.failures.append(str(exc))
        return finalize(report, reporter, start_time)
    finally:
        if producer is not None:
            producer.close()


def finalize(report: TestReport, reporter: Reporter, start_time: float) -> int:
    report.duration_seconds = time.monotonic() - start_time
    paths = reporter.write(report)

    print("\n" + "=" * 72)
    print("PIPELINE TEST SUMMARY")
    print("=" * 72)
    for stage in report.stages:
        print(f"{'PASS' if stage.passed else 'FAIL'} - {stage.name}")
        for check in stage.checks:
            status = "PASS" if check.passed else "FAIL"
            tail = f" | {check.error}" if check.error else ""
            print(f"  {status} - {check.name}{tail}")
    print("-" * 72)
    print(f"Run ID: {report.run_id}")
    print(f"Duration: {report.duration_seconds:.2f}s")
    print(f"Overall: {'PASS' if report.passed else 'FAIL'}")
    print(f"JSON report: {paths['json']}")
    print(f"HTML report: {paths['html']}")
    print("=" * 72)

    return 0 if report.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
