"""Live pipeline runner with Kafka hop visualizer.

Flow:
  /metrics -> metrics.events -> service.state -> topo.decisions

Run after Kafka and the mock services are up:
  python -m runners.pipeline_live_runner --bootstrap-servers localhost:9092

Open:
  http://127.0.0.1:8088
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

import requests
from flask import Flask, Response, jsonify, render_template_string
from kafka import KafkaConsumer
from kafka.structs import TopicPartition

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from agents.aks.topo import TopographyAgent as AKSTopo
from agents.aws.topo import TopographyAgent as AWSTopo
from agents.do.topo import TopographyAgent as DOTopo
from agents.service_agent import ServiceAgent
from kafka_core.config import KafkaConfig
from kafka_core.pipeline import PIPELINE_TOPICS, iter_services, service_key
from kafka_core.producer_base import KafkaProducerTemplate
from kafka_core.prometheus_kafka_adaptar import (
    MetricsNormalizer,
    PrometheusParser,
    PrometheusPoller,
)
from kafka_core.schemas import MetricsEvent, MetricsEventValue
from kafka_core.topic_initializer import TopicInitializer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("pipeline_live_runner")


class EventHub:
    def __init__(self, max_events: int = 300):
        self.max_events = max_events
        self.events: List[Dict[str, Any]] = []
        self.lock = threading.Lock()
        self.subscribers: List[queue.Queue] = []

    def publish(self, event: Dict[str, Any]) -> None:
        event.setdefault("seen_at", datetime.now(timezone.utc).isoformat())
        with self.lock:
            self.events.append(event)
            self.events = self.events[-self.max_events :]
            subscribers = list(self.subscribers)
        for subscriber in subscribers:
            try:
                subscriber.put_nowait(event)
            except queue.Full:
                pass

    def snapshot(self) -> List[Dict[str, Any]]:
        with self.lock:
            return list(self.events)

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=100)
        with self.lock:
            self.subscribers.append(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self.lock:
            if q in self.subscribers:
                self.subscribers.remove(q)


class ManagedThread(threading.Thread):
    def __init__(self, name: str, target, *args):
        super().__init__(name=name, daemon=True)
        self._target = target
        self._args = args
        self.exc: Optional[BaseException] = None

    def run(self) -> None:
        try:
            self._target(*self._args)
        except BaseException as exc:
            self.exc = exc
            logger.exception("%s crashed: %s", self.name, exc)


class AgentThread(threading.Thread):
    def __init__(self, agent, name: str):
        super().__init__(name=name, daemon=True)
        self.agent = agent
        self.exc: Optional[BaseException] = None

    def run(self) -> None:
        try:
            self.agent.start()
        except BaseException as exc:
            self.exc = exc
            logger.exception("%s crashed: %s", self.name, exc)

    def stop(self) -> None:
        try:
            self.agent.close()
        except Exception:
            pass


class AdapterLoop:
    def __init__(self, producer: KafkaProducerTemplate, hub: EventHub, interval: float):
        self.producer = producer
        self.hub = hub
        self.interval = interval
        self.poller = PrometheusPoller()
        self.running = True

    def stop(self) -> None:
        self.running = False
        self.poller.session.close()

    def run(self) -> None:
        cycle = 0
        while self.running:
            cycle += 1
            cycle_id = uuid4().hex[:12]
            cycle_started = time.monotonic()
            for svc in iter_services():
                self._poll_one(cycle, cycle_id, svc)
            elapsed = time.monotonic() - cycle_started
            time.sleep(max(0.0, self.interval - elapsed))

    def _poll_one(self, cycle: int, cycle_id: str, svc) -> None:
        started = time.monotonic()
        dump = self.poller.poll(svc.metrics_url)
        if not dump:
            self.hub.publish(
                {
                    "stage": "adapter.error",
                    "service": svc.service_id,
                    "partition": svc.partition,
                    "cycle": cycle,
                    "correlation_id": cycle_id,
                    "error": f"empty poll from {svc.metrics_url}",
                }
            )
            return

        parsed = PrometheusParser.parse_dump(dump)
        metrics = MetricsNormalizer.normalize(parsed)
        event = MetricsEvent(
            key=svc.key,
            value=MetricsEventValue(
                service=svc.service_id,
                cloud=svc.cloud,
                timestamp=datetime.now(timezone.utc),
                metrics=metrics,
                correlation_id=cycle_id,
                parent_event_id=f"cycle-{cycle}",
            ),
        )
        metadata = self.producer.send(
            "metrics.events",
            event,
            key=svc.key,
            partition=svc.partition,
        )
        self.hub.publish(
            {
                "stage": "adapter.sent",
                "topic": "metrics.events",
                "service": svc.service_id,
                "cloud": svc.cloud.value,
                "partition": svc.partition,
                "cycle": cycle,
                "correlation_id": cycle_id,
                "latency_ms": round((time.monotonic() - started) * 1000, 1),
                "metadata": metadata,
            }
        )


class KafkaObserver(threading.Thread):
    def __init__(
        self,
        topic: str,
        partitions: int,
        hub: EventHub,
        from_beginning: bool = False,
    ):
        super().__init__(name=f"Observe-{topic}", daemon=True)
        self.topic = topic
        self.partitions = partitions
        self.hub = hub
        self.from_beginning = from_beginning
        self.consumer: Optional[KafkaConsumer] = None
        self.running = True

    def run(self) -> None:
        cfg = KafkaConfig.get_consumer_config(group_id=f"viz_{self.topic}_{uuid4().hex[:8]}")
        cfg["enable_auto_commit"] = False
        consumer = KafkaConsumer(
            **cfg,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
            key_deserializer=lambda m: m.decode("utf-8") if m else None,
        )
        self.consumer = consumer
        topic_partitions = [TopicPartition(self.topic, p) for p in range(self.partitions)]
        consumer.assign(topic_partitions)
        consumer.poll(timeout_ms=300)
        if self.from_beginning:
            consumer.seek_to_beginning(*topic_partitions)
        else:
            consumer.seek_to_end(*topic_partitions)

        try:
            while self.running:
                records = consumer.poll(timeout_ms=500, max_records=100)
                for _, recs in records.items():
                    for rec in recs:
                        value = rec.value or {}
                        self.hub.publish(
                            {
                                "stage": "kafka.observed",
                                "topic": rec.topic,
                                "partition": rec.partition,
                                "offset": rec.offset,
                                "key": rec.key,
                                "service": value.get("service"),
                                "cloud": value.get("cloud"),
                                "correlation_id": value.get("correlation_id"),
                                "parent_event_id": value.get("parent_event_id"),
                                "summary": summarize_payload(rec.topic, value),
                            }
                        )
        finally:
            consumer.close()

    def stop(self) -> None:
        self.running = False
        if self.consumer:
            self.consumer.close()


def summarize_payload(topic: str, value: Dict[str, Any]) -> Dict[str, Any]:
    if topic == "metrics.events":
        metrics = value.get("metrics", {})
        return {
            "latency_ms": metrics.get("request_latency_ms") or metrics.get("latency_ms"),
            "cpu": metrics.get("cpu_usage_percent"),
            "errors": metrics.get("error_rate_percent"),
        }
    if topic == "service.state":
        return {
            "status": (value.get("belief") or {}).get("status"),
            "current_load": (value.get("intent") or {}).get("current_load"),
            "optimal_load": (value.get("intent") or {}).get("optimal_load"),
            "confidence": (value.get("belief") or {}).get("confidence"),
        }
    if topic == "topo.decisions":
        return {
            "actions": len(value.get("actions") or []),
            "risk": value.get("risk_level"),
            "status": value.get("status"),
        }
    return {}


def wait_for_mocks(timeout_s: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_s
    pending = {svc.name: svc.metrics_url for svc in iter_services()}
    while pending and time.monotonic() < deadline:
        for name, url in list(pending.items()):
            try:
                if requests.get(url, timeout=1.0).status_code == 200:
                    del pending[name]
            except requests.RequestException:
                pass
        if pending:
            time.sleep(0.3)
    if pending:
        names = ", ".join(sorted(pending))
        raise RuntimeError(f"Mock /metrics endpoints not ready: {names}")


HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Pipeline Visualizer</title>
  <style>
    :root { color-scheme: light; --ink:#17202a; --muted:#65717d; --line:#d9dee4; --bg:#f6f7f9; --ok:#13795b; --warn:#9a6700; --bad:#b42318; --panel:#ffffff; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif; color: var(--ink); background: var(--bg); }
    header { padding: 18px 22px; border-bottom: 1px solid var(--line); background: var(--panel); display:flex; justify-content:space-between; align-items:center; gap:16px; }
    h1 { margin:0; font-size:20px; font-weight:700; }
    .sub { color:var(--muted); font-size:13px; }
    main { padding: 18px 22px; display:grid; gap:16px; }
    .flow { display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; }
    .stage { background:var(--panel); border:1px solid var(--line); border-radius:8px; min-height:180px; overflow:hidden; }
    .stage h2 { margin:0; padding:12px 14px; font-size:14px; border-bottom:1px solid var(--line); display:flex; justify-content:space-between; }
    .count { color:var(--muted); font-weight:500; }
    .rows { display:grid; }
    .row { padding:10px 14px; border-bottom:1px solid #edf0f3; display:grid; grid-template-columns: 72px 1fr 68px; gap:10px; align-items:center; font-size:13px; }
    .pill { display:inline-flex; justify-content:center; border:1px solid var(--line); border-radius:999px; padding:2px 8px; font-size:12px; background:#fafbfc; }
    .svc { font-weight:650; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .meta { color:var(--muted); overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .timeline { background:var(--panel); border:1px solid var(--line); border-radius:8px; overflow:hidden; }
    .timeline h2 { margin:0; padding:12px 14px; font-size:14px; border-bottom:1px solid var(--line); }
    table { width:100%; border-collapse:collapse; font-size:13px; }
    th, td { text-align:left; padding:9px 10px; border-bottom:1px solid #edf0f3; vertical-align:top; }
    th { color:var(--muted); font-weight:600; background:#fbfcfd; }
    td { max-width:280px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
    .status { font-size:13px; color:var(--ok); }
    @media (max-width: 900px) { .flow { grid-template-columns:1fr; } header { align-items:flex-start; flex-direction:column; } }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Adaptive Network Pipeline</h1>
      <div class="sub">/metrics -> metrics.events -> service.state -> topo.decisions</div>
    </div>
    <div class="status" id="status">connecting</div>
  </header>
  <main>
    <section class="flow">
      <div class="stage"><h2>metrics.events <span class="count" id="count-metrics.events">0</span></h2><div class="rows" id="stage-metrics.events"></div></div>
      <div class="stage"><h2>service.state <span class="count" id="count-service.state">0</span></h2><div class="rows" id="stage-service.state"></div></div>
      <div class="stage"><h2>topo.decisions <span class="count" id="count-topo.decisions">0</span></h2><div class="rows" id="stage-topo.decisions"></div></div>
    </section>
    <section class="timeline">
      <h2>Live Event Timeline</h2>
      <table>
        <thead><tr><th>Time</th><th>Stage</th><th>Topic</th><th>Partition</th><th>Service</th><th>Correlation</th><th>Summary</th></tr></thead>
        <tbody id="events"></tbody>
      </table>
    </section>
  </main>
  <script>
    const events = [];
    const byTopic = {"metrics.events": [], "service.state": [], "topo.decisions": []};
    const counts = {"metrics.events": 0, "service.state": 0, "topo.decisions": 0};

    function short(value) {
      if (value === null || value === undefined) return "";
      return String(value).length > 18 ? String(value).slice(0, 18) + "..." : String(value);
    }
    function summaryText(obj) {
      if (!obj) return "";
      return Object.entries(obj).map(([k,v]) => `${k}=${v}`).join(" ");
    }
    function rowFor(e) {
      const div = document.createElement("div");
      div.className = "row";
      div.innerHTML = `<span class="pill">p${e.partition ?? "-"}</span><span><span class="svc">${e.service || "-"}</span><br><span class="meta">${summaryText(e.summary)}</span></span><span class="meta">#${e.offset ?? "-"}</span>`;
      return div;
    }
    function renderTopic(topic) {
      const el = document.getElementById(`stage-${topic}`);
      el.innerHTML = "";
      byTopic[topic].slice(-6).reverse().forEach(e => el.appendChild(rowFor(e)));
      document.getElementById(`count-${topic}`).textContent = counts[topic];
    }
    function addEvent(e) {
      events.push(e);
      if (events.length > 80) events.shift();
      if (e.topic && byTopic[e.topic]) {
        byTopic[e.topic].push(e);
        if (byTopic[e.topic].length > 20) byTopic[e.topic].shift();
        counts[e.topic] += 1;
        renderTopic(e.topic);
      }
      const body = document.getElementById("events");
      body.innerHTML = "";
      events.slice().reverse().forEach(ev => {
        const tr = document.createElement("tr");
        tr.innerHTML = `<td>${short(ev.seen_at)}</td><td>${ev.stage || ""}</td><td>${ev.topic || ""}</td><td>${ev.partition ?? ""}</td><td>${ev.service || ""}</td><td>${short(ev.correlation_id)}</td><td>${summaryText(ev.summary) || ev.error || ""}</td>`;
        body.appendChild(tr);
      });
    }
    fetch("/events").then(r => r.json()).then(items => items.forEach(addEvent));
    const source = new EventSource("/stream");
    source.onopen = () => document.getElementById("status").textContent = "live";
    source.onerror = () => document.getElementById("status").textContent = "reconnecting";
    source.onmessage = (msg) => addEvent(JSON.parse(msg.data));
  </script>
</body>
</html>
"""


def create_app(hub: EventHub) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def index():
        return render_template_string(HTML)

    @app.get("/events")
    def events():
        return jsonify(hub.snapshot())

    @app.get("/stream")
    def stream():
        q = hub.subscribe()

        def generate():
            try:
                while True:
                    event = q.get()
                    yield f"data: {json.dumps(event, default=str)}\n\n"
            finally:
                hub.unsubscribe(q)

        return Response(generate(), mimetype="text/event-stream")

    return app


def build_agents(producer: KafkaProducerTemplate, run_id: str) -> List[AgentThread]:
    threads: List[AgentThread] = []
    for svc in iter_services():
        agent = ServiceAgent(
            service_id=svc.service_id,
            cloud=svc.cloud.value,
            cloud_producer=producer,
            group_id=f"live_service_agents_{run_id}",
            partition=svc.partition,
        )
        threads.append(AgentThread(agent, f"ServiceAgent-{svc.name}"))

    topo_classes = {"aws": AWSTopo, "aks": AKSTopo, "do": DOTopo}
    for svc in iter_services():
        agent = topo_classes[svc.name](
            service_id=svc.name,
            producer=producer,
            group_id=f"live_topo_{svc.name}_{run_id}",
            partitions={"service.state": [0, 1, 2]},
        )
        threads.append(AgentThread(agent, f"TopoAgent-{svc.name}"))
    return threads


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full pipeline with a live UI visualizer.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8088)
    parser.add_argument("--from-beginning", action="store_true", help="Visualizer reads existing topic data too.")
    parser.add_argument("--skip-mock-check", action="store_true")
    args = parser.parse_args()

    servers = [s.strip() for s in args.bootstrap_servers.split(",") if s.strip()]
    KafkaConfig.BOOTSTRAP_SERVERS = servers
    os.environ["KAFKA_BOOTSTRAP_SERVERS"] = ",".join(servers)

    if not args.skip_mock_check:
        wait_for_mocks()

    with TopicInitializer() as initializer:
        results = initializer.create_all_topics()
    if not all(results.values()):
        raise RuntimeError(f"Kafka topic initialization failed: {results}")

    hub = EventHub()
    producer = KafkaProducerTemplate()
    run_id = uuid4().hex[:8]
    agent_threads = build_agents(producer, run_id)
    observers = [
        KafkaObserver("metrics.events", 3, hub, args.from_beginning),
        KafkaObserver("service.state", 3, hub, args.from_beginning),
        KafkaObserver("topo.decisions", 3, hub, args.from_beginning),
    ]
    adapter = AdapterLoop(producer, hub, args.interval)
    adapter_thread = ManagedThread("PrometheusAdapterLoop", adapter.run)

    for thread in observers + agent_threads:
        thread.start()
    time.sleep(2.0)
    adapter_thread.start()

    url = f"http://{args.host}:{args.port}"
    logger.info("Pipeline visualizer running at %s", url)
    logger.info("Press Ctrl-C to stop.")

    try:
        app = create_app(hub)
        app.run(host=args.host, port=args.port, threaded=True, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("Stopping live pipeline runner.")
    finally:
        adapter.stop()
        for observer in observers:
            observer.stop()
        for thread in agent_threads:
            thread.stop()
        for thread in observers + agent_threads + [adapter_thread]:
            thread.join(timeout=5)
        producer.close()


if __name__ == "__main__":
    main()
