"""Prometheus Metrics Adapter Runner

Standalone entrypoint for running the Prometheus-to-Kafka metrics adapter.
Polls Prometheus endpoints and sends metrics to Kafka topics.

USAGE:
  python runners/prometheus_adapter_runner.py
"""

import logging

from kafka_core.prometheus_kafka_adaptar import main


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    main()
