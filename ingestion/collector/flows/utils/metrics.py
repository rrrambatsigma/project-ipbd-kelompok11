"""
utils/metrics.py
Prometheus metrics — push ke Pushgateway untuk monitoring Grafana
"""

import os
from prometheus_client import CollectorRegistry, Counter, Gauge, push_to_gateway
from loguru import logger

PUSHGATEWAY_URL = os.getenv("PUSHGATEWAY_URL", "http://pushgateway:9091")
JOB_NAME = "ingestion"


def push_metrics(session: str, summary: dict, duration_seconds: float) -> None:
    registry = CollectorRegistry()

    articles_counter = Counter(
        "ingestion_articles_total",
        "Total articles processed per source",
        ["source", "status"],
        registry=registry,
    )

    for src in summary.get("per_source", []):
        source = src.get("source", "unknown")
        articles_counter.labels(source=source, status="input").inc(src.get("total_input", 0))
        articles_counter.labels(source=source, status="uploaded").inc(src.get("total_uploaded", 0))
        articles_counter.labels(source=source, status="skipped").inc(src.get("skipped", 0))
        if src.get("failed", 0) > 0:
            articles_counter.labels(source=source, status="failed").inc(src.get("failed", 0))

    errors_counter = Counter(
        "ingestion_errors_total",
        "Total errors per source",
        ["source"],
        registry=registry,
    )
    for src in summary.get("per_source", []):
        if src.get("failed", 0) > 0:
            errors_counter.labels(source=src["source"]).inc(src["failed"])

    duration_gauge = Gauge(
        "ingestion_run_duration_seconds",
        "Duration of ingestion run",
        ["session"],
        registry=registry,
    )
    duration_gauge.labels(session=session).set(duration_seconds)

    unique_gauge = Gauge(
        "ingestion_articles_unique",
        "Unique articles uploaded per session",
        ["session"],
        registry=registry,
    )
    unique_gauge.labels(session=session).set(summary.get("total_uploaded", 0))

    try:
        push_to_gateway(PUSHGATEWAY_URL, job=JOB_NAME, registry=registry)
        logger.info(f"[Metrics] Pushed to Pushgateway ({PUSHGATEWAY_URL})")
    except Exception as e:
        logger.warning(f"[Metrics] Gagal push ke Pushgateway: {e}")
