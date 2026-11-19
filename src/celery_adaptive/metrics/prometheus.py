"""
Prometheus metrics for celery-adaptive.

Metrics are registered on the default ``prometheus_client`` registry when
the package is installed (``pip install celery-adaptive[metrics]``) and are
no-ops otherwise. Expose them with your framework's metrics endpoint, or the
Django/FastAPI integrations. For multi-process servers (gunicorn, uWSGI) use
prometheus_client's multiprocess mode.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    _PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised via monkeypatch
    _PROMETHEUS_AVAILABLE = False


class _NoopMetric:
    def labels(self, *_, **__):
        return self

    def inc(self, *_):
        pass

    def observe(self, *_):
        pass

    def set(self, *_):
        pass


_NOOP = _NoopMetric()

#: Numeric encoding of circuit breaker states for the ``circuit_state`` gauge.
CIRCUIT_STATE_VALUES = {"closed": 0, "half_open": 1, "open": 2}

if _PROMETHEUS_AVAILABLE:
    # -- Dispatch --
    TASKS_PUBLISHED = Counter(
        "celery_adaptive_tasks_published_total",
        "Tasks published to the broker",
        ["task_name", "queue"],
    )
    DEDUP_HITS = Counter(
        "celery_adaptive_dedup_hits_total",
        "Dispatches suppressed because an identical task was in flight",
        ["task_name"],
    )
    FALLBACKS = Counter(
        "celery_adaptive_fallbacks_total",
        "Dispatches that could not reach the broker, by fallback strategy",
        ["task_name", "strategy"],
    )
    CIRCUIT_STATE = Gauge(
        "celery_adaptive_circuit_state",
        "Broker circuit breaker state (0=closed, 1=half-open, 2=open)",
        ["broker"],
    )
    OUTBOX_REPLAYED = Counter(
        "celery_adaptive_outbox_replayed_total",
        "Tasks published from the outbox after the broker recovered",
    )
    ROUTING_DECISIONS = Counter(
        "celery_adaptive_routing_decisions_total",
        "Health-based routing decisions",
        ["action", "queue"],
    )

    # -- Batching --
    BATCH_SIZE = Histogram(
        "celery_adaptive_batch_size",
        "Payloads per published batch",
        ["task_name"],
        buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000],
    )
    BATCH_FLUSHES = Counter(
        "celery_adaptive_batch_flushes_total",
        "Published batches by trigger (size, time, stale, manual, unbuffered)",
        ["task_name", "trigger"],
    )

    # -- Infrastructure health (updated by the health monitor) --
    BROKER_UP = Gauge("celery_adaptive_broker_up", "1 if the broker is reachable")
    QUEUE_DEPTH = Gauge(
        "celery_adaptive_queue_depth", "Messages waiting per queue", ["queue"]
    )
    REDIS_LATENCY = Gauge(
        "celery_adaptive_redis_latency_ms", "Redis round-trip time in milliseconds"
    )
    CELERY_ACTIVE = Gauge(
        "celery_adaptive_worker_active_tasks", "Active tasks per worker", ["worker"]
    )
else:  # pragma: no cover
    TASKS_PUBLISHED = DEDUP_HITS = FALLBACKS = CIRCUIT_STATE = _NOOP
    OUTBOX_REPLAYED = ROUTING_DECISIONS = BATCH_SIZE = BATCH_FLUSHES = _NOOP
    BROKER_UP = QUEUE_DEPTH = REDIS_LATENCY = CELERY_ACTIVE = _NOOP


def update_health_metrics(snapshot) -> None:
    """Update gauges from a :class:`~celery_adaptive.health.HealthSnapshot`."""
    if not _PROMETHEUS_AVAILABLE:
        return

    if snapshot.broker is not None:
        BROKER_UP.set(1 if snapshot.broker.healthy else 0)

    for queue, depth in snapshot.queue_depths.items():
        QUEUE_DEPTH.labels(queue=queue).set(depth)

    if snapshot.redis is not None and snapshot.redis.healthy:
        REDIS_LATENCY.set(snapshot.redis.latency_ms or 0)

    if snapshot.celery is not None and snapshot.celery.healthy:
        for w in snapshot.celery.workers:
            CELERY_ACTIVE.labels(worker=w.hostname).set(w.active_tasks)


def get_latest_metrics() -> tuple[bytes, str]:
    """Return the latest metrics payload and its content type."""
    if not _PROMETHEUS_AVAILABLE:
        return b"", "text/plain; charset=utf-8"
    return generate_latest(), CONTENT_TYPE_LATEST
