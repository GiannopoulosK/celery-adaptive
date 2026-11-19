"""
Unified Prometheus metrics for celery-adaptive.
"""

import logging

logger = logging.getLogger("celery_adaptive.metrics")

try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    _PROMETHEUS_AVAILABLE = True
except ImportError:
    _PROMETHEUS_AVAILABLE = False


class _NoopMetric:
    def labels(self, **_):
        return self

    def inc(self, *_):
        pass

    def observe(self, *_):
        pass

    def set(self, *_):
        pass


_NOOP = _NoopMetric()


if _PROMETHEUS_AVAILABLE:
    TASKS_ENQUEUED = Counter(
        "celery_adaptive_tasks_enqueued_total",
        "Total tasks successfully enqueued",
        ["task_name", "queue"],
    )
    DEDUP_HITS = Counter(
        "celery_adaptive_dedup_hits_total",
        "Dispatches suppressed by deduplication",
        ["task_name"],
    )
    SYNC_FALLBACKS = Counter(
        "celery_adaptive_sync_fallbacks_total",
        "Tasks that bypassed the broker due to health issues",
        ["task_name", "reason"],
    )

    # -- Batching --
    BATCH_SIZE = Histogram(
        "celery_adaptive_batch_size",
        "Items per flushed batch",
        ["task_name"],
        buckets=[1, 10, 50, 100, 500],
    )
    BATCH_FLUSHES = Counter(
        "celery_adaptive_batch_flushes_total",
        "Batch flush triggers",
        ["task_name", "trigger"],
    )

    # -- Infrastructure Health --
    BROKER_UP = Gauge("celery_adaptive_broker_up", "1 if the broker is reachable")
    QUEUE_DEPTH = Gauge(
        "celery_adaptive_queue_depth", "Messages waiting per queue", ["queue"]
    )
    REDIS_LATENCY = Gauge("celery_adaptive_redis_latency_ms", "Redis round-trip time")
    CELERY_ACTIVE = Gauge(
        "celery_adaptive_worker_active_tasks", "Active tasks per worker", ["worker"]
    )

    # -- Routing --
    ROUTING_DECISIONS = Counter(
        "celery_adaptive_routing_decisions_total",
        "Routing actions taken",
        ["action", "queue"],
    )

else:
    TASKS_ENQUEUED = SYNC_FALLBACKS = DEDUP_HITS = BATCH_SIZE = BATCH_FLUSHES = _NOOP
    BROKER_UP = QUEUE_DEPTH = REDIS_LATENCY = CELERY_ACTIVE = ROUTING_DECISIONS = _NOOP


# ── Health snapshot → gauges ──────────────────────────────────────────────────


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
