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
    REDIS_LATENCY = Gauge("celery_adaptive_redis_latency_ms", "Redis RTT")
    RABBITMQ_DEPTH = Gauge("celery_adaptive_rabbitmq_depth", "Queue depth", ["queue"])
    CELERY_ACTIVE = Gauge(
        "celery_adaptive_worker_active_tasks", "Active tasks", ["worker"]
    )

    # -- Routing --
    ROUTING_DECISIONS = Counter(
        "celery_adaptive_routing_decisions_total",
        "Routing actions taken",
        ["action", "queue"],
    )

else:
    TASKS_ENQUEUED = SYNC_FALLBACKS = BATCH_SIZE = BATCH_FLUSHES = _NOOP
    REDIS_LATENCY = RABBITMQ_DEPTH = CELERY_ACTIVE = ROUTING_DECISIONS = _NOOP


# ── Health snapshot → gauges ──────────────────────────────────────────────────


def update_health_metrics(snapshot) -> None:
    """Updates gauges based on a HealthSnapshot."""
    if not _PROMETHEUS_AVAILABLE:
        return

    if snapshot.redis and snapshot.redis.healthy:
        REDIS_LATENCY.set(snapshot.redis.latency_ms or 0)

    if snapshot.rabbitmq and snapshot.rabbitmq.healthy:
        for q in snapshot.rabbitmq.queues:
            RABBITMQ_DEPTH.labels(queue=q.name).set(q.messages)

    if snapshot.celery and snapshot.celery.healthy:
        for w in snapshot.celery.workers:
            CELERY_ACTIVE.labels(worker=w.hostname).set(w.active_tasks)


def get_latest_metrics():
    """Generates the latest metrics buffer and content type."""
    return generate_latest(), CONTENT_TYPE_LATEST
