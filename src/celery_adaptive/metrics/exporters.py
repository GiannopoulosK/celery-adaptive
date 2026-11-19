"""
Metric recording hooks used throughout the library.
"""

from .prometheus import (
    BATCH_FLUSHES,
    BATCH_SIZE,
    DEDUP_HITS,
    ROUTING_DECISIONS,
    SYNC_FALLBACKS,
    TASKS_ENQUEUED,
)


def record_enqueue(task_name: str, queue: str) -> None:
    TASKS_ENQUEUED.labels(task_name=task_name, queue=queue).inc()


def record_dedup_hit(task_name: str) -> None:
    DEDUP_HITS.labels(task_name=task_name).inc()


def record_sync_fallback(task_name: str, reason: str) -> None:
    SYNC_FALLBACKS.labels(task_name=task_name, reason=reason).inc()


def record_batch_flush(task_name: str, count: int, trigger: str) -> None:
    BATCH_SIZE.labels(task_name=task_name).observe(count)
    BATCH_FLUSHES.labels(task_name=task_name, trigger=trigger).inc()


def record_routing_decision(action: str, queue: str) -> None:
    ROUTING_DECISIONS.labels(action=action, queue=queue).inc()
