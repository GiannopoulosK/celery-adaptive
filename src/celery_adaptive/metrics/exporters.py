"""
Metric recording hooks used throughout the library.

Every function swallows its own errors: observability must never break
task dispatch.
"""

from __future__ import annotations

import logging

from . import prometheus as m

logger = logging.getLogger(__name__)


def _safe(fn):
    def wrapper(*args, **kwargs):
        try:
            fn(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Recording metric %s failed: %s", fn.__name__, exc)

    wrapper.__name__ = fn.__name__
    wrapper.__doc__ = fn.__doc__
    return wrapper


@_safe
def record_published(task_name: str, queue: str) -> None:
    m.TASKS_PUBLISHED.labels(task_name=task_name, queue=queue).inc()


@_safe
def record_dedup_hit(task_name: str) -> None:
    m.DEDUP_HITS.labels(task_name=task_name).inc()


@_safe
def record_fallback(task_name: str, strategy: str) -> None:
    m.FALLBACKS.labels(task_name=task_name, strategy=strategy).inc()


@_safe
def record_circuit_state(broker: str, state: str) -> None:
    m.CIRCUIT_STATE.labels(broker=broker).set(m.CIRCUIT_STATE_VALUES[state])


@_safe
def record_outbox_replayed(count: int) -> None:
    m.OUTBOX_REPLAYED.inc(count)


@_safe
def record_batch_flush(task_name: str, count: int, trigger: str) -> None:
    m.BATCH_SIZE.labels(task_name=task_name).observe(count)
    m.BATCH_FLUSHES.labels(task_name=task_name, trigger=trigger).inc()


@_safe
def record_routing_decision(action: str, queue: str) -> None:
    m.ROUTING_DECISIONS.labels(action=action, queue=queue).inc()
