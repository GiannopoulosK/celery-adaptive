"""
Buffered batch execution.

A batch task receives a list of payloads instead of being called once per
item::

    @batch_task(max_batch_size=500, max_wait_ms=2000, queue="analytics")
    def record_events(payloads):
        Event.objects.bulk_create(Event(**p) for p in payloads)

    record_events.push(user_id=1, kind="click")  # buffered in Redis

A batch is published when ``max_batch_size`` payloads are buffered, or on
the next push once the oldest payload is ``max_wait_ms`` old. Buffers that
stop receiving pushes are drained by the ``celery_adaptive.flush_batches``
periodic task (see :func:`celery_adaptive.beat_schedule`).
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Callable
from typing import Any

from celery import Task, shared_task

from ..redis_client import get_redis_client
from .collector import BatchCollector

logger = logging.getLogger(__name__)


class BatchTask(Task):
    """Celery task whose single argument is ``payloads: list[dict]``."""

    batch_max_size: int = 100
    batch_max_wait_ms: int = 1000
    #: Queue batches are published to. ``None`` leaves routing to Celery.
    batch_queue: str | None = None

    def push(self, **payload: Any) -> bool:
        """
        Buffer one payload. Returns True if this push triggered a flush.

        Payloads must be JSON-serializable. Without Redis (or if Redis
        errors), the payload is published immediately as a batch of one.
        """
        redis = get_redis_client()
        if redis is None:
            logger.warning(
                "No Redis client configured; publishing '%s' unbatched.", self.name
            )
            self._publish([payload], trigger="unbuffered")
            return True
        try:
            length, age_ms = BatchCollector(redis).add(self.name, payload)
        except Exception as exc:
            logger.warning(
                "Batch buffer unavailable for '%s' (%s); publishing unbatched.",
                self.name,
                exc,
            )
            self._publish([payload], trigger="unbuffered")
            return True

        if length >= self.batch_max_size:
            self.flush(trigger="size")
            return True
        if age_ms >= self.batch_max_wait_ms:
            self.flush(trigger="time")
            return True
        return False

    async def apush(self, **payload: Any) -> bool:
        """Async counterpart of :meth:`push`, run in a worker thread."""
        return await asyncio.to_thread(functools.partial(self.push, **payload))

    def flush(self, trigger: str = "manual") -> int:
        """
        Publish buffered payloads, up to ``batch_max_size`` per batch.

        Returns the number of payloads published. If publishing fails the
        payloads are returned to the buffer and the error is logged.
        """
        redis = get_redis_client()
        if redis is None:
            return 0
        collector = BatchCollector(redis)
        published = 0
        while True:
            raw = collector.collect(self.name, self.batch_max_size)
            if not raw:
                break
            payloads = collector.decode(raw)
            try:
                if payloads:
                    self._publish(payloads, trigger=trigger)
            except Exception as exc:
                collector.requeue(self.name, raw)
                logger.warning(
                    "Flushing '%s' failed; %d payload(s) kept in the buffer: %s",
                    self.name,
                    len(raw),
                    exc,
                )
                break
            published += len(payloads)
            if len(raw) < self.batch_max_size:
                break
        return published

    def pending(self) -> int:
        redis = get_redis_client()
        return BatchCollector(redis).pending_count(self.name) if redis else 0

    def is_stale(self) -> bool:
        """True when the oldest buffered payload exceeded ``batch_max_wait_ms``."""
        redis = get_redis_client()
        if redis is None:
            return False
        age = BatchCollector(redis).oldest_age_ms(self.name)
        return age is not None and age >= self.batch_max_wait_ms

    def _publish(self, payloads: list[dict], *, trigger: str) -> None:
        options = {"queue": self.batch_queue} if self.batch_queue else {}
        self.apply_async(kwargs={"payloads": payloads}, **options)
        from ..metrics.exporters import record_batch_flush

        record_batch_flush(self.name, len(payloads), trigger)


def batch_task(
    fn: Callable | None = None,
    *,
    max_batch_size: int = 100,
    max_wait_ms: int = 1000,
    queue: str | None = None,
    app=None,
    base: type[BatchTask] = BatchTask,
    **celery_options: Any,
):
    """
    Declare a batch task. The function receives ``payloads: list[dict]``.

    Args:
        max_batch_size: Flush once this many payloads are buffered; also the
            largest batch a single task call receives.
        max_wait_ms: Flush on the next push once the oldest payload is this
            old. Idle buffers are drained by ``flush_batches``.
        queue: Queue each batch is published to.
        app: Register on this Celery app instead of using ``shared_task``.
        **celery_options: Passed to Celery's task decorator (``name``,
            ``acks_late``, ``autoretry_for``...).
    """
    if max_batch_size < 1:
        raise ValueError("max_batch_size must be >= 1")
    if max_wait_ms < 0:
        raise ValueError("max_wait_ms must be >= 0")
    if not (isinstance(base, type) and issubclass(base, BatchTask)):
        raise TypeError("base must be a subclass of BatchTask")

    options = {
        "base": base,
        "batch_max_size": max_batch_size,
        "batch_max_wait_ms": max_wait_ms,
        "batch_queue": queue,
        **celery_options,
    }
    decorator = app.task(**options) if app is not None else shared_task(**options)
    return decorator(fn) if fn is not None else decorator


def batch_tasks(app) -> list[BatchTask]:
    """All batch tasks registered on ``app``."""
    return [t for t in app.tasks.values() if isinstance(t, BatchTask)]


def flush_all(app, *, only_stale: bool = False, name_filter: str | None = None):
    """Flush every batch task on ``app``. Returns ``{task_name: count}``."""
    flushed: dict[str, int] = {}
    for task in batch_tasks(app):
        if name_filter and task.name != name_filter:
            continue
        if only_stale and not task.is_stale():
            continue
        count = task.flush(trigger="stale" if only_stale else "manual")
        if count:
            flushed[task.name] = count

    redis = get_redis_client()
    if redis is not None:
        known = {t.name for t in batch_tasks(app)}
        for orphan in set(BatchCollector(redis).pending_task_names()) - known:
            logger.warning(
                "Payloads buffered for '%s' but no such batch task is registered "
                "on this app; import its module so it can be flushed.",
                orphan,
            )
    return flushed
