"""
Buffered batch execution engine.
"""

import functools
import logging
from collections.abc import Callable

from celery import current_app

from ..redis_client import get_redis_client

logger = logging.getLogger("celery_adaptive.batching.engine")

_DEFAULT_QUEUE = "default"


def batch_task(
    *,
    max_batch_size: int = 100,
    max_wait_ms: int = 200,
    queue: str = _DEFAULT_QUEUE,
    **celery_kwargs,
) -> Callable:

    def decorator(func: Callable) -> Callable:
        # Register as a standard Celery task that expects a list of payloads
        celery_task = current_app.task(
            name=f"celery_adaptive.batch.{func.__module__}.{func.__qualname__}",
            queue=queue,
            **celery_kwargs,
        )(func)

        @functools.wraps(func)
        def push(**payload_kwargs) -> bool:
            redis = get_redis_client()
            if not redis:
                logger.warning(
                    "Redis down: executing batch_task '%s' sync.", celery_task.name
                )
                func(payloads=[payload_kwargs])
                return False

            from .collector import BatchCollector

            collector = BatchCollector(
                redis_client=redis,
                max_batch_size=max_batch_size,
                max_wait_ms=max_wait_ms,
            )

            if collector.add(celery_task.name, payload_kwargs):
                _internal_flush(collector, celery_task)
                return True
            return False

        def _internal_flush(collector, task):
            payloads = collector.collect(task.name)
            if payloads:
                task.apply_async(kwargs={"payloads": payloads}, queue=queue)
                return len(payloads)
            return 0

        def manual_flush() -> int:
            redis = get_redis_client()
            if not redis:
                return 0
            from .collector import BatchCollector

            return _internal_flush(
                BatchCollector(redis, max_batch_size, max_wait_ms), celery_task
            )

        push.push = push
        push.flush = manual_flush
        push.celery_task = celery_task
        return push

    return decorator
