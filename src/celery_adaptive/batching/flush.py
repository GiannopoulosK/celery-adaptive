"""
Celery beat task to drain low-traffic buffers.
"""

import logging

from celery import current_app

from ..redis_client import get_redis_client

logger = logging.getLogger("celery_adaptive.batching.flush")


@current_app.task(name="celery_adaptive.batching.flush_stale_batches")
def flush_stale_batches():
    redis = get_redis_client()
    if not redis:
        return {"flushed": 0}

    from .collector import BatchCollector

    collector = BatchCollector(redis)
    pending_tasks = collector.all_pending_tasks()

    stats = {"flushed": 0, "task_counts": {}}

    for task_name in pending_tasks:
        # Use send_task to trigger the flush logic via the worker
        payloads = collector.collect(task_name)
        if payloads:
            current_app.send_task(task_name, kwargs={"payloads": payloads})
            stats["flushed"] += len(payloads)
            stats["task_counts"][task_name] = len(payloads)

    return stats
