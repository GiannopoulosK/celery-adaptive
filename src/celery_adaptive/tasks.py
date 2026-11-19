"""
Maintenance tasks. Make your worker import this module, e.g.::

    app = Celery(..., include=["celery_adaptive.tasks"])
    app.conf.beat_schedule.update(celery_adaptive.beat_schedule())
"""

from __future__ import annotations

from celery import current_app, shared_task


@shared_task(name="celery_adaptive.flush_batches", ignore_result=True)
def flush_batches() -> dict[str, int]:
    """Drain batch buffers whose oldest payload exceeded ``max_wait_ms``."""
    from .batching import flush_all

    return flush_all(current_app, only_stale=True)


@shared_task(name="celery_adaptive.replay_outbox", ignore_result=True)
def replay_outbox(limit: int | None = None) -> int:
    """Publish tasks stored in the outbox while the broker was down."""
    from .core.outbox import replay

    return replay(current_app, limit=limit)
