"""The :func:`adaptive_task` decorator."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from celery import shared_task

from .task import AdaptiveTask


def adaptive_task(
    fn: Callable | None = None,
    *,
    priority: str | None = None,
    deduplicate: bool = False,
    dedup_key: Callable[..., Any] | None = None,
    fallback_sync: bool | None = None,
    app=None,
    base: type[AdaptiveTask] = AdaptiveTask,
    **celery_options: Any,
):
    """
    Declare a Celery task with adaptive dispatch.

    Works with or without arguments::

        @adaptive_task
        def ping(): ...

        @adaptive_task(priority="high", deduplicate=True, bind=True)
        def charge(self, order_id): ...

    Args:
        priority: Key into the ``QUEUE_MAP`` setting (``"high"``,
            ``"default"``, ``"low"``). ``None`` leaves routing to Celery.
        deduplicate: While a task with identical arguments is in flight,
            return its result handle instead of publishing again.
        dedup_key: ``f(*args, **kwargs)`` returning the value that
            identifies duplicates, e.g. ``lambda order_id, **_: order_id``.
        fallback_sync: Run inline when the broker is unreachable. ``None``
            uses the ``SYNC_FALLBACK`` setting.
        app: Register on this Celery app. Defaults to ``shared_task`` so the
            task binds to whichever app is current.
        base: Task base class; must subclass :class:`AdaptiveTask`.
        **celery_options: Passed through to Celery's task decorator
            (``bind``, ``name``, ``max_retries``, ``autoretry_for``...).

    The decorated object is a regular Celery task: call ``.delay()``,
    ``.apply_async()``, ``.s()`` and use it in canvas primitives. Per-call
    overrides go in ``apply_async(..., adaptive={"priority": "high"})``.
    """
    if not (isinstance(base, type) and issubclass(base, AdaptiveTask)):
        raise TypeError("base must be a subclass of AdaptiveTask")

    options = {
        "base": base,
        "adaptive_priority": priority,
        "adaptive_deduplicate": deduplicate,
        "adaptive_dedup_key": staticmethod(dedup_key) if dedup_key else None,
        "adaptive_fallback_sync": fallback_sync,
        **celery_options,
    }
    decorator = app.task(**options) if app is not None else shared_task(**options)

    if fn is not None:
        return decorator(fn)
    return decorator
