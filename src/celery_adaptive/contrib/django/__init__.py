"""
Django integration.

Add ``"celery_adaptive.contrib.django"`` to ``INSTALLED_APPS``. Settings are
read from ``settings.CELERY_ADAPTIVE`` at startup, and a django-redis client
is reused automatically when your default cache is django-redis.

Use this module's ``adaptive_task`` to get ``delay_on_commit()``::

    from celery_adaptive.contrib.django import adaptive_task

    @adaptive_task(priority="high")
    def send_receipt(order_id): ...

    send_receipt.delay_on_commit(order.id)
"""

from __future__ import annotations

from typing import Any

from .task import DjangoAdaptiveTask


def adaptive_task(fn=None, **options: Any):
    """:func:`celery_adaptive.adaptive_task` using :class:`DjangoAdaptiveTask`."""
    from celery_adaptive.core.decorators import adaptive_task as _adaptive_task

    options.setdefault("base", DjangoAdaptiveTask)
    return _adaptive_task(fn, **options)


__all__ = ["DjangoAdaptiveTask", "adaptive_task"]
