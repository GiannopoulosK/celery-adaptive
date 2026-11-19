"""Django-aware adaptive task with transaction-safe dispatch."""

from __future__ import annotations

from celery.utils import uuid

from celery_adaptive.core.task import AdaptiveTask


class DjangoAdaptiveTask(AdaptiveTask):
    """
    :class:`~celery_adaptive.AdaptiveTask` plus ``delay_on_commit``.

    Dispatching inside a database transaction is a classic race: the worker
    can pick the task up before the transaction commits and not find the
    row it was meant to process, or the transaction rolls back and the task
    acts on data that never existed. ``delay_on_commit`` publishes only
    after a successful commit (immediately when not in a transaction).
    """

    def delay_on_commit(self, *args, **kwargs):
        """Like :meth:`delay`, but publishes after the transaction commits."""
        return self.apply_async_on_commit(args, kwargs)

    def apply_async_on_commit(self, args=None, kwargs=None, *, using=None, **options):
        """
        Like :meth:`apply_async`, but publishes after the transaction commits.

        Returns an ``AsyncResult`` for the task id that *will* be used. If
        the transaction rolls back nothing is published and the result stays
        ``PENDING``.
        """
        from django.db import transaction

        task_id = options.pop("task_id", None) or uuid()
        transaction.on_commit(
            lambda: self.apply_async(args, kwargs, task_id=task_id, **options),
            using=using,
        )
        return self.AsyncResult(task_id)
