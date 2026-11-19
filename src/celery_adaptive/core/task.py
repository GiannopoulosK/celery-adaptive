"""
``AdaptiveTask``: a :class:`celery.Task` with health-aware dispatch.

Because it is a real Celery task class, everything Celery offers keeps
working: signatures (``.s()``, ``.si()``), chains, groups, chords,
``bind=True``, ``self.retry()``, task routing and eager mode. Only
:meth:`apply_async` is extended:

1. Resolve the target queue from the task's ``adaptive_priority``.
2. ``FORCE_SYNC``: run inline and return an ``EagerResult``.
3. Deduplicate identical in-flight calls (optional).
4. Consult the health monitor, which may reroute the task (optional).
5. Publish, guarded by a per-broker circuit breaker with a bounded retry
   policy. If the broker is unreachable, apply the fallback strategy.

Use it through :func:`celery_adaptive.adaptive_task` or directly::

    @app.task(base=AdaptiveTask, adaptive_priority="high")
    def send_email(user_id): ...
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from celery import Task
from celery.result import AsyncResult, EagerResult
from celery.utils import uuid
from kombu.exceptions import OperationalError
from kombu.utils.url import maybe_sanitize_url

from ..conf import conf
from ..exceptions import BrokerUnavailableError, ConfigurationError
from ..redis_client import get_redis_client
from .circuit import get_breaker

logger = logging.getLogger(__name__)

# Only connectivity failures trigger the fallback. Anything else (bad
# arguments, serialization errors, bugs) propagates to the caller.
BROKER_ERRORS: tuple[type[BaseException], ...] = (
    OperationalError,
    ConnectionError,
    TimeoutError,
)

# Fail fast instead of kombu's default of 3 retries per publish; the
# circuit breaker takes care of repeated outages.
DEFAULT_PUBLISH_RETRY_POLICY = {
    "max_retries": 1,
    "interval_start": 0,
    "interval_step": 0.2,
    "interval_max": 0.2,
}

_OVERRIDABLE = frozenset({"priority", "deduplicate", "fallback_sync"})


@dataclass(frozen=True)
class DispatchPolicy:
    """Effective adaptive options for a single dispatch."""

    priority: str | None
    deduplicate: bool
    fallback_sync: bool


class AdaptiveTask(Task):
    """Celery task base class with adaptive, failure-aware dispatch."""

    #: Key into the ``QUEUE_MAP`` setting. ``None`` leaves queue selection
    #: to Celery (``queue=`` option, ``task_routes`` or the default queue).
    adaptive_priority: str | None = None
    #: Suppress duplicate dispatches with identical arguments.
    adaptive_deduplicate: bool = False
    #: Run inline when the broker is unreachable. ``None`` uses the
    #: ``SYNC_FALLBACK`` setting.
    adaptive_fallback_sync: bool | None = None

    # ------------------------------------------------------------------ API

    def apply_async(
        self,
        args=None,
        kwargs=None,
        task_id=None,
        producer=None,
        link=None,
        link_error=None,
        shadow=None,
        **options,
    ):
        policy = self._resolve_policy(options.pop("adaptive", None))
        args = tuple(args or ())
        kwargs = dict(kwargs or {})
        task_id = task_id or uuid()

        if policy.priority is not None and "queue" not in options:
            options["queue"] = conf.QUEUE_MAP[policy.priority]

        if conf.FORCE_SYNC:
            logger.debug("FORCE_SYNC: running '%s' inline.", self.name)
            return self._run_inline(args, kwargs, task_id, link, link_error)

        if (
            policy.deduplicate
            and not conf.DISABLE_DEDUPLICATION
            and not self._acquire_dedup(args, kwargs)
        ):
            return None

        if conf.HEALTH.get("ENABLED"):
            self._apply_health_routing(options, args, kwargs)

        options.setdefault("retry_policy", DEFAULT_PUBLISH_RETRY_POLICY)
        breaker = get_breaker(self._broker_key())

        if not breaker.allow():
            return self._handle_unavailable(
                policy, args, kwargs, task_id, link, link_error, reason="circuit open"
            )

        try:
            result = super().apply_async(
                args,
                kwargs,
                task_id=task_id,
                producer=producer,
                link=link,
                link_error=link_error,
                shadow=shadow,
                **options,
            )
        except BROKER_ERRORS as exc:
            breaker.record_failure()
            logger.warning("Publishing '%s' failed: %s", self.name, exc)
            return self._handle_unavailable(
                policy, args, kwargs, task_id, link, link_error, reason=str(exc)
            )

        breaker.record_success()
        return result

    # ------------------------------------------------------------ internals

    def _resolve_policy(self, overrides: dict[str, Any] | None) -> DispatchPolicy:
        values: dict[str, Any] = {
            "priority": self.adaptive_priority,
            "deduplicate": self.adaptive_deduplicate,
            "fallback_sync": self.adaptive_fallback_sync,
        }
        if overrides:
            unknown = set(overrides) - _OVERRIDABLE
            if unknown:
                raise ConfigurationError(
                    f"Unknown adaptive option(s) {sorted(unknown)} for "
                    f"'{self.name}'. Valid: {sorted(_OVERRIDABLE)}."
                )
            values.update(overrides)

        if values["priority"] is not None and values["priority"] not in conf.QUEUE_MAP:
            raise ConfigurationError(
                f"Invalid priority '{values['priority']}' for '{self.name}'. "
                f"Valid: {sorted(conf.QUEUE_MAP)}."
            )
        if values["fallback_sync"] is None:
            values["fallback_sync"] = conf.SYNC_FALLBACK
        return DispatchPolicy(**values)

    def _broker_key(self) -> str:
        return maybe_sanitize_url(self.app.conf.broker_url or "default")

    def _effective_queue(self, options: dict[str, Any]) -> str:
        queue = options.get("queue")
        if queue is not None:
            return getattr(queue, "name", queue)
        try:
            routed = self.app.amqp.router.route(dict(options), self.name)
            return routed["queue"].name
        except Exception:  # pragma: no cover - defensive, routing is user code
            return self.app.conf.task_default_queue

    def _apply_health_routing(self, options, args, kwargs) -> None:
        from ..health.routing import decide

        queue = self._effective_queue(options)
        decision = decide(queue)
        if conf.LOG_DECISIONS:
            logger.info(
                "Adaptive decision for '%s': %s (queue=%s, reason=%s)",
                self.name,
                decision.action.name,
                queue,
                decision.reason,
            )
        if decision.should_reroute and decision.target_queue:
            options["queue"] = decision.target_queue

    def _acquire_dedup(self, args, kwargs) -> bool:
        redis = get_redis_client()
        if redis is None:
            logger.warning(
                "No Redis client configured; deduplication skipped for '%s'.",
                self.name,
            )
            return True
        from .idempotency import acquire_dedup_lock

        return acquire_dedup_lock(redis, self.name, args, kwargs)

    def _run_inline(self, args, kwargs, task_id, link, link_error) -> EagerResult:
        return self.apply(
            args, kwargs, task_id=task_id, link=link, link_error=link_error
        )

    def _handle_unavailable(
        self, policy, args, kwargs, task_id, link, link_error, *, reason: str
    ) -> AsyncResult | EagerResult:
        if policy.fallback_sync:
            logger.warning(
                "Broker unavailable (%s); running '%s' inline.", reason, self.name
            )
            return self._run_inline(args, kwargs, task_id, link, link_error)
        raise BrokerUnavailableError(f"Broker unavailable for '{self.name}': {reason}")
