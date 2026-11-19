"""
``AdaptiveTask``: a :class:`celery.Task` with failure-aware dispatch.

Because it is a real Celery task class, everything Celery offers keeps
working: signatures (``.s()``, ``.si()``), chains, groups, chords,
``bind=True``, ``self.retry()``, task routing and eager mode. Only
:meth:`apply_async` is extended:

1. Resolve the target queue from the task's ``adaptive_priority``.
2. Deduplicate identical in-flight calls (optional).
3. ``FORCE_SYNC``: run inline and return an ``EagerResult``.
4. Consult the health monitor, which may reroute the task (optional).
5. Publish, guarded by a per-broker circuit breaker with a bounded retry
   policy. If the broker is unreachable, apply the fallback strategy:
   ``"sync"`` (run inline), ``"outbox"`` (store in Redis, replay later) or
   ``"raise"`` (:class:`~celery_adaptive.BrokerUnavailableError`).

Use it through :func:`celery_adaptive.adaptive_task` or directly::

    @app.task(base=AdaptiveTask, adaptive_priority="high")
    def send_email(user_id): ...
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from celery import Task, states
from celery.result import AsyncResult, EagerResult
from celery.utils import uuid
from kombu.exceptions import OperationalError
from kombu.utils.url import maybe_sanitize_url

from ..conf import FALLBACK_STRATEGIES, conf
from ..exceptions import BrokerUnavailableError, ConfigurationError
from ..redis_client import get_redis_client
from .circuit import get_breaker
from .idempotency import DEDUP_HEADER, DedupKeyFunc, acquire, make_dedup_key, release

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

_OVERRIDABLE = frozenset({"priority", "deduplicate", "fallback"})


@dataclass(frozen=True)
class DispatchPolicy:
    """Effective adaptive options for a single dispatch."""

    priority: str | None
    deduplicate: bool
    fallback: str


@dataclass
class _Dispatch:
    """Everything needed to publish, or fall back for, one task call."""

    args: tuple
    kwargs: dict
    task_id: str
    link: Any
    link_error: Any
    options: dict[str, Any]


def _request_header(request, name: str) -> Any:
    # Workers expose custom headers as request attributes; eager apply()
    # only keeps them in request.headers.
    value = getattr(request, name, None)
    if value is None:
        value = (getattr(request, "headers", None) or {}).get(name)
    return value


class AdaptiveTask(Task):
    """Celery task base class with adaptive, failure-aware dispatch."""

    #: Key into the ``QUEUE_MAP`` setting. ``None`` leaves queue selection
    #: to Celery (``queue=`` option, ``task_routes`` or the default queue).
    adaptive_priority: str | None = None
    #: Suppress duplicate dispatches while an identical task is in flight.
    adaptive_deduplicate: bool = False
    #: Optional ``f(*args, **kwargs) -> value`` choosing which arguments
    #: identify a duplicate. Defaults to all of them.
    adaptive_dedup_key: DedupKeyFunc | None = None
    #: What to do when the broker is unreachable: ``"sync"``, ``"outbox"``
    #: or ``"raise"``. ``None`` uses the ``FALLBACK`` setting.
    adaptive_fallback: str | None = None

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
        call = _Dispatch(
            args=tuple(args or ()),
            kwargs=dict(kwargs or {}),
            task_id=task_id or uuid(),
            link=link,
            link_error=link_error,
            options=options,
        )
        if shadow is not None:
            options["shadow"] = shadow

        if policy.priority is not None and "queue" not in options:
            options["queue"] = conf.QUEUE_MAP[policy.priority]

        if policy.deduplicate and not conf.DISABLE_DEDUPLICATION:
            holder = self._claim_dedup(call)
            if holder is not None:
                logger.debug(
                    "Duplicate '%s' suppressed; returning in-flight task %s.",
                    self.name,
                    holder,
                )
                self._on_dedup_hit()
                return self.AsyncResult(holder)

        if conf.FORCE_SYNC:
            logger.debug("FORCE_SYNC: running '%s' inline.", self.name)
            return self._run_inline(call)

        if conf.HEALTH.get("ENABLED"):
            self._apply_health_routing(options)

        options.setdefault("retry_policy", DEFAULT_PUBLISH_RETRY_POLICY)
        breaker = get_breaker(self._broker_key())

        if not breaker.allow():
            return self._handle_unavailable(policy, call, reason="circuit open")

        try:
            result = super().apply_async(
                call.args,
                call.kwargs,
                task_id=call.task_id,
                producer=producer,
                link=link,
                link_error=link_error,
                **options,
            )
        except BROKER_ERRORS as exc:
            breaker.record_failure()
            logger.warning("Publishing '%s' failed: %s", self.name, exc)
            return self._handle_unavailable(policy, call, reason=str(exc))

        if breaker.record_success():
            self._on_broker_recovered()
        self._on_published(options)
        return result

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        """Release the deduplication key once the task reaches a final state.

        Subclasses overriding this must call ``super().after_return(...)``.
        """
        super().after_return(status, retval, task_id, args, kwargs, einfo)
        if status not in states.READY_STATES:
            return
        key = _request_header(self.request, DEDUP_HEADER)
        if key:
            redis = get_redis_client()
            if redis is not None:
                release(redis, key, task_id)

    # ------------------------------------------------------------ internals

    def _resolve_policy(self, overrides: dict[str, Any] | None) -> DispatchPolicy:
        values: dict[str, Any] = {
            "priority": self.adaptive_priority,
            "deduplicate": self.adaptive_deduplicate,
            "fallback": self.adaptive_fallback,
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
        if values["fallback"] is None:
            values["fallback"] = conf.FALLBACK
        if values["fallback"] not in FALLBACK_STRATEGIES:
            raise ConfigurationError(
                f"Invalid fallback '{values['fallback']}' for '{self.name}'. "
                f"Valid: {sorted(FALLBACK_STRATEGIES)}."
            )
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

    def _apply_health_routing(self, options: dict[str, Any]) -> None:
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

    # -- deduplication

    def _claim_dedup(self, call: _Dispatch) -> str | None:
        """Claim the dedup key; return the holder's task id for duplicates."""
        redis = get_redis_client()
        if redis is None:
            logger.warning(
                "No Redis client configured; deduplication skipped for '%s'.",
                self.name,
            )
            return None
        # Read from the class so a plain function isn't bound as a method.
        key_func = type(self).adaptive_dedup_key
        key = make_dedup_key(self.name, call.args, call.kwargs, key_func)
        holder = acquire(redis, key, call.task_id)
        if holder is None:
            call.options["headers"] = {
                **(call.options.get("headers") or {}),
                DEDUP_HEADER: key,
            }
        return holder

    def _release_dedup(self, call: _Dispatch) -> None:
        key = (call.options.get("headers") or {}).get(DEDUP_HEADER)
        redis = get_redis_client()
        if key and redis is not None:
            release(redis, key, call.task_id)

    # -- fallbacks

    def _run_inline(self, call: _Dispatch) -> EagerResult:
        return self.apply(
            call.args,
            call.kwargs,
            task_id=call.task_id,
            link=call.link,
            link_error=call.link_error,
            headers=call.options.get("headers"),
        )

    def _handle_unavailable(
        self, policy: DispatchPolicy, call: _Dispatch, *, reason: str
    ) -> AsyncResult | EagerResult:
        self._on_fallback(policy.fallback, call)

        if policy.fallback == "sync":
            logger.warning(
                "Broker unavailable (%s); running '%s' inline.", reason, self.name
            )
            return self._run_inline(call)

        if policy.fallback == "outbox":
            from . import outbox

            options = dict(call.options)
            if call.link is not None:
                options["link"] = call.link
            if call.link_error is not None:
                options["link_error"] = call.link_error
            try:
                outbox.store(self.name, call.task_id, call.args, call.kwargs, options)
            except Exception as exc:
                self._release_dedup(call)
                raise BrokerUnavailableError(
                    f"Broker unavailable for '{self.name}' ({reason}) and the "
                    f"outbox could not store it: {exc}"
                ) from exc
            logger.warning(
                "Broker unavailable (%s); stored '%s' [%s] in the outbox.",
                reason,
                self.name,
                call.task_id,
            )
            return self.AsyncResult(call.task_id)

        self._release_dedup(call)
        raise BrokerUnavailableError(f"Broker unavailable for '{self.name}': {reason}")

    # -- hooks (metrics, outbox replay)

    def _on_published(self, options: dict[str, Any]) -> None:
        pass

    def _on_dedup_hit(self) -> None:
        pass

    def _on_fallback(self, strategy: str, call: _Dispatch) -> None:
        pass

    def _on_broker_recovered(self) -> None:
        if not conf.OUTBOX_AUTO_REPLAY:
            return
        from . import outbox

        try:
            if outbox.pending():
                outbox.replay_in_background(self.app)
        except Exception as exc:
            logger.debug("Outbox check after broker recovery failed: %s", exc)
