"""
Background health monitoring.

:class:`HealthMonitor` runs the configured backends on a daemon thread every
``HEALTH["INTERVAL"]`` seconds and keeps the latest :class:`HealthSnapshot`.
Dispatch only reads that snapshot, so health checks never add latency to
``.delay()``. The monitor is fork-safe: a child process (e.g. a gunicorn or
Celery prefork worker) starts its own thread on first use.

When the broker check fails the monitor trips the broker's circuit breaker,
so producers skip straight to their fallback instead of waiting for a
publish timeout.
"""

from __future__ import annotations

import logging
import os
import threading
import time
import weakref
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from .backends.broker import BrokerHealthBackend, BrokerHealthResult
from .backends.celery import CeleryHealthBackend, CeleryHealthResult
from .backends.rabbitmq import RabbitMQHealthBackend, RabbitMQHealthResult
from .backends.redis import RedisHealthBackend, RedisHealthResult
from .decisions import Decision, DecisionEngine

logger = logging.getLogger(__name__)

STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_DOWN = "down"


@dataclass
class HealthSnapshot:
    broker: BrokerHealthResult | None = None
    redis: RedisHealthResult | None = None
    rabbitmq: RabbitMQHealthResult | None = None
    celery: CeleryHealthResult | None = None
    captured_at: float = field(default_factory=time.time)
    depth_warn: int = 500
    _monotonic: float = field(default_factory=time.monotonic, repr=False)

    def age_seconds(self) -> float:
        return time.monotonic() - self._monotonic

    def queue_depth(self, queue: str) -> int | None:
        if self.broker is not None and self.broker.healthy:
            depth = self.broker.depth_for(queue)
            if depth is not None:
                return depth
        if self.rabbitmq is not None and self.rabbitmq.healthy:
            return self.rabbitmq.depth_for(queue)
        return None

    @property
    def queue_depths(self) -> dict[str, int]:
        depths: dict[str, int] = {}
        if self.rabbitmq is not None and self.rabbitmq.healthy:
            depths.update({q.name: q.messages for q in self.rabbitmq.queues})
        if self.broker is not None and self.broker.healthy:
            depths.update(self.broker.queue_depths)
        return depths

    @property
    def issues(self) -> list[str]:
        """Human-readable list of everything that is not healthy."""
        return [
            *self._broker_issues(),
            *self._redis_issues(),
            *self._worker_issues(),
            *(
                f"queue '{queue}' backed up ({depth})"
                for queue, depth in sorted(self.queue_depths.items())
                if depth >= self.depth_warn
            ),
        ]

    def _broker_issues(self) -> list[str]:
        issues = []
        if self.broker is not None and not self.broker.healthy:
            issues.append(f"broker unreachable: {self.broker.error}")
        if self.rabbitmq is not None and not self.rabbitmq.healthy:
            issues.append(f"rabbitmq management API: {self.rabbitmq.error}")
        return issues

    def _redis_issues(self) -> list[str]:
        if self.redis is None:
            return []
        if not self.redis.healthy:
            return [f"redis unreachable: {self.redis.error}"]
        if self.redis.degraded:
            return [
                f"redis degraded (latency {self.redis.latency_ms}ms, "
                f"memory {self.redis.memory_used_pct}%)"
            ]
        return []

    def _worker_issues(self) -> list[str]:
        if self.celery is None:
            return []
        if not self.celery.healthy:
            return [f"workers: {self.celery.error}"]
        if self.celery.saturated:
            return ["all workers saturated"]
        return []

    @property
    def status(self) -> str:
        if self.broker is not None and not self.broker.healthy:
            return STATUS_DOWN
        return STATUS_DEGRADED if self.issues else STATUS_OK

    @property
    def healthy(self) -> bool:
        return self.status == STATUS_OK

    def to_dict(self) -> dict[str, Any]:
        def dump(result):
            return asdict(result) if result is not None else None

        celery = dump(self.celery)
        if celery is not None:
            celery["saturated"] = self.celery.saturated
        return {
            "status": self.status,
            "issues": self.issues,
            "captured_at": self.captured_at,
            "age_seconds": round(self.age_seconds(), 3),
            "queue_depths": self.queue_depths,
            "broker": dump(self.broker),
            "redis": dump(self.redis),
            "rabbitmq": dump(self.rabbitmq),
            "celery": celery,
        }


_monitors: weakref.WeakSet[HealthMonitor] = weakref.WeakSet()


def _after_fork_in_child() -> None:
    for monitor in list(_monitors):
        monitor._reinit_after_fork()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)


class HealthMonitor:
    """Runs health backends in the background and serves routing decisions."""

    def __init__(
        self,
        *,
        broker: BrokerHealthBackend | None = None,
        redis: RedisHealthBackend | None = None,
        rabbitmq: RabbitMQHealthBackend | None = None,
        celery: CeleryHealthBackend | None = None,
        interval: float = 5.0,
        stale_after: float = 30.0,
        depth_warn: int = 500,
        engine: DecisionEngine | None = None,
        breaker_key: str | None = None,
    ):
        self.broker = broker
        self.redis = redis
        self.rabbitmq = rabbitmq
        self.celery = celery
        self.interval = interval
        self.stale_after = stale_after
        self.depth_warn = depth_warn
        self.engine = engine or DecisionEngine()
        self.breaker_key = breaker_key
        self._snapshot: HealthSnapshot | None = None
        self._reinit_after_fork()
        _monitors.add(self)

    def _reinit_after_fork(self) -> None:
        self._lock = threading.Lock()
        self._check_lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._pid = os.getpid()

    # ------------------------------------------------------------ factory

    @classmethod
    def from_conf(
        cls,
        app,
        health: Mapping[str, Any],
        queue_map: Mapping[str, str] | None = None,
        redis_client=None,
    ) -> HealthMonitor:
        """Build a monitor from the ``HEALTH`` settings block."""
        from kombu.utils.url import maybe_sanitize_url

        watched = health["WATCHED_QUEUES"]
        if watched is None:
            watched = _default_watched_queues(app, health, queue_map or {})

        return cls(
            broker=BrokerHealthBackend(app, watched) if health["BROKER"] else None,
            redis=(
                RedisHealthBackend(
                    redis_client,
                    latency_warn_ms=health["REDIS_LATENCY_WARN_MS"],
                    memory_warn_pct=health["REDIS_MEMORY_WARN_PCT"],
                )
                if health["REDIS"] and redis_client is not None
                else None
            ),
            rabbitmq=(
                RabbitMQHealthBackend(
                    base_url=health["RABBITMQ_URL"],
                    username=health["RABBITMQ_USER"],
                    password=health["RABBITMQ_PASSWORD"],
                    vhost=health["RABBITMQ_VHOST"],
                    queue_names=watched,
                )
                if health["RABBITMQ_URL"]
                else None
            ),
            celery=(
                CeleryHealthBackend(app, timeout=health["INSPECT_TIMEOUT"])
                if health["CELERY_INSPECT"]
                else None
            ),
            interval=health["INTERVAL"],
            stale_after=health["STALE_AFTER"],
            depth_warn=health["QUEUE_DEPTH_WARN"],
            engine=DecisionEngine.from_conf(health),
            breaker_key=maybe_sanitize_url(app.conf.broker_url or "default"),
        )

    # ------------------------------------------------------------ checks

    def check_now(self) -> HealthSnapshot:
        """Run every backend synchronously and store the resulting snapshot."""
        with self._check_lock:
            snapshot = HealthSnapshot(
                broker=self.broker.check() if self.broker else None,
                redis=self.redis.check() if self.redis else None,
                rabbitmq=self.rabbitmq.check() if self.rabbitmq else None,
                celery=self.celery.check() if self.celery else None,
                depth_warn=self.depth_warn,
            )
            self._snapshot = snapshot

        if snapshot.broker is not None and not snapshot.broker.healthy:
            self._trip_breaker()

        try:
            from ..metrics.prometheus import update_health_metrics

            update_health_metrics(snapshot)
        except Exception as exc:  # pragma: no cover - metrics must never break
            logger.debug("Updating health metrics failed: %s", exc)

        if snapshot.status != STATUS_OK:
            logger.info("Health %s: %s", snapshot.status, "; ".join(snapshot.issues))
        return snapshot

    def _trip_breaker(self) -> None:
        if self.breaker_key is None:
            return
        from ..core.circuit import get_breaker

        get_breaker(self.breaker_key).trip()

    @property
    def snapshot(self) -> HealthSnapshot | None:
        """The latest snapshot, however old."""
        return self._snapshot

    def fresh_snapshot(self) -> HealthSnapshot | None:
        """The latest snapshot if younger than ``stale_after``, else None."""
        snapshot = self._snapshot
        if snapshot is None or snapshot.age_seconds() > self.stale_after:
            return None
        return snapshot

    def decide(self, queue: str) -> Decision:
        """Routing decision for ``queue``. Never blocks on health checks."""
        self.ensure_running()
        return self.engine.decide(self.fresh_snapshot(), queue)

    # ------------------------------------------------------------ thread

    def ensure_running(self) -> None:
        if self._pid != os.getpid():  # forked without register_at_fork
            self._reinit_after_fork()
        thread = self._thread
        if thread is not None and thread.is_alive():
            return
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="celery-adaptive-health", daemon=True
            )
            self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.check_now()
            except Exception:
                logger.exception("Health check iteration failed.")
            self._stop.wait(self.interval)

    def stop(self, timeout: float | None = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)
        self._thread = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()


def _default_watched_queues(
    app, health: Mapping[str, Any], queue_map: Mapping[str, str]
) -> Sequence[str]:
    queues = [*queue_map.values(), app.conf.task_default_queue]
    for source, target in health["REROUTE"].items():
        queues += [source, target]
    return tuple(dict.fromkeys(q for q in queues if q))
