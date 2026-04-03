"""
HealthChecker orchestrates all health backends and caches the composite result.
"""

import logging
import threading
import time
from dataclasses import dataclass

from .backends.celery import CeleryHealthBackend, CeleryHealthResult
from .backends.rabbitmq import RabbitMQHealthBackend, RabbitMQHealthResult
from .backends.redis import RedisHealthBackend, RedisHealthResult
from .decisions import Decision, DecisionEngine
from .thresholds import load_thresholds

logger = logging.getLogger("celery_adaptive.health.checker")

_DEFAULT_CACHE_TTL = 10


@dataclass
class HealthSnapshot:
    redis: RedisHealthResult | None
    rabbitmq: RabbitMQHealthResult | None
    celery: CeleryHealthResult | None
    captured_at: float

    def age_seconds(self) -> float:
        return time.monotonic() - self.captured_at

    @property
    def all_healthy(self) -> bool:
        return all(
            c is None or c.healthy for c in [self.redis, self.rabbitmq, self.celery]
        )


class HealthChecker:
    """Thread-safe health checker with TTL-cached snapshots."""

    def __init__(
        self,
        redis_backend: RedisHealthBackend | None = None,
        rabbitmq_backend: RabbitMQHealthBackend | None = None,
        celery_backend: CeleryHealthBackend | None = None,
        cache_ttl: float = _DEFAULT_CACHE_TTL,
        health_conf: dict | None = None,
    ):
        self._redis_backend = redis_backend
        self._rabbitmq_backend = rabbitmq_backend
        self._celery_backend = celery_backend
        self._cache_ttl = cache_ttl
        self._snapshot: HealthSnapshot | None = None
        self._lock = threading.Lock()
        self._thresholds = load_thresholds(health_conf)
        self._engine = DecisionEngine(self._thresholds)

    @classmethod
    def from_conf(
        cls, health_conf: dict | None = None, redis_client=None
    ) -> "HealthChecker":
        """
        Build a HealthChecker from a plain config dict and an optional
        redis-py client. No Django imports.

        Args:
            health_conf: The HEALTH sub-dict from celery_adaptive conf, e.g.
                {
                    "CACHE_TTL": 10,
                    "WATCHED_QUEUES": ["default"],
                    "RABBITMQ_URL": "http://localhost:15672",
                    "CELERY_APP": "myproject.celery:app",
                }
            redis_client: A redis-py client. If None, Redis backend is skipped.
        """
        health_conf = health_conf or {}
        cache_ttl = health_conf.get("CACHE_TTL", 10)
        watched = health_conf.get("WATCHED_QUEUES", ["default"])

        # Redis backend
        redis_backend = RedisHealthBackend(redis_client) if redis_client else None

        # RabbitMQ backend
        rabbitmq_backend = None
        if "RABBITMQ_URL" in health_conf:
            rabbitmq_backend = RabbitMQHealthBackend(
                base_url=health_conf["RABBITMQ_URL"],
                username=health_conf.get("RABBITMQ_USER", "guest"),
                password=health_conf.get("RABBITMQ_PASSWORD", "guest"),
                queue_names=watched,
            )

        # Celery backend
        celery_backend = None
        celery_app_path = health_conf.get("CELERY_APP")
        if celery_app_path:
            try:
                import importlib

                module_path, attr = celery_app_path.rsplit(":", 1)
                module = importlib.import_module(module_path)
                celery_backend = CeleryHealthBackend(getattr(module, attr))
            except Exception as exc:
                logger.debug("Backend init failed for '%s': %s", celery_app_path, exc)
        else:
            try:
                from celery import current_app

                if current_app.conf.broker_url:
                    celery_backend = CeleryHealthBackend(current_app)
            except Exception as exc:
                logger.debug("Current_app backend init failed: %s", exc)

        return cls(
            redis_backend=redis_backend,
            rabbitmq_backend=rabbitmq_backend,
            celery_backend=celery_backend,
            cache_ttl=cache_ttl,
            health_conf=health_conf,
        )

    def get_snapshot(self, force: bool = False) -> HealthSnapshot:
        if (
            not force
            and self._snapshot
            and self._snapshot.age_seconds() < self._cache_ttl
        ):
            return self._snapshot

        with self._lock:
            if (
                not force
                and self._snapshot
                and self._snapshot.age_seconds() < self._cache_ttl
            ):
                return self._snapshot
            self._snapshot = self._run_checks()
            return self._snapshot

    def decide(self, queue: str = "default") -> Decision:
        snapshot = self.get_snapshot()
        decision = self._engine.decide(
            redis=snapshot.redis,
            rabbitmq=snapshot.rabbitmq,
            celery=snapshot.celery,
            queue_name=queue,
        )
        if decision.action.name != "ENQUEUE":
            logger.info(
                "Health routing: %s — %s (queue=%s)",
                decision.action.name,
                decision.reason,
                queue,
            )
        return decision

    def _run_checks(self) -> HealthSnapshot:
        snapshot = HealthSnapshot(
            redis=self._redis_backend.check() if self._redis_backend else None,
            rabbitmq=self._rabbitmq_backend.check() if self._rabbitmq_backend else None,
            celery=self._celery_backend.check() if self._celery_backend else None,
            captured_at=time.monotonic(),
        )
        try:
            from ..metrics.prometheus import update_health_metrics

            update_health_metrics(snapshot)
        except (ImportError, Exception):
            pass
        return snapshot
