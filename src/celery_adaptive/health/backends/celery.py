"""
Celery worker health backend for celery-adaptive.
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("celery_adaptive.health.celery")

_INSPECT_TIMEOUT = 2.0  # seconds


@dataclass
class WorkerStats:
    hostname: str
    active_tasks: int
    scheduled_tasks: int

    @property
    def is_idle(self) -> bool:
        return self.active_tasks == 0


@dataclass
class CeleryHealthResult:
    healthy: bool
    workers: list[WorkerStats] = field(default_factory=list)
    total_active: int = 0
    error: str | None = None

    @property
    def worker_count(self) -> int:
        return len(self.workers)

    @property
    def overloaded(self) -> bool:
        # Overloaded if we have workers but none have idle concurrency
        return bool(self.workers) and all(w.active_tasks > 0 for w in self.workers)


class CeleryHealthBackend:
    """Checks Celery worker availability and backlog."""

    def __init__(self, celery_app, destination: list[str] | None = None):
        self._app = celery_app
        self._destination = destination

    def check(self) -> CeleryHealthResult:
        try:
            inspect = self._app.control.inspect(
                destination=self._destination, timeout=_INSPECT_TIMEOUT
            )
            ping_result = inspect.ping() or {}

            if not ping_result:
                logger.warning("Celery health check: no workers responded to ping")
                return CeleryHealthResult(
                    healthy=False, error="No Celery workers responded to ping"
                )

            active_map = inspect.active() or {}
            scheduled_map = inspect.scheduled() or {}

            workers = [
                WorkerStats(
                    hostname=hostname,
                    active_tasks=len(active_map.get(hostname, [])),
                    scheduled_tasks=len(scheduled_map.get(hostname, [])),
                )
                for hostname in ping_result
            ]

            total_active = sum(w.active_tasks for w in workers)

            if total_active > 0:
                logger.debug(
                    "Celery: %d active task(s) across %d worker(s)",
                    total_active,
                    len(workers),
                )

            return CeleryHealthResult(
                healthy=True, workers=workers, total_active=total_active
            )

        except Exception as exc:
            logger.error("Celery health check failed: %s", exc)
            return CeleryHealthResult(healthy=False, error=str(exc))
