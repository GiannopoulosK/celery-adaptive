"""
Celery worker health backend (``celery inspect``).

Each check costs two broadcast round-trips bounded by ``INSPECT_TIMEOUT``,
so it only runs when ``HEALTH["CELERY_INSPECT"]`` is enabled.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class WorkerStats:
    hostname: str
    active_tasks: int
    max_concurrency: int | None = None

    @property
    def saturated(self) -> bool:
        """True when every pool slot on this worker is busy."""
        return bool(self.max_concurrency) and self.active_tasks >= self.max_concurrency


@dataclass
class CeleryHealthResult:
    healthy: bool
    workers: list[WorkerStats] = field(default_factory=list)
    error: str | None = None

    @property
    def worker_count(self) -> int:
        return len(self.workers)

    @property
    def total_active(self) -> int:
        return sum(w.active_tasks for w in self.workers)

    @property
    def saturated(self) -> bool:
        """True when every responding worker has no free pool slot."""
        return bool(self.workers) and all(w.saturated for w in self.workers)


class CeleryHealthBackend:
    """Checks that workers respond and how busy their pools are."""

    name = "celery"

    def __init__(
        self,
        celery_app,
        timeout: float = 1.0,
        destination: list[str] | None = None,
    ):
        self._app = celery_app
        self._timeout = timeout
        self._destination = destination

    def check(self) -> CeleryHealthResult:
        try:
            inspect = self._app.control.inspect(
                destination=self._destination, timeout=self._timeout
            )
            stats = inspect.stats() or {}
            if not stats:
                return CeleryHealthResult(
                    healthy=False, error="No Celery workers responded"
                )
            active = inspect.active() or {}
            workers = [
                WorkerStats(
                    hostname=hostname,
                    active_tasks=len(active.get(hostname) or []),
                    max_concurrency=(info.get("pool") or {}).get("max-concurrency"),
                )
                for hostname, info in stats.items()
            ]
            return CeleryHealthResult(healthy=True, workers=workers)
        except Exception as exc:
            logger.warning("Celery health check failed: %s", exc)
            return CeleryHealthResult(healthy=False, error=str(exc))
