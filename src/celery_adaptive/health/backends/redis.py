"""
Redis health backend for celery-adaptive.
"""

import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger("celery_adaptive.health.redis")


@dataclass
class RedisHealthResult:
    healthy: bool
    latency_ms: float | None = None
    memory_used_pct: float | None = None
    connected_clients: int | None = None
    error: str | None = None
    raw: dict = field(default_factory=dict)

    @property
    def degraded(self) -> bool:
        if not self.healthy:
            return False
        return (self.latency_ms is not None and self.latency_ms > 50) or (
            self.memory_used_pct is not None and self.memory_used_pct > 80
        )


class RedisHealthBackend:
    """Checks Redis using a redis-py client."""

    def __init__(self, client):
        self._client = client

    def check(self) -> RedisHealthResult:
        try:
            # 1. Latency
            start = time.monotonic()
            self._client.ping()
            latency_ms = (time.monotonic() - start) * 1000

            # 2. Memory
            mem_info = self._client.info("memory")
            used = mem_info.get("used_memory", 0)
            total = mem_info.get("maxmemory", 0)

            # Fallback: if no maxmemory set, we can't reliably calculate pct
            pct = (used / total * 100) if total > 0 else None

            # 3. Clients
            client_info = self._client.info("clients")
            connected = client_info.get("connected_clients")

            return RedisHealthResult(
                healthy=True,
                latency_ms=round(latency_ms, 2),
                memory_used_pct=round(pct, 1) if pct is not None else None,
                connected_clients=connected,
                raw=mem_info,
            )
        except Exception as exc:
            logger.error("Redis health check failed: %s", exc)
            return RedisHealthResult(healthy=False, error=str(exc))
