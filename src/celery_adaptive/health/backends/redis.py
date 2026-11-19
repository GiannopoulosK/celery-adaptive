"""Redis health backend (latency, memory, clients) for a redis-py client."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class RedisHealthResult:
    healthy: bool
    latency_ms: float | None = None
    memory_used_pct: float | None = None
    connected_clients: int | None = None
    degraded: bool = False
    error: str | None = None


class RedisHealthBackend:
    """Checks the Redis used for deduplication, batching and the outbox."""

    name = "redis"

    def __init__(
        self,
        client,
        latency_warn_ms: float = 50.0,
        memory_warn_pct: float = 80.0,
    ):
        self._client = client
        self._latency_warn_ms = latency_warn_ms
        self._memory_warn_pct = memory_warn_pct

    def check(self) -> RedisHealthResult:
        try:
            start = time.monotonic()
            self._client.ping()
            latency_ms = round((time.monotonic() - start) * 1000, 2)

            pct, clients = self._memory_and_clients()

            degraded = latency_ms >= self._latency_warn_ms or (
                pct is not None and pct >= self._memory_warn_pct
            )
            return RedisHealthResult(
                healthy=True,
                latency_ms=latency_ms,
                memory_used_pct=pct,
                connected_clients=clients,
                degraded=degraded,
            )
        except Exception as exc:
            logger.warning("Redis health check failed: %s", exc)
            return RedisHealthResult(healthy=False, error=str(exc))

    def _memory_and_clients(self) -> tuple[float | None, int | None]:
        # INFO is disabled on some managed Redis offerings; PING succeeding
        # is enough to call Redis healthy.
        try:
            memory = self._client.info("memory")
            clients = self._client.info("clients").get("connected_clients")
        except Exception as exc:
            logger.debug("Redis INFO unavailable: %s", exc)
            return None, None
        used = memory.get("used_memory", 0)
        limit = memory.get("maxmemory", 0)
        # Without maxmemory there is no meaningful percentage.
        pct = round(used / limit * 100, 1) if limit else None
        return pct, clients
