"""
RabbitMQ health backend using the HTTP management API.

Optional: queue depth is already available from the broker backend. This
adds consumer counts and publish rates. Requires ``requests``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from urllib.parse import quote

logger = logging.getLogger(__name__)


@dataclass
class QueueStats:
    name: str
    messages: int
    consumers: int
    message_rate: float

    @property
    def is_backed_up(self) -> bool:
        """Messages are waiting but nothing is consuming them."""
        return self.messages > 0 and self.consumers == 0


@dataclass
class RabbitMQHealthResult:
    healthy: bool
    queues: list[QueueStats] = field(default_factory=list)
    error: str | None = None

    def depth_for(self, queue_name: str) -> int | None:
        for q in self.queues:
            if q.name == queue_name:
                return q.messages
        return None

    @property
    def any_backed_up(self) -> bool:
        return any(q.is_backed_up for q in self.queues)


class RabbitMQHealthBackend:
    """Reads queue statistics from the RabbitMQ management API."""

    name = "rabbitmq"

    def __init__(
        self,
        base_url: str = "http://localhost:15672",
        username: str = "guest",
        password: str = "guest",
        vhost: str = "/",
        queue_names: list[str] | tuple[str, ...] | None = None,
        timeout: float = 2.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._auth = (username, password)
        self._vhost = quote(vhost, safe="")
        self._queue_names = set(queue_names or ())
        self._timeout = timeout

    def check(self) -> RabbitMQHealthResult:
        try:
            import requests
        except ImportError:
            return RabbitMQHealthResult(
                healthy=False,
                error="requests missing: pip install celery-adaptive[rabbitmq]",
            )

        try:
            response = requests.get(
                f"{self._base_url}/api/queues/{self._vhost}",
                auth=self._auth,
                timeout=self._timeout,
            )
            response.raise_for_status()
            queues = [
                QueueStats(
                    name=q.get("name"),
                    messages=q.get("messages") or 0,
                    consumers=q.get("consumers") or 0,
                    message_rate=(q.get("message_stats") or {})
                    .get("publish_details", {})
                    .get("rate", 0.0),
                )
                for q in response.json()
                if not self._queue_names or q.get("name") in self._queue_names
            ]
            return RabbitMQHealthResult(healthy=True, queues=queues)
        except Exception as exc:
            logger.warning("RabbitMQ health check failed: %s", exc)
            return RabbitMQHealthResult(healthy=False, error=str(exc))
