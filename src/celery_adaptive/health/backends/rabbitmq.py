"""
RabbitMQ health backend using HTTP Management API.
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("celery_adaptive.health.rabbitmq")


@dataclass
class QueueStats:
    name: str
    messages: int
    consumers: int
    message_rate: float

    @property
    def is_backed_up(self) -> bool:
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
    """Checks RabbitMQ queues via HTTP API."""

    def __init__(
        self,
        base_url: str = "http://localhost:15672",
        username: str = "guest",
        password: str = "guest",
        vhost: str = "%2F",
        queue_names: list[str] | None = None,
    ):
        self._base_url = base_url.rstrip("/")
        self._auth = (username, password)
        self._vhost = vhost
        self._queue_names = queue_names or []

    def check(self) -> RabbitMQHealthResult:
        try:
            import requests
        except ImportError:
            return RabbitMQHealthResult(
                healthy=False, error="requests library not installed"
            )

        try:
            url = f"{self._base_url}/api/queues/{self._vhost}"
            response = requests.get(url, auth=self._auth, timeout=5)
            response.raise_for_status()

            raw_queues = response.json()
            filtered = []

            filter_set = set(self._queue_names) if self._queue_names else None

            for q in raw_queues:
                name = q.get("name")
                if filter_set and name not in filter_set:
                    continue

                stats = QueueStats(
                    name=name,
                    messages=q.get("messages", 0),
                    consumers=q.get("consumers", 0),
                    message_rate=q.get("message_stats", {})
                    .get("publish_details", {})
                    .get("rate", 0.0),
                )
                filtered.append(stats)

            return RabbitMQHealthResult(healthy=True, queues=filtered)
        except Exception as exc:
            logger.error("RabbitMQ health check failed: %s", exc)
            return RabbitMQHealthResult(healthy=False, error=str(exc))
