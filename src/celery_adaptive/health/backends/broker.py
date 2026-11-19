"""
Broker health backend using the broker connection itself.

Queue depths come from a passive ``queue_declare``, which works for every
kombu transport Celery supports in production (RabbitMQ, Redis, SQS...)
without the RabbitMQ management plugin or extra dependencies.
"""

from __future__ import annotations

import contextlib
import logging
import time
from dataclasses import dataclass, field

from amqp.exceptions import NotFound
from kombu.exceptions import ChannelError

logger = logging.getLogger(__name__)


@dataclass
class BrokerHealthResult:
    healthy: bool
    latency_ms: float | None = None
    queue_depths: dict[str, int] = field(default_factory=dict)
    consumers: dict[str, int] = field(default_factory=dict)
    error: str | None = None

    def depth_for(self, queue: str) -> int | None:
        return self.queue_depths.get(queue)


class BrokerHealthBackend:
    """Checks broker reachability and the depth of the watched queues."""

    name = "broker"

    def __init__(self, app, queues: list[str] | tuple[str, ...] = ()):
        self._app = app
        self._queues = list(dict.fromkeys(queues))

    @property
    def queues(self) -> list[str]:
        return list(self._queues)

    def check(self) -> BrokerHealthResult:
        start = time.monotonic()
        try:
            with self._app.connection_for_write() as conn:
                conn.ensure_connection(max_retries=1)
                latency_ms = (time.monotonic() - start) * 1000
                depths: dict[str, int] = {}
                consumers: dict[str, int] = {}
                for queue in self._queues:
                    depth, consumer_count = self._inspect_queue(conn, queue)
                    depths[queue] = depth
                    consumers[queue] = consumer_count
            return BrokerHealthResult(
                healthy=True,
                latency_ms=round(latency_ms, 2),
                queue_depths=depths,
                consumers=consumers,
            )
        except Exception as exc:
            logger.warning("Broker health check failed: %s", exc)
            return BrokerHealthResult(healthy=False, error=str(exc))

    @staticmethod
    def _inspect_queue(conn, queue: str) -> tuple[int, int]:
        # A fresh channel per queue: AMQP closes the channel on a 404.
        channel = conn.channel()
        try:
            ok = channel.queue_declare(queue=queue, passive=True)
            return int(ok.message_count), int(ok.consumer_count)
        except (ChannelError, NotFound):
            # Queue not declared yet (or an empty Redis list): nothing waiting.
            return 0, 0
        finally:
            with contextlib.suppress(Exception):  # already closed by a 404
                channel.close()
