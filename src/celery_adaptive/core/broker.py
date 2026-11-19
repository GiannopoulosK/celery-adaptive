"""
Broker connection tuning.

Celery's defaults favour patience over failing fast: AMQP connects time out
after 4 seconds, and the Redis transport has *no* socket timeout, so a
publish to an unreachable Redis host can block indefinitely. For a library
whose job is to react to broker outages, both are too slow. The helper here
bounds them by ``BROKER_TIMEOUT``.
"""

from __future__ import annotations

import logging

from ..conf import conf

logger = logging.getLogger(__name__)

_CELERY_DEFAULT_CONNECTION_TIMEOUT = 4
_REDIS_SCHEMES = ("redis", "rediss", "sentinel", "socket")
# Must stay above the Redis transport's 1s BRPOP polling interval, or idle
# workers would see spurious timeouts.
_MIN_REDIS_SOCKET_TIMEOUT = 5.0


def tune_broker_timeouts(app, timeout: float | None = None) -> None:
    """
    Bound broker connection time on ``app`` so outages are detected quickly.

    Only values the user has not customised are changed:

    * ``broker_connection_timeout`` when it is still Celery's default (4s).
    * ``socket_connect_timeout`` / ``socket_timeout`` in
      ``broker_transport_options`` for Redis brokers, if absent.

    Call this before the app opens its first broker connection.
    """
    timeout = float(timeout if timeout is not None else conf.BROKER_TIMEOUT)

    if app.conf.broker_connection_timeout == _CELERY_DEFAULT_CONNECTION_TIMEOUT:
        app.conf.broker_connection_timeout = timeout

    url = app.conf.broker_url or ""
    scheme = url.split("://", 1)[0].split("+", 1)[0].lower()
    if scheme in _REDIS_SCHEMES:
        options = dict(app.conf.broker_transport_options or {})
        options.setdefault("socket_connect_timeout", timeout)
        options.setdefault("socket_timeout", max(timeout, _MIN_REDIS_SOCKET_TIMEOUT))
        app.conf.broker_transport_options = options

    logger.debug("Tuned broker timeouts for app '%s' (timeout=%ss).", app.main, timeout)
