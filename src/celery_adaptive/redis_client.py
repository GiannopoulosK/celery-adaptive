"""
Redis client registry for celery-adaptive.

Decouples Redis access from any framework. A client is either registered
explicitly via ``setup(redis_client=...)`` / a framework integration, or
created lazily from the ``REDIS_URL`` setting on first use.
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Any

logger = logging.getLogger(__name__)

_redis_client: Any | None = None
_lock = threading.Lock()


def _reinit_lock_after_fork() -> None:
    global _lock
    _lock = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reinit_lock_after_fork)


def set_redis_client(client: Any) -> None:
    """Register a Redis client instance (a ``redis.Redis`` or compatible)."""
    global _redis_client

    if _redis_client is not None and _redis_client is not client:
        logger.debug("Replacing previously registered Redis client.")

    _redis_client = client


def get_redis_client() -> Any | None:
    """
    Return the Redis client, or ``None`` if none is available.

    Never raises: callers degrade gracefully when Redis is missing.
    """
    global _redis_client

    if _redis_client is not None:
        return _redis_client

    from .conf import conf

    url = conf.REDIS_URL
    if not url:
        return None

    with _lock:
        if _redis_client is None:
            try:
                import redis
            except ImportError:
                logger.warning(
                    "REDIS_URL is set but the 'redis' package is not installed. "
                    "Install celery-adaptive[redis]."
                )
                return None
            timeout = float(conf.BROKER_TIMEOUT)
            _redis_client = redis.Redis.from_url(
                url, socket_connect_timeout=timeout, socket_timeout=timeout
            )
    return _redis_client


def reset_redis_client() -> None:
    """Clear the registered Redis client. Intended for tests."""
    global _redis_client
    _redis_client = None
