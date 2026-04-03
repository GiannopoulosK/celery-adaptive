"""
Redis client registry for celery-adaptive.

Decouples Redis access from any framework. The client is registered
once via setup(redis_client=...) and retrieved here by internal code.

Django users: celery_adaptive.contrib.django may register a client
automatically via AppConfig.ready().
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

_redis_client: Any | None = None


def set_redis_client(client: Any) -> None:
    """
    Register a Redis client instance.

    Typically called via setup(redis_client=...) or a framework integration.
    """
    global _redis_client

    if _redis_client is not None:
        logger.warning("Redis client is already set — overriding existing client.")

    _redis_client = client


def get_redis_client() -> Any | None:
    """
    Return the registered Redis client, or None if unavailable.

    Never raises — callers should degrade gracefully if Redis is missing.
    """
    if _redis_client is None:
        logger.debug("No Redis client registered — Redis features will be skipped.")
    return _redis_client


def reset_redis_client() -> None:
    """
    Clear the registered Redis client.

    Intended for tests.
    """
    global _redis_client
    _redis_client = None
