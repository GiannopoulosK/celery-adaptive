"""Reuse the Redis client behind a Django cache for celery-adaptive."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def _client_from_cache(cache) -> Any | None:
    # django-redis
    client = getattr(cache, "client", None)
    if client is not None and hasattr(client, "get_client"):
        return client.get_client(write=True)
    # Django's built-in RedisCache (django.core.cache.backends.redis)
    inner = getattr(cache, "_cache", None)
    if inner is not None and hasattr(inner, "get_client"):
        return inner.get_client(None, write=True)
    return None


def register_django_redis_client(alias: str = "default") -> bool:
    """
    Register the Redis client behind cache ``alias``, if it is Redis-backed.

    Supports django-redis and Django's built-in ``RedisCache``. Skipped when
    ``REDIS_URL`` is configured or a client is already registered. Returns
    True if a client was registered.
    """
    from celery_adaptive.conf import conf
    from celery_adaptive.redis_client import _redis_client, set_redis_client

    if conf.REDIS_URL or _redis_client is not None:
        return False

    try:
        from django.core.cache import caches

        client = _client_from_cache(caches[alias])
    except Exception as exc:
        logger.debug("Could not read Redis client from cache '%s': %s", alias, exc)
        client = None

    if client is None:
        logger.debug(
            "Cache '%s' is not Redis-backed; set CELERY_ADAPTIVE['REDIS_URL'] to "
            "enable deduplication, batching and the outbox.",
            alias,
        )
        return False

    set_redis_client(client)
    logger.debug("Registered Redis client from Django cache '%s'.", alias)
    return True
