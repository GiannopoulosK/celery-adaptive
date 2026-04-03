"""
Django-redis client helper for celery-adaptive contrib.django.

Registers the django-redis native client so celery_adaptive's
deduplication and health backends can use it without any framework
coupling in core.
"""

import logging

logger = logging.getLogger("celery_adaptive.contrib.django.cache")


def register_django_redis_client() -> None:
    """
    Pull the write client from Django's default cache and register it
    with celery_adaptive's redis_client module.

    Called automatically by DjangoCeleryAdaptiveConfig.ready().
    Silently skips if the default cache is not a django-redis backend.
    """
    try:
        from django.core.cache import cache

        from celery_adaptive.redis_client import set_redis_client

        client = cache.client.get_client(write=True)
        set_redis_client(client)
        logger.debug("Registered django-redis client with celery_adaptive.")
    except Exception as exc:
        logger.warning(
            "Could not register django-redis client — Redis features will be "
            "skipped unless you call celery_adaptive.setup(redis_client=...) "
            "manually: %s",
            exc,
        )
