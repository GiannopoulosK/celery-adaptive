"""Django settings bridge: ``settings.CELERY_ADAPTIVE`` -> ``setup()``."""

from django.conf import settings


def configure_from_django_settings() -> None:
    """
    Configure celery-adaptive from ``settings.CELERY_ADAPTIVE``.

    Invalid settings raise ConfigurationError at startup. Example::

        CELERY_ADAPTIVE = {
            "REDIS_URL": "redis://localhost:6379/1",
            "FALLBACK": "outbox",
            "QUEUE_MAP": {"high": "critical", "default": "default", "low": "bulk"},
            "HEALTH": {"ENABLED": True, "REROUTE": {"default": "overflow"}},
        }
    """
    import celery_adaptive

    celery_adaptive.setup(config=getattr(settings, "CELERY_ADAPTIVE", {}))
