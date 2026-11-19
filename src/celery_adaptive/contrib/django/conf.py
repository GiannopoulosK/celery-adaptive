"""
Django settings bridge for celery-adaptive.

Reads ``settings.CELERY_ADAPTIVE`` (a dict) and calls
``celery_adaptive.setup(config=...)`` so application code never has to.

Place this in INSTALLED_APPS via the AppConfig, not directly.
"""

from django.conf import settings


def configure_from_django_settings() -> None:
    """
    Read CELERY_ADAPTIVE from Django settings and configure celery_adaptive.

    Raises ConfigurationError on invalid settings (propagates to startup).

    Example settings.py:
        CELERY_ADAPTIVE = {
            "DEDUPLICATION_TTL": 600,
            "QUEUE_MAP": {
                "high": "critical",
                "default": "default",
                "low": "bulk",
            },
            "HEALTH": {
                "RABBITMQ_URL": "http://localhost:15672",
                "WATCHED_QUEUES": ["default", "critical"],
            },
        }
    """
    import celery_adaptive

    user_conf = getattr(settings, "CELERY_ADAPTIVE", {})
    celery_adaptive.setup(config=user_conf)
