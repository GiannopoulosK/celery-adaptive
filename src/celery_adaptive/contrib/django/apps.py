"""
Django AppConfig for celery-adaptive contrib.django.

Add to INSTALLED_APPS:
    "celery_adaptive.contrib.django"
"""

from django.apps import AppConfig


class DjangoCeleryAdaptiveConfig(AppConfig):
    name = "celery_adaptive.contrib.django"
    label = "celery_adaptive"
    verbose_name = "Celery Adaptive"

    def ready(self) -> None:
        # 1. Read CELERY_ADAPTIVE from settings and configure core
        from .conf import configure_from_django_settings

        configure_from_django_settings()

        # 2. Register the django-redis client for deduplication/health
        from .cache import register_django_redis_client

        register_django_redis_client()
