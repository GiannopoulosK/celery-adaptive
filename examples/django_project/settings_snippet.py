# settings.py (excerpt)

INSTALLED_APPS = [
    # ...
    "celery_adaptive.contrib.django",
]

# Reused automatically by celery-adaptive when it is Redis-backed.
CACHES = {
    "default": {
        "BACKEND": "django.core.cache.backends.redis.RedisCache",
        "LOCATION": "redis://localhost:6379/1",
    }
}

CELERY_ADAPTIVE = {
    "FALLBACK": "outbox",
    "QUEUE_MAP": {"high": "critical", "default": "default", "low": "bulk"},
    "HEALTH": {
        "ENABLED": True,
        "QUEUE_DEPTH_CRITICAL": 5000,
        "REROUTE": {"default": "overflow"},
    },
}
