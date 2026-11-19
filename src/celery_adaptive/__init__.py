"""
celery-adaptive: health-aware Celery task routing.

Quickstart:
    import celery_adaptive
    celery_adaptive.setup(config={...}, redis_client=...)

Django users: use celery_adaptive.contrib.django instead.
"""

from .conf import conf as settings
from .core.circuit import reset_breakers
from .core.decorators import adaptive_task
from .exceptions import (
    BrokerUnavailableError,
    CeleryAdaptiveError,
    ConfigurationError,
)
from .redis_client import set_redis_client

__all__ = [
    "setup",
    "settings",
    "adaptive_task",
    "CeleryAdaptiveError",
    "ConfigurationError",
    "BrokerUnavailableError",
]


def setup(
    *,
    config: dict | None = None,
    redis_client=None,
) -> None:
    """
    Configure celery-adaptive. Call once at application startup.

    Args:
        config: Settings overriding :data:`celery_adaptive.conf.DEFAULTS`.
        redis_client: A ``redis.Redis`` client used for deduplication,
            batching and the outbox. Alternatively set ``REDIS_URL``.

    Calling ``setup()`` again replaces the previous configuration.
    """
    settings.configure(config or {})
    reset_breakers()

    if redis_client is not None:
        set_redis_client(redis_client)


def __getattr__(name):
    if name == "__version__":
        from importlib.metadata import PackageNotFoundError, version

        try:
            return version("celery-adaptive")
        except PackageNotFoundError:
            return "0.0.0.dev0"
    raise AttributeError(f"module {__name__} has no attribute {name}")


def __dir__():
    return sorted(__all__ + ["__version__"])
