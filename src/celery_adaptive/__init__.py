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
from .core.task import AdaptiveTask
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
    "AdaptiveTask",
    "CeleryAdaptiveError",
    "ConfigurationError",
    "BrokerUnavailableError",
]


def setup(
    *,
    config: dict | None = None,
    redis_client=None,
    app=None,
) -> None:
    """
    Configure celery-adaptive. Call once at application startup.

    Args:
        config: Settings overriding :data:`celery_adaptive.conf.DEFAULTS`.
        redis_client: A ``redis.Redis`` client used for deduplication,
            batching and the outbox. Alternatively set ``REDIS_URL``.
        app: Your Celery app. When given, broker connection timeouts are
            bounded by ``BROKER_TIMEOUT`` so outages are detected in about a
            second instead of 4s (AMQP) or never (Redis transport). See
            :func:`celery_adaptive.core.broker.tune_broker_timeouts`.

    Calling ``setup()`` again replaces the previous configuration.
    """
    settings.configure(config or {})
    reset_breakers()
    from .health.routing import reset_checker

    reset_checker()

    if redis_client is not None:
        set_redis_client(redis_client)

    if app is not None:
        from .core.broker import tune_broker_timeouts

        tune_broker_timeouts(app)


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
