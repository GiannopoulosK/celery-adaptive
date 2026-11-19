"""
Configuration for celery-adaptive.

Settings are optional. Until :func:`celery_adaptive.setup` is called the
library runs on :data:`DEFAULTS`. Call ``setup()`` once at startup to
override them:

    from celery_adaptive import setup
    setup(config={
        "DEDUPLICATION_TTL": 600,
        "QUEUE_MAP": {"high": "critical", "default": "default", "low": "bulk"},
    })

In Django projects, the ``celery_adaptive.contrib.django`` app calls
``setup()`` for you from ``settings.CELERY_ADAPTIVE``.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any

from .exceptions import ConfigurationError

PREFIX = "celery_adaptive:"

_NONE = type(None)

DEFAULTS: dict[str, Any] = {
    # Redis used for deduplication, batching and the outbox. Ignored when a
    # client is registered explicitly via setup(redis_client=...).
    "REDIS_URL": None,
    # Deduplication
    "DEDUPLICATION_TTL": 300,
    "DEDUP_KEY_PREFIX": "cadp:",
    "DISABLE_DEDUPLICATION": False,
    # Dispatch
    # What to do when the broker is unreachable: "sync" runs the task inline,
    # "outbox" stores it in Redis for replay, "raise" raises
    # BrokerUnavailableError.
    "FALLBACK": "sync",
    "OUTBOX_KEY": "cadp:outbox",
    "OUTBOX_AUTO_REPLAY": True,
    "FORCE_SYNC": False,
    "BROKER_TIMEOUT": 1.0,
    "LOG_DECISIONS": False,
    "CIRCUIT_BREAKER_THRESHOLD": 3,
    "CIRCUIT_BREAKER_RESET": 30.0,
    "QUEUE_MAP": {
        "high": "high_priority",
        "default": "default",
        "low": "low_priority",
    },
    # Health monitoring, see HEALTH_DEFAULTS and celery_adaptive.health
    "HEALTH": {},
}

EXPECTED_TYPES: dict[str, type | tuple[type, ...]] = {
    "REDIS_URL": (str, _NONE),
    "DEDUPLICATION_TTL": int,
    "DEDUP_KEY_PREFIX": str,
    "DISABLE_DEDUPLICATION": bool,
    "FALLBACK": str,
    "OUTBOX_KEY": str,
    "OUTBOX_AUTO_REPLAY": bool,
    "FORCE_SYNC": bool,
    "BROKER_TIMEOUT": (int, float),
    "LOG_DECISIONS": bool,
    "CIRCUIT_BREAKER_THRESHOLD": int,
    "CIRCUIT_BREAKER_RESET": (int, float),
    "QUEUE_MAP": dict,
    "HEALTH": dict,
}

HEALTH_DEFAULTS: dict[str, Any] = {
    # Monitoring is opt-in: it runs a background thread in each process.
    "ENABLED": False,
    # Seconds between background checks.
    "INTERVAL": 5.0,
    # Ignore snapshots older than this when making routing decisions.
    "STALE_AFTER": 30.0,
    # Queues whose depth is measured. None: the QUEUE_MAP queues, the
    # Celery default queue and all REROUTE sources and targets.
    "WATCHED_QUEUES": None,
    "QUEUE_DEPTH_WARN": 500,
    "QUEUE_DEPTH_CRITICAL": 2000,
    # {"source_queue": "overflow_queue"}: when the source reaches
    # QUEUE_DEPTH_CRITICAL, new tasks go to the overflow queue instead
    # (as long as it is less backed up).
    "REROUTE": {},
    # Broker connectivity and queue depth via the Celery connection.
    "BROKER": True,
    # Latency/memory of the registered Redis client (reporting only).
    "REDIS": True,
    "REDIS_LATENCY_WARN_MS": 50.0,
    "REDIS_MEMORY_WARN_PCT": 80.0,
    # Worker stats via `celery inspect` broadcasts. Costs a broadcast
    # round-trip per check, so it is off by default.
    "CELERY_INSPECT": False,
    "INSPECT_TIMEOUT": 1.0,
    # RabbitMQ management API (optional, adds consumer/rate details).
    "RABBITMQ_URL": None,
    "RABBITMQ_USER": "guest",
    "RABBITMQ_PASSWORD": "guest",
    "RABBITMQ_VHOST": "/",
}

HEALTH_TYPES: dict[str, type | tuple[type, ...]] = {
    "ENABLED": bool,
    "INTERVAL": (int, float),
    "STALE_AFTER": (int, float),
    "WATCHED_QUEUES": (list, tuple, _NONE),
    "QUEUE_DEPTH_WARN": int,
    "QUEUE_DEPTH_CRITICAL": int,
    "REROUTE": dict,
    "BROKER": bool,
    "REDIS": bool,
    "REDIS_LATENCY_WARN_MS": (int, float),
    "REDIS_MEMORY_WARN_PCT": (int, float),
    "CELERY_INSPECT": bool,
    "INSPECT_TIMEOUT": (int, float),
    "RABBITMQ_URL": (str, _NONE),
    "RABBITMQ_USER": str,
    "RABBITMQ_PASSWORD": str,
    "RABBITMQ_VHOST": str,
}

REQUIRED_PRIORITIES = frozenset({"high", "default", "low"})
FALLBACK_STRATEGIES = frozenset({"sync", "outbox", "raise"})


def _type_name(expected: type | tuple[type, ...]) -> str:
    types = expected if isinstance(expected, tuple) else (expected,)
    return " or ".join("None" if t is _NONE else t.__name__ for t in types)


def _check_type(key: str, value: Any, expected: type | tuple[type, ...]) -> None:
    types = expected if isinstance(expected, tuple) else (expected,)
    # bool is a subclass of int; don't let True pass as a number.
    if isinstance(value, bool) and bool not in types:
        ok = False
    else:
        ok = isinstance(value, types)
    if not ok:
        raise ConfigurationError(
            f"{PREFIX} {key} must be {_type_name(expected)}, "
            f"got {type(value).__name__}."
        )


def validate_health(user_health: dict) -> dict[str, Any]:
    """Validate the ``HEALTH`` block and merge it over :data:`HEALTH_DEFAULTS`."""
    unknown = set(user_health) - set(HEALTH_DEFAULTS)
    if unknown:
        raise ConfigurationError(
            f"{PREFIX} unknown HEALTH keys: {sorted(unknown)}. "
            f"Valid: {sorted(HEALTH_DEFAULTS)}."
        )
    for key, value in user_health.items():
        _check_type(f"HEALTH.{key}", value, HEALTH_TYPES[key])

    health = {**HEALTH_DEFAULTS, **user_health}
    for key in ("INTERVAL", "STALE_AFTER", "INSPECT_TIMEOUT"):
        if health[key] <= 0:
            raise ConfigurationError(f"{PREFIX} HEALTH.{key} must be positive.")
    if health["QUEUE_DEPTH_WARN"] > health["QUEUE_DEPTH_CRITICAL"]:
        raise ConfigurationError(
            f"{PREFIX} HEALTH.QUEUE_DEPTH_WARN must not exceed QUEUE_DEPTH_CRITICAL."
        )
    for source, target in health["REROUTE"].items():
        if not (isinstance(source, str) and isinstance(target, str)):
            raise ConfigurationError(
                f"{PREFIX} HEALTH.REROUTE must map queue names to queue names."
            )
        if source == target:
            raise ConfigurationError(
                f"{PREFIX} HEALTH.REROUTE cannot send '{source}' to itself."
            )
    if health["WATCHED_QUEUES"] is not None:
        health["WATCHED_QUEUES"] = tuple(health["WATCHED_QUEUES"])
    health["REROUTE"] = MappingProxyType(dict(health["REROUTE"]))
    return health


def validate(user_settings: dict) -> dict[str, Any]:
    """Validate ``user_settings`` and return them merged over the defaults."""
    if not isinstance(user_settings, dict):
        raise ConfigurationError(f"{PREFIX} config must be a dict.")

    unknown = set(user_settings) - set(DEFAULTS)
    if unknown:
        raise ConfigurationError(
            f"{PREFIX} unknown keys: {sorted(unknown)}. Valid: {sorted(DEFAULTS)}."
        )

    for key, value in user_settings.items():
        _check_type(key, value, EXPECTED_TYPES[key])

    merged = {**DEFAULTS, **user_settings}
    merged["QUEUE_MAP"] = {
        **DEFAULTS["QUEUE_MAP"],
        **user_settings.get("QUEUE_MAP", {}),
    }

    merged["HEALTH"] = MappingProxyType(validate_health(merged["HEALTH"]))

    missing = REQUIRED_PRIORITIES - merged["QUEUE_MAP"].keys()
    if missing:
        raise ConfigurationError(f"{PREFIX} QUEUE_MAP missing {sorted(missing)}")

    if merged["FALLBACK"] not in FALLBACK_STRATEGIES:
        raise ConfigurationError(
            f"{PREFIX} FALLBACK must be one of {sorted(FALLBACK_STRATEGIES)}, "
            f"got {merged['FALLBACK']!r}."
        )

    for name, value in (
        ("DEDUPLICATION_TTL", merged["DEDUPLICATION_TTL"]),
        ("BROKER_TIMEOUT", merged["BROKER_TIMEOUT"]),
        ("CIRCUIT_BREAKER_THRESHOLD", merged["CIRCUIT_BREAKER_THRESHOLD"]),
        ("CIRCUIT_BREAKER_RESET", merged["CIRCUIT_BREAKER_RESET"]),
    ):
        if value <= 0:
            raise ConfigurationError(f"{PREFIX} {name} must be positive.")

    return merged


_DEFAULT_VIEW = MappingProxyType(validate({}))


class _Settings:
    """Read-only view over the active configuration.

    Attribute access (``conf.QUEUE_MAP``) returns the configured value, or
    the default when :func:`celery_adaptive.setup` has not been called.
    """

    _cache: MappingProxyType | None = None

    def configure(self, user_settings: dict) -> None:
        self._cache = MappingProxyType(validate(user_settings))

    def _active(self) -> MappingProxyType:
        return self._cache if self._cache is not None else _DEFAULT_VIEW

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            return self._active()[name]
        except KeyError:
            raise AttributeError(
                f"'{name}' is not a valid celery-adaptive setting."
            ) from None

    def as_dict(self) -> dict[str, Any]:
        return dict(self._active())

    def __repr__(self) -> str:
        return f"<_Settings configured={self.is_configured}>"

    def reset(self) -> None:
        self._cache = None

    @property
    def is_configured(self) -> bool:
        return self._cache is not None


conf = _Settings()
