"""
Configuration for celery-adaptive.

Configure once at application startup:

    from celery_adaptive import setup
    setup(config={
        "DEDUPLICATION_TTL": 600,
        "QUEUE_MAP": {"high": "critical", "default": "default", "low": "bulk"},
    })

In Django projects, prefer contrib.django which calls setup() for you
via AppConfig.ready().
"""

from types import MappingProxyType

from .exceptions import ConfigurationError

PREFIX = "celery_adaptive:"


DEFAULTS: dict = {
    "DEDUPLICATION_TTL": 300,
    "DEDUP_KEY_PREFIX": "cadp:",
    "SYNC_FALLBACK": True,
    "SYNC_FALLBACK_LOG": True,
    "RETRY_MAX_ATTEMPTS": 3,
    "RETRY_BASE_DELAY": 2,
    "RETRY_CEILING": 60,
    "RETRY_JITTER": True,
    "LOG_DECISIONS": True,
    "METRICS_ENABLED": False,
    "DISABLE_DEDUPLICATION": False,
    "FORCE_SYNC": False,
    "BROKER_TIMEOUT": 1.0,
    "QUEUE_MAP": {
        "high": "high_priority",
        "default": "default",
        "low": "low_priority",
    },
    "HEALTH": {},
}


EXPECTED_TYPES: dict = {
    "DEDUPLICATION_TTL": int,
    "DEDUP_KEY_PREFIX": str,
    "SYNC_FALLBACK": bool,
    "SYNC_FALLBACK_LOG": bool,
    "RETRY_MAX_ATTEMPTS": int,
    "RETRY_BASE_DELAY": int,
    "RETRY_CEILING": int,
    "RETRY_JITTER": bool,
    "LOG_DECISIONS": bool,
    "METRICS_ENABLED": bool,
    "DISABLE_DEDUPLICATION": bool,
    "FORCE_SYNC": bool,
    "BROKER_TIMEOUT": (int, float),
    "QUEUE_MAP": dict,
    "HEALTH": dict,
}


class _Settings:
    _cache: MappingProxyType | None = None

    def configure(self, user_settings: dict) -> None:
        if not isinstance(user_settings, dict):
            raise ConfigurationError(f"{PREFIX} config must be a dict.")

        unknown = set(user_settings) - set(DEFAULTS)
        if unknown:
            raise ConfigurationError(
                f"{PREFIX} unknown keys: {sorted(unknown)}. Valid: {sorted(DEFAULTS)}."
            )

        for key, value in user_settings.items():
            expected = EXPECTED_TYPES.get(key)
            if expected and not isinstance(value, expected):
                exp_name = (
                    ", ".join(t.__name__ for t in expected)
                    if isinstance(expected, tuple)
                    else expected.__name__
                )
                raise ConfigurationError(
                    f"{PREFIX} {key} must be {exp_name}, got {type(value).__name__}."
                )

        # Merge defaults with user settings
        merged = {**DEFAULTS, **user_settings}

        # Deep merge QUEUE_MAP
        merged["QUEUE_MAP"] = {
            **DEFAULTS["QUEUE_MAP"],
            **user_settings.get("QUEUE_MAP", {}),
        }

        required = {"high", "default", "low"}
        if not required.issubset(merged["QUEUE_MAP"].keys()):
            raise ConfigurationError(
                f"{PREFIX} QUEUE_MAP missing {required - merged['QUEUE_MAP'].keys()}"
            )

        # Freeze config to prevent accidental mutation
        self._cache = MappingProxyType(merged)

    def __getattr__(self, name: str):
        if self._cache is None:
            raise ConfigurationError(
                "celery-adaptive not configured. Call setup() first."
            )

        value = self._cache.get(name)
        if value is None and name not in self._cache:
            raise AttributeError(f"'{name}' is not a valid celery-adaptive setting.")

        return value

    def __repr__(self) -> str:
        return f"<_Settings configured={self.is_configured}>"

    def reset(self) -> None:
        self._cache = None

    @property
    def is_configured(self) -> bool:
        return self._cache is not None


conf = _Settings()
