"""
Exception hierarchy for celery-adaptive.

All exceptions inherit from ``CeleryAdaptiveError``.
"""


class CeleryAdaptiveError(Exception):
    """Base exception for all celery-adaptive errors."""


class BrokerUnavailableError(CeleryAdaptiveError):
    """
    Raised when the broker is unreachable and sync fallback is disabled.

    Raised when ``FALLBACK`` is ``"raise"``, or when it is ``"outbox"`` and
    the outbox cannot store the task either.
    """


class DuplicateTaskError(CeleryAdaptiveError):
    """
    Raised when a task is rejected due to deduplication.

    Not used by default; provided for applications that want to surface
    duplicate suppression explicitly.
    """


class ConfigurationError(CeleryAdaptiveError):
    """
    Raised for invalid celery-adaptive configuration.

    Typically triggered during setup() or first access to settings.
    Common causes:
    - invalid types
    - unknown keys
    - malformed QUEUE_MAP
    """
