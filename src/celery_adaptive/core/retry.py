"""
Retry helpers for django-smart-queue.

Provides exponential backoff with optional jitter.
"""

import random

from ..conf import conf


def compute_backoff(attempt: int) -> float:
    """
    Return the number of seconds to wait before retry attempt N.

    Args:
        attempt: zero-based retry count (0 = first retry).
    """
    delay = min(
        conf.RETRY_BASE_DELAY * (2**attempt),
        conf.RETRY_CEILING,
    )
    if conf.RETRY_JITTER:
        # Full jitter in [0, delay] to avoid thundering herd
        delay = random.uniform(0, delay)

    return round(delay, 2)


def get_retry_kwargs(attempt: int) -> dict:
    """Return kwargs for ``task.retry()``."""
    return {
        "countdown": compute_backoff(attempt),
        "max_retries": conf.RETRY_MAX_ATTEMPTS,
    }
