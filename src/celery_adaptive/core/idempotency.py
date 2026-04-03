"""
Deduplication layer for django-smart-queue.

Uses Redis SET NX EX for atomic locking. Keys are derived from a hash of
task name and arguments. Failures degrade gracefully (fail-open).
"""

import hashlib
import json
import logging

from ..conf import conf

logger = logging.getLogger("django_smart_queue.idempotency")


def _make_key(task_name: str, args: tuple | list, kwargs: dict) -> str:
    """Generate a stable Redis key for a task invocation.

    Normalises args/kwargs so equivalent calls produce identical keys.
    """
    payload = json.dumps(
        {"task": task_name, "args": list(args), "kwargs": dict(sorted(kwargs.items()))},
        sort_keys=True,
        default=str,  # handle non-serialisable types gracefully
    )
    digest = hashlib.sha256(payload.encode()).hexdigest()
    return f"{conf.DEDUP_KEY_PREFIX}{digest}"


def acquire_dedup_lock(
    redis_client, task_name: str, args: tuple | list, kwargs: dict
) -> bool:
    """Attempt to acquire a deduplication lock.

    Returns:
        True if the task should proceed.
        False if a duplicate is already in flight.

    Fails open on Redis errors.
    """
    key = _make_key(task_name, args, kwargs)
    ttl = conf.DEDUPLICATION_TTL

    try:
        acquired = redis_client.set(key, "1", nx=True, ex=ttl)
        if not acquired:
            logger.debug("Dedup hit — skipping task '%s'.", task_name)
            return False
    except Exception as exc:
        logger.warning("Dedup Redis error for '%s', failing open: %s", task_name, exc)
        return True  # Fail open: run task rather than drop it


def release_dedup_lock(
    redis_client, task_name: str, args: tuple | list, kwargs: dict
) -> None:
    """Release the deduplication lock before TTL expiry.

    Errors are logged and ignored; TTL expiry is the fallback.
    """
    key = _make_key(task_name, args, kwargs)

    try:
        redis_client.delete(key)
    except Exception as exc:
        logger.warning("Dedup release failed for '%s': %s", task_name, exc)
