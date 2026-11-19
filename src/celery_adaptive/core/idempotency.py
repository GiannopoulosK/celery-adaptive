"""
Deduplication of in-flight task dispatches.

When a task is dispatched with deduplication enabled, a Redis key derived
from the task name and arguments is set to the new task id (``SET NX EX``).
While that key exists, identical dispatches return the *original* task's
result handle instead of publishing again. The worker deletes the key when
the task reaches a final state (success or failure); ``DEDUPLICATION_TTL``
is only the safety net for tasks that never report back.

All Redis errors fail open: running a task twice is better than dropping it.
"""

from __future__ import annotations

import datetime as dt
import decimal
import hashlib
import json
import logging
import uuid
from collections.abc import Callable
from typing import Any

from ..conf import conf

logger = logging.getLogger(__name__)

DEDUP_HEADER = "adaptive_dedup_key"

DedupKeyFunc = Callable[..., Any]


def _canonical(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (uuid.UUID, decimal.Decimal)):
        return str(value)
    if isinstance(value, (set, frozenset)):
        return sorted(value, key=repr)
    if isinstance(value, bytes):
        return value.hex()
    raise TypeError(
        f"Cannot derive a deduplication key from {type(value).__name__!r}. "
        "Pass dedup_key=... to adaptive_task to choose the identifying fields."
    )


def make_dedup_key(
    task_name: str,
    args: tuple | list,
    kwargs: dict,
    key_func: DedupKeyFunc | None = None,
) -> str:
    """
    Return the Redis key identifying a task invocation.

    By default the key covers all arguments. ``key_func`` receives the task's
    arguments and returns the identifying value, e.g.
    ``dedup_key=lambda user_id, **_: user_id``.
    """
    if key_func is not None:
        identity: Any = key_func(*args, **kwargs)
    else:
        identity = {"args": list(args), "kwargs": kwargs}

    payload = json.dumps(identity, sort_keys=True, default=_canonical)
    digest = hashlib.sha256(payload.encode()).hexdigest()
    return f"{conf.DEDUP_KEY_PREFIX}{task_name}:{digest}"


def _decode(value: Any) -> str | None:
    if value is None:
        return None
    return value.decode() if isinstance(value, bytes) else str(value)


def acquire(redis_client, key: str, task_id: str, ttl: int | None = None) -> str | None:
    """
    Try to claim ``key`` for ``task_id``.

    Returns ``None`` when the caller may proceed (lock acquired, re-acquired
    by the same task id on retry, or Redis failed), otherwise the id of the
    task already holding the key.
    """
    ttl = ttl or conf.DEDUPLICATION_TTL
    try:
        if redis_client.set(key, task_id, nx=True, ex=ttl):
            return None
        holder = _decode(redis_client.get(key))
        if holder is None:
            # Expired or released between SET and GET: try once more.
            return (
                None
                if redis_client.set(key, task_id, nx=True, ex=ttl)
                else (_decode(redis_client.get(key)))
            )
        if holder == task_id:
            redis_client.expire(key, ttl)
            return None
        return holder
    except Exception as exc:
        logger.warning("Deduplication unavailable for %s, failing open: %s", key, exc)
        return None


def release(redis_client, key: str, task_id: str) -> bool:
    """Delete ``key`` if it is still held by ``task_id``. Returns True if deleted."""
    try:
        with redis_client.pipeline() as pipe:
            pipe.watch(key)
            if _decode(pipe.get(key)) != task_id:
                pipe.unwatch()
                return False
            pipe.multi()
            pipe.delete(key)
            pipe.execute()
            return True
    except Exception as exc:
        # WatchError (concurrent change) or connectivity: TTL is the fallback.
        logger.debug("Deduplication release skipped for %s: %s", key, exc)
        return False
