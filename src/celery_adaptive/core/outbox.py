"""
Redis outbox for tasks that could not be published.

With ``FALLBACK = "outbox"``, a task dispatched while the broker is down is
stored in a Redis list instead of running inline. :func:`replay` publishes
stored tasks with their **original task id**, so ``AsyncResult`` handles
returned to callers stay valid.

Replay happens automatically when the circuit breaker sees the broker
recover (``OUTBOX_AUTO_REPLAY``), and can be triggered with the
``celery_adaptive.replay_outbox`` task, the ``celery-adaptive replay-outbox``
CLI command, or Django's ``replay_outbox`` management command.

The outbox needs a Redis that is *not* your broker; if both are the same
server they go down together.
"""

from __future__ import annotations

import datetime as dt
import logging
import threading
import time
from typing import Any

from kombu.utils.json import dumps, loads

from ..conf import conf
from ..redis_client import get_redis_client

logger = logging.getLogger(__name__)

# Publish options worth preserving; everything else is connection-specific.
_KEPT_OPTIONS = (
    "queue",
    "routing_key",
    "exchange",
    "priority",
    "headers",
    "eta",
    "expires",
    "link",
    "link_error",
    "shadow",
)


def _processing_key(key: str) -> str:
    return f"{key}:processing"


def _portable_options(options: dict[str, Any]) -> dict[str, Any]:
    kept: dict[str, Any] = {}
    for name in _KEPT_OPTIONS:
        value = options.get(name)
        if value is None:
            continue
        if name in ("queue", "exchange"):
            value = getattr(value, "name", value)
        kept[name] = value

    countdown = options.get("countdown")
    if countdown is not None and "eta" not in kept:
        kept["eta"] = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=countdown)
    expires = kept.get("expires")
    if isinstance(expires, (int, float)):
        kept["expires"] = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=expires)
    return kept


def store(
    task_name: str,
    task_id: str,
    args: tuple,
    kwargs: dict,
    options: dict[str, Any],
    redis_client=None,
) -> None:
    """Append a task envelope to the outbox. Raises if Redis is unavailable."""
    redis_client = redis_client or get_redis_client()
    if redis_client is None:
        raise RuntimeError("No Redis client configured for the outbox.")
    envelope = {
        "task": task_name,
        "id": task_id,
        "args": list(args),
        "kwargs": kwargs,
        "options": _portable_options(options),
        "stored_at": time.time(),
    }
    redis_client.rpush(conf.OUTBOX_KEY, dumps(envelope))


def pending(redis_client=None) -> int:
    """Number of tasks waiting in the outbox (including any mid-replay)."""
    redis_client = redis_client or get_redis_client()
    if redis_client is None:
        return 0
    key = conf.OUTBOX_KEY
    return int(redis_client.llen(key)) + int(redis_client.llen(_processing_key(key)))


def replay(app=None, *, limit: int | None = None, redis_client=None) -> int:
    """
    Publish stored tasks to the broker. Returns the number published.

    Each envelope is moved to a processing list before publishing and
    removed only after the broker accepts it, so a crash mid-replay never
    loses tasks (they are recovered on the next replay). Stops at the first
    publish failure, leaving the remaining tasks in place.
    """
    from celery import current_app

    app = app or current_app
    redis_client = redis_client or get_redis_client()
    if redis_client is None:
        return 0

    key = conf.OUTBOX_KEY
    processing = _processing_key(key)

    # Recover envelopes left behind by a replay that crashed.
    while redis_client.lmove(processing, key, "RIGHT", "LEFT") is not None:
        pass

    published = 0
    while limit is None or published < limit:
        raw = redis_client.lmove(key, processing, "LEFT", "RIGHT")
        if raw is None:
            break
        try:
            envelope = loads(raw)
        except ValueError:
            logger.error("Dropping malformed outbox entry: %r", raw)
            redis_client.lrem(processing, 1, raw)
            continue
        try:
            app.send_task(
                envelope["task"],
                args=envelope["args"],
                kwargs=envelope["kwargs"],
                task_id=envelope["id"],
                **envelope["options"],
            )
        except Exception as exc:
            logger.warning("Outbox replay stopped, broker still unavailable: %s", exc)
            redis_client.lmove(processing, key, "RIGHT", "LEFT")
            break
        redis_client.lrem(processing, 1, raw)
        published += 1

    if published:
        logger.info("Replayed %d task(s) from the outbox.", published)
        from ..metrics.exporters import record_outbox_replayed

        record_outbox_replayed(published)
    return published


_replay_lock = threading.Lock()


def replay_in_background(app) -> threading.Thread | None:
    """Start a daemon thread replaying the outbox, unless one is running."""
    if not _replay_lock.acquire(blocking=False):
        return None

    def _run():
        try:
            replay(app)
        except Exception:
            logger.exception("Background outbox replay failed.")
        finally:
            _replay_lock.release()

    thread = threading.Thread(target=_run, name="celery-adaptive-outbox", daemon=True)
    thread.start()
    return thread
