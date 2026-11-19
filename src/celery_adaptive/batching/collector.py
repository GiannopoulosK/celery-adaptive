"""
Redis-backed batch buffer.

Each batch task has a Redis list of JSON payloads and a watermark key
holding the arrival time of the oldest buffered payload. All operations are
Lua scripts, so they are atomic across processes, and every timestamp comes
from the Redis server clock (``TIME``), so producers on different hosts
agree on how old a batch is.
"""

from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

KEY_PREFIX = "cadpb:"
WATERMARK_PREFIX = "cadpbw:"

_NOW_MS = """
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
"""

# KEYS: list, watermark. ARGV: payload. Returns {length, age_ms}.
_LUA_ADD = (
    _NOW_MS
    + """
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], now, 'NX')
local length = redis.call('LLEN', KEYS[1])
local watermark = tonumber(redis.call('GET', KEYS[2])) or now
return {length, now - watermark}
"""
)

# KEYS: list, watermark. ARGV: max items. Returns the claimed raw payloads.
# The remaining items start a new wait window.
_LUA_COLLECT = (
    _NOW_MS
    + """
local items = redis.call('LRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
if #items > 0 then
    redis.call('LTRIM', KEYS[1], #items, -1)
end
if redis.call('LLEN', KEYS[1]) == 0 then
    redis.call('DEL', KEYS[2])
elseif #items > 0 then
    redis.call('SET', KEYS[2], now)
end
return items
"""
)

# KEYS: list, watermark. Returns age of the oldest item in ms, or -1.
_LUA_AGE = (
    _NOW_MS
    + """
local watermark = tonumber(redis.call('GET', KEYS[2]))
if not watermark or redis.call('LLEN', KEYS[1]) == 0 then
    return -1
end
return now - watermark
"""
)


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


class BatchCollector:
    """Buffers payloads per task name and reports flush thresholds."""

    def __init__(self, redis_client):
        self._r = redis_client

    @staticmethod
    def _keys(task_name: str) -> tuple[str, str]:
        return f"{KEY_PREFIX}{task_name}", f"{WATERMARK_PREFIX}{task_name}"

    def add(self, task_name: str, payload: dict) -> tuple[int, int]:
        """Buffer ``payload``. Returns ``(buffered_count, oldest_age_ms)``."""
        raw = json.dumps(payload, sort_keys=True)
        length, age = self._r.eval(_LUA_ADD, 2, *self._keys(task_name), raw)
        return int(length), int(age)

    def collect(self, task_name: str, max_items: int) -> list[str]:
        """Atomically remove and return up to ``max_items`` raw payloads."""
        items = self._r.eval(_LUA_COLLECT, 2, *self._keys(task_name), max_items)
        return [_decode(i) for i in items or []]

    def requeue(self, task_name: str, raw_items: list[str]) -> None:
        """Put claimed payloads back at the head, preserving their order."""
        if not raw_items:
            return
        list_key, watermark_key = self._keys(task_name)
        with self._r.pipeline() as pipe:
            pipe.lpush(list_key, *reversed(raw_items))
            pipe.set(watermark_key, 0, nx=True)  # flush on the next check
            pipe.execute()

    def pending_count(self, task_name: str) -> int:
        return int(self._r.llen(self._keys(task_name)[0]))

    def oldest_age_ms(self, task_name: str) -> int | None:
        age = int(self._r.eval(_LUA_AGE, 2, *self._keys(task_name)))
        return None if age < 0 else age

    def pending_task_names(self) -> list[str]:
        """Names of all tasks with buffered payloads (non-blocking SCAN)."""
        names = []
        for key in self._r.scan_iter(match=f"{KEY_PREFIX}*", count=100):
            names.append(_decode(key)[len(KEY_PREFIX) :])
        return sorted(names)

    @staticmethod
    def decode(raw_items: list[str]) -> list[dict]:
        payloads = []
        for raw in raw_items:
            try:
                payloads.append(json.loads(raw))
            except ValueError:
                logger.error("Dropping malformed batch payload: %r", raw)
        return payloads
