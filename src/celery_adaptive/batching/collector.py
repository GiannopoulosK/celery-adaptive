"""
Redis-backed batch buffer for celery-adaptive.
"""

import json
import logging
import time

logger = logging.getLogger("celery_adaptive.batching.collector")

_KEY_PREFIX = "cadpb:"
_WATERMARK_PREFIX = "cadpbw:"

# Atomically claim up to ARGV[1] items from a list and trim them in one
# operation.  Returns the claimed items.  Safe for multi-consumer use.
_LUA_COLLECT = """
local items = redis.call('LRANGE', KEYS[1], 0, ARGV[1] - 1)
if #items > 0 then
    redis.call('LTRIM', KEYS[1], #items, -1)
end
return items
"""


class BatchCollector:
    """Accumulates payloads and signals flush thresholds."""

    def __init__(self, redis_client, max_batch_size: int = 100, max_wait_ms: int = 200):
        self._r = redis_client
        self.max_batch_size = max_batch_size
        self.max_wait_ms = max_wait_ms

    def add(self, task_name: str, payload: dict) -> bool:
        """Append a payload and return True if a flush threshold is reached."""
        list_key = f"{_KEY_PREFIX}{task_name}"
        watermark_key = f"{_WATERMARK_PREFIX}{task_name}"

        self._r.rpush(list_key, json.dumps(payload, default=str))

        # SETNX preserves the original timestamp — only sets if absent,
        # so the watermark always reflects when the *first* item arrived.
        now_ms = int(time.monotonic() * 1000)
        self._r.setnx(watermark_key, now_ms)

        length = self._r.llen(list_key)

        if length >= self.max_batch_size:
            logger.debug(
                "Batch size threshold reached for '%s': %d items.",
                task_name,
                length,
            )
            return True

        watermark = self._r.get(watermark_key)
        if watermark and (now_ms - int(watermark)) >= self.max_wait_ms:
            logger.debug(
                "Batch time threshold reached for '%s': %dms elapsed.",
                task_name,
                now_ms - int(watermark),
            )
            return True

        return False

    def collect(self, task_name: str, max_items: int | None = None) -> list[dict]:
        """Atomically drain up to `max_items` payloads and adjust watermark."""
        list_key = f"{_KEY_PREFIX}{task_name}"
        watermark_key = f"{_WATERMARK_PREFIX}{task_name}"
        count = max_items or self.max_batch_size

        raw_items = self._r.eval(_LUA_COLLECT, 1, list_key, count)
        if not raw_items:
            return []

        remaining = self._r.llen(list_key)
        if remaining == 0:
            self._r.delete(watermark_key)
        else:
            # Shift watermark to now so remaining items start a new wait window
            self._r.set(watermark_key, int(time.monotonic() * 1000))

        payloads = []
        for raw in raw_items:
            try:
                payloads.append(json.loads(raw))
            except json.JSONDecodeError:
                logger.warning("BatchCollector: skipping malformed payload: %r", raw)

        logger.debug("Collected %d item(s) for '%s'.", len(payloads), task_name)
        return payloads

    def pending_count(self, task_name: str) -> int:
        """Return the number of items currently buffered for ``task_name``."""
        return self._r.llen(f"{_KEY_PREFIX}{task_name}")

    def all_pending_tasks(self) -> list[str]:
        """Return all task names with buffered items using non-blocking SCAN."""
        prefix = f"{_KEY_PREFIX}*"
        prefix_len = len(_KEY_PREFIX)
        tasks = []
        cursor = 0
        while True:
            cursor, keys = self._r.scan(cursor, match=prefix, count=100)
            for k in keys:
                name = (k.decode() if isinstance(k, bytes) else k)[prefix_len:]
                tasks.append(name)
            if cursor == 0:
                break
        return tasks
