"""
Decision engine — maps a health snapshot to a routing action.

Actions priority (highest wins):
1. SYNC     — broker unreachable; execute inline.
2. DELAY    — broker reachable but under pressure.
3. REROUTE  — target queue backed up; send to less-loaded queue.
4. BATCH    — Phase 3 (not yet implemented).
5. ENQUEUE  — all systems nominal.

Decisions are cheap and rely on cached snapshots.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class Action(Enum):
    """Possible routing actions."""

    ENQUEUE = auto()  # All clear — proceed normally.
    DELAY = auto()  # Enqueue with a ``countdown`` to ease pressure.
    REROUTE = auto()  # Send to an alternate, less-loaded queue.
    BATCH = auto()  # Collect into a batch for deferred flush (Phase 3).
    SYNC = auto()  # Broker down — execute the task inline.


# ---------------------------------------------------------------------------
# Decision dataclass
# ---------------------------------------------------------------------------


@dataclass
class Decision:
    """Outcome of a DecisionEngine call."""

    action: Action
    reason: str
    delay_seconds: int = 0
    target_queue: str | None = None

    @property
    def should_run_sync(self) -> bool:
        return self.action == Action.SYNC

    @property
    def should_delay(self) -> bool:
        return self.action == Action.DELAY

    @property
    def should_reroute(self) -> bool:
        return self.action == Action.REROUTE


class DecisionEngine:
    def __init__(self, thresholds):
        self._t = thresholds

    def decide(self, redis, rabbitmq, celery, queue_name="default") -> Decision:
        # 1. Critical Failures -> SYNC
        if redis and not redis.healthy:
            return Decision(Action.SYNC, f"Redis error: {redis.error}")
        if celery and not celery.healthy:
            return Decision(Action.SYNC, f"Celery unhealthy: {celery.error}")

        # 2. Redis Latency -> DELAY
        if (
            redis
            and redis.latency_ms
            and redis.latency_ms >= self._t.redis_latency_critical_ms
        ):
            return Decision(
                Action.DELAY,
                f"High Redis latency: {redis.latency_ms:.0f}ms",
                delay_seconds=10,
            )

        # 3. RabbitMQ Backpressure -> REROUTE or DELAY
        if rabbitmq and rabbitmq.healthy:
            depth = rabbitmq.depth_for(queue_name)
            if depth is not None:
                if depth >= self._t.rabbitmq_depth_critical:
                    return Decision(
                        Action.REROUTE,
                        f"Queue '{queue_name}' critical ({depth})",
                        target_queue="low_priority",
                    )
                if depth >= self._t.rabbitmq_depth_warn:
                    return Decision(
                        Action.DELAY,
                        f"Queue '{queue_name}' warn ({depth})",
                        delay_seconds=5,
                    )

        # 4. Resource Saturation -> DELAY
        if celery and celery.healthy and celery.overloaded:
            return Decision(Action.DELAY, "Workers at capacity", delay_seconds=3)

        if (
            redis
            and redis.memory_used_pct
            and redis.memory_used_pct >= self._t.redis_memory_critical_pct
        ):
            return Decision(
                Action.DELAY,
                f"Redis memory critical ({redis.memory_used_pct:.0f}%)",
                delay_seconds=5,
            )

        return Decision(Action.ENQUEUE, "Normal")
