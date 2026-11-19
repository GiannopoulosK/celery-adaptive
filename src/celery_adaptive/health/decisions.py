"""
Decision engine: maps a health snapshot to a routing action.

The engine deliberately does little. Broker outages are handled by the
circuit breaker and the fallback strategy, not by guessing from health
data. What health data *can* improve is placement: when a queue is backed
up past ``QUEUE_DEPTH_CRITICAL`` and an overflow queue is configured in
``HEALTH["REROUTE"]``, new tasks go there instead.

Decisions are pure functions of the snapshot, so they are cheap and easy
to test.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .checker import HealthSnapshot


class Action(Enum):
    ENQUEUE = auto()  # Publish to the intended queue.
    REROUTE = auto()  # Publish to the configured overflow queue instead.


@dataclass(frozen=True)
class Decision:
    action: Action
    reason: str
    target_queue: str | None = None

    @property
    def should_reroute(self) -> bool:
        return self.action is Action.REROUTE


class DecisionEngine:
    def __init__(self, depth_critical: int = 2000, reroute: Mapping[str, str] = ()):
        self.depth_critical = depth_critical
        self.reroute = dict(reroute)

    @classmethod
    def from_conf(cls, health: Mapping) -> DecisionEngine:
        return cls(
            depth_critical=health["QUEUE_DEPTH_CRITICAL"],
            reroute=health["REROUTE"],
        )

    def decide(self, snapshot: HealthSnapshot | None, queue: str) -> Decision:
        target = self.reroute.get(queue)
        if target is None:
            return Decision(Action.ENQUEUE, "no reroute configured")
        if snapshot is None:
            return Decision(Action.ENQUEUE, "no recent health data")

        depth = snapshot.queue_depth(queue)
        if depth is None:
            return Decision(Action.ENQUEUE, f"depth of '{queue}' unknown")
        if depth < self.depth_critical:
            return Decision(Action.ENQUEUE, f"'{queue}' depth {depth}")

        target_depth = snapshot.queue_depth(target)
        if target_depth is not None and target_depth >= depth:
            return Decision(
                Action.ENQUEUE,
                f"'{queue}' depth {depth} but overflow '{target}' is no better "
                f"({target_depth})",
            )
        return Decision(
            Action.REROUTE,
            f"'{queue}' depth {depth} >= {self.depth_critical}",
            target_queue=target,
        )
