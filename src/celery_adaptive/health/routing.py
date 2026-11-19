"""
Health-aware routing decisions used by :class:`~celery_adaptive.AdaptiveTask`.
"""

from __future__ import annotations

import logging
import threading

from ..conf import conf
from ..metrics.exporters import record_routing_decision
from ..redis_client import get_redis_client
from .decisions import Decision

logger = logging.getLogger(__name__)

_checker = None
_lock = threading.Lock()


def get_checker():
    """Return the process-wide :class:`HealthChecker`, creating it lazily."""
    global _checker
    if _checker is None:
        with _lock:
            if _checker is None:
                from .checker import HealthChecker

                _checker = HealthChecker.from_conf(
                    health_conf=conf.HEALTH,
                    redis_client=get_redis_client(),
                )
    return _checker


def reset_checker() -> None:
    global _checker
    _checker = None


def decide(queue: str) -> Decision:
    """Return the routing decision for a task headed to ``queue``."""
    decision = get_checker().decide(queue=queue)
    record_routing_decision(decision.action.name, queue)
    return decision
