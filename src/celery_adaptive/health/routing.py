"""
Process-wide health monitor used by :class:`~celery_adaptive.AdaptiveTask`.
"""

from __future__ import annotations

import logging
import threading

from ..conf import conf
from ..metrics.exporters import record_routing_decision
from ..redis_client import get_redis_client
from .checker import HealthMonitor
from .decisions import Decision

logger = logging.getLogger(__name__)

_monitor: HealthMonitor | None = None
_lock = threading.Lock()


def get_monitor(app=None) -> HealthMonitor:
    """Return the process-wide :class:`HealthMonitor`, creating it lazily."""
    global _monitor
    if _monitor is None:
        with _lock:
            if _monitor is None:
                if app is None:
                    from celery import current_app

                    app = current_app._get_current_object()
                _monitor = HealthMonitor.from_conf(
                    app,
                    conf.HEALTH,
                    queue_map=conf.QUEUE_MAP,
                    redis_client=get_redis_client(),
                )
    return _monitor


def reset_monitor() -> None:
    """Stop and forget the monitor. Called by setup() and in tests."""
    global _monitor
    with _lock:
        if _monitor is not None:
            _monitor.stop(timeout=1.0)
        _monitor = None


def decide(queue: str) -> Decision:
    """Return the routing decision for a task headed to ``queue``."""
    decision = get_monitor().decide(queue)
    record_routing_decision(decision.action.name, queue)
    return decision
