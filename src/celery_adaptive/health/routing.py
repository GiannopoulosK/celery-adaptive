"""
Health-aware task routing. Raises OperationalError for SYNC decisions.
"""

import logging
from collections.abc import Callable
from typing import Any

from kombu.exceptions import OperationalError

from ..conf import conf
from ..metrics.exporters import record_routing_decision, record_sync_fallback
from ..redis_client import get_redis_client
from .decisions import Action

logger = logging.getLogger(__name__)

_checker = None


def _get_checker():
    global _checker
    if _checker is None:
        from .checker import HealthChecker

        # We safely try to get the HEALTH dict from our global settings
        # If the user didn't provide a 'HEALTH' key, we pass an empty dict to checker
        health_conf = getattr(conf, "HEALTH", {})
        _checker = HealthChecker.from_conf(
            health_conf=health_conf,
            redis_client=get_redis_client(),
        )
    return _checker


def route_task(
    *,
    celery_task,
    queue: str,
    args: tuple,
    kwargs: dict,
    options: dict,
    fallback_sync: bool,
    original_func: Callable,
) -> Any:
    """Enqueue task with health-aware routing."""
    checker = _get_checker()
    decision = checker.decide(queue=queue)

    # Record the routing decision for observability before acting on it.
    record_routing_decision(decision.action.name, queue)

    if conf.LOG_DECISIONS:
        logger.info(
            "Adaptive Decision: [%s] queue=%s reason=%s",
            decision.action.name,
            queue,
            decision.reason,
        )

    if decision.action == Action.SYNC:
        record_sync_fallback(celery_task.name, "health_sync")
        # Raising OperationalError triggers the try/except in decorators.py
        raise OperationalError(f"Health-based sync fallback: {decision.reason}")

    if decision.action == Action.DELAY:
        options["countdown"] = decision.delay_seconds

    if decision.action == Action.REROUTE:
        queue = decision.target_queue or queue

    return celery_task.apply_async(args=args, kwargs=kwargs, queue=queue, **options)
