"""
Framework-neutral health endpoint logic shared by the Django and FastAPI
integrations.
"""

from __future__ import annotations

from typing import Any

from .checker import STATUS_DEGRADED, STATUS_DOWN


def health_response(*, strict: bool = False) -> tuple[int, dict[str, Any]]:
    """
    Return ``(http_status, body)`` for a health endpoint.

    Uses the background monitor's snapshot when it is fresh, otherwise runs
    the checks once. ``down`` maps to 503; ``degraded`` maps to 503 only
    when ``strict`` (useful for readiness probes), 200 otherwise.
    """
    from ..core import outbox
    from ..core.circuit import all_breakers
    from ..redis_client import get_redis_client
    from .routing import get_monitor

    monitor = get_monitor()
    snapshot = monitor.fresh_snapshot() or monitor.check_now()
    body = snapshot.to_dict()
    body["circuit_breakers"] = {
        name: breaker.state.value for name, breaker in all_breakers().items()
    }
    redis = get_redis_client()
    try:
        body["outbox_pending"] = outbox.pending(redis) if redis else None
    except Exception:
        body["outbox_pending"] = None

    unhealthy = {STATUS_DOWN} | ({STATUS_DEGRADED} if strict else set())
    return (503 if snapshot.status in unhealthy else 200), body
