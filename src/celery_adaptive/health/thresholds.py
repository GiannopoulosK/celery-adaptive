"""
Threshold configurations for adaptive routing.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class HealthThresholds:
    redis_latency_warn_ms: float = 30.0
    redis_latency_critical_ms: float = 100.0
    redis_memory_warn_pct: float = 75.0
    redis_memory_critical_pct: float = 90.0
    rabbitmq_depth_warn: int = 500
    rabbitmq_depth_critical: int = 2000


def load_thresholds(health_conf: dict | None = None) -> HealthThresholds:
    u = health_conf or {}
    return HealthThresholds(
        redis_latency_warn_ms=u.get("REDIS_LATENCY_WARN_MS", 30.0),
        redis_latency_critical_ms=u.get("REDIS_LATENCY_CRITICAL_MS", 100.0),
        redis_memory_warn_pct=u.get("REDIS_MEMORY_WARN_PCT", 75.0),
        redis_memory_critical_pct=u.get("REDIS_MEMORY_CRITICAL_PCT", 90.0),
        rabbitmq_depth_warn=u.get("RABBITMQ_DEPTH_WARN", 500),
        rabbitmq_depth_critical=u.get("RABBITMQ_DEPTH_CRITICAL", 2000),
    )
