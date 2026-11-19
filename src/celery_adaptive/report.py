"""Plain-text rendering of health snapshots for the CLI and Django command."""

from __future__ import annotations

import datetime as dt

from .health.checker import STATUS_DEGRADED, STATUS_DOWN, STATUS_OK, HealthSnapshot

EXIT_CODES = {STATUS_OK: 0, STATUS_DEGRADED: 1, STATUS_DOWN: 2}


def _line(label: str, value: str) -> str:
    return f"  {label:<18}{value}"


def _broker_lines(snapshot: HealthSnapshot) -> list[str]:
    broker = snapshot.broker
    if broker is None:
        return []
    if broker.healthy:
        return [_line("broker", f"up ({broker.latency_ms}ms)")]
    return [_line("broker", "DOWN"), _line("", str(broker.error))]


def _redis_lines(snapshot: HealthSnapshot) -> list[str]:
    redis = snapshot.redis
    if redis is None:
        return []
    if not redis.healthy:
        return [_line("redis", f"DOWN: {redis.error}")]
    memory = (
        f", memory {redis.memory_used_pct}%"
        if redis.memory_used_pct is not None
        else ""
    )
    return [_line("redis", f"up ({redis.latency_ms}ms{memory})")]


def _worker_lines(snapshot: HealthSnapshot) -> list[str]:
    celery = snapshot.celery
    if celery is None:
        return []
    if not celery.healthy:
        return [_line("workers", f"DOWN: {celery.error}")]
    saturated = " (saturated)" if celery.saturated else ""
    return [
        _line(
            "workers",
            f"{celery.worker_count} up, {celery.total_active} active{saturated}",
        )
    ]


def _queue_lines(snapshot: HealthSnapshot) -> list[str]:
    depths = snapshot.queue_depths
    if not depths:
        return []
    width = max(len(q) for q in depths)
    lines = ["", "  queues"]
    for queue, depth in sorted(depths.items()):
        flag = "  <- backed up" if depth >= snapshot.depth_warn else ""
        lines.append(f"    {queue:<{width}}  {depth:>8}{flag}")
    return lines


def render_snapshot(
    snapshot: HealthSnapshot,
    *,
    outbox_pending: int | None = None,
    batches: dict[str, int] | None = None,
) -> str:
    """Render ``snapshot`` as a human-readable multi-line report."""
    captured = dt.datetime.fromtimestamp(snapshot.captured_at).strftime("%H:%M:%S")
    lines = [
        f"celery-adaptive health: {snapshot.status.upper()}  ({captured})",
        "",
        *_broker_lines(snapshot),
        *_redis_lines(snapshot),
        *_worker_lines(snapshot),
        *_queue_lines(snapshot),
    ]
    if outbox_pending is not None:
        lines += ["", _line("outbox", f"{outbox_pending} task(s) waiting")]
    if batches:
        lines += ["", "  batch buffers"]
        lines += [f"    {name}  {count}" for name, count in sorted(batches.items())]
    if snapshot.issues:
        lines += ["", "  issues", *(f"    - {issue}" for issue in snapshot.issues)]
    return "\n".join(lines)
