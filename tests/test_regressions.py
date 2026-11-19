"""Regression tests for bugs found in the initial release candidate."""

from celery_adaptive.core.idempotency import acquire_dedup_lock


def test_dedup_lock_allows_first_call_and_blocks_duplicate(configured):
    redis = configured
    assert acquire_dedup_lock(redis, "t", (1, 2), {}) is True
    assert acquire_dedup_lock(redis, "t", (1, 2), {}) is False
    assert acquire_dedup_lock(redis, "t", (3, 4), {}) is True


def test_metrics_package_is_importable():
    import celery_adaptive.metrics as metrics

    metrics.record_dedup_hit("t")
    payload, content_type = metrics.get_latest_metrics()
    assert b"celery_adaptive_dedup_hits_total" in payload
    assert content_type.startswith("text/plain")


def test_get_latest_metrics_without_prometheus(monkeypatch):
    from celery_adaptive.metrics import prometheus

    monkeypatch.setattr(prometheus, "_PROMETHEUS_AVAILABLE", False)
    assert prometheus.get_latest_metrics()[0] == b""


def test_health_sync_decision_raises_operational_error(configured, monkeypatch):
    """A SYNC decision must surface as OperationalError, not a TypeError."""
    import pytest
    from kombu.exceptions import OperationalError

    from celery_adaptive.health import routing
    from celery_adaptive.health.decisions import Action, Decision

    class StubChecker:
        def decide(self, queue):
            return Decision(Action.SYNC, "broker down")

    monkeypatch.setattr(routing, "_checker", StubChecker())

    class StubTask:
        name = "t"

    with pytest.raises(OperationalError):
        routing.route_task(
            celery_task=StubTask(),
            queue="default",
            args=(),
            kwargs={},
            options={},
            fallback_sync=True,
            original_func=lambda: None,
        )
