"""Regression tests for bugs found in the initial release candidate."""

from celery_adaptive.core.idempotency import acquire, make_dedup_key


def test_dedup_lock_allows_first_call_and_blocks_duplicate(configured):
    redis = configured
    key = make_dedup_key("t", (1, 2), {})
    assert acquire(redis, key, "id-1") is None  # acquired: proceed
    assert acquire(redis, key, "id-2") == "id-1"  # duplicate of id-1
    assert acquire(redis, make_dedup_key("t", (3, 4), {}), "id-3") is None


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


def test_non_broker_errors_are_not_masked_as_sync_fallback(configured, monkeypatch):
    """Bugs during dispatch must propagate instead of running the task inline."""
    import pytest
    from celery.app.task import Task

    from celery_adaptive import adaptive_task

    calls = []

    @adaptive_task
    def add(x, y):
        calls.append((x, y))

    def boom(*args, **kwargs):
        raise ValueError("serialization bug")

    monkeypatch.setattr(Task, "apply_async", boom)
    with pytest.raises(ValueError):
        add.delay(1, 2)
    assert calls == []


def test_broker_errors_raise_when_fallback_disabled(configured, broker_down):
    import pytest

    from celery_adaptive import adaptive_task
    from celery_adaptive.exceptions import BrokerUnavailableError

    @adaptive_task(fallback_sync=False)
    def add(x, y):
        return x + y

    with pytest.raises(BrokerUnavailableError):
        add.delay(1, 2)


def test_health_settings_are_accepted(celery_app):
    import celery_adaptive
    from celery_adaptive.conf import conf

    celery_adaptive.setup(config={"HEALTH": {"RABBITMQ_URL": "http://mq:15672"}})
    assert conf.HEALTH["RABBITMQ_URL"] == "http://mq:15672"


def test_star_import_exposes_public_api():
    namespace: dict = {}
    exec("from celery_adaptive import *", namespace)
    assert {"setup", "adaptive_task", "CeleryAdaptiveError"} <= namespace.keys()
