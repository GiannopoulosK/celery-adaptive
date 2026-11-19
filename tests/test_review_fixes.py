"""Regression tests for defects found in an independent review."""

import pytest
from celery.app.task import Task
from kombu.exceptions import EncodeError

import celery_adaptive
from celery_adaptive import adaptive_task
from celery_adaptive.core.circuit import CircuitState, get_breaker

from .conftest import queue_depth


def test_non_broker_error_during_probe_does_not_wedge_circuit(
    configured, celery_app, monkeypatch
):
    @adaptive_task
    def job(x):
        return x

    breaker = get_breaker(job._broker_key())
    breaker.trip()
    breaker.reset_timeout = 0  # next call is the half-open probe

    original = Task.apply_async

    def encode_error(self, *a, **kw):
        raise EncodeError("cannot serialize")

    monkeypatch.setattr(Task, "apply_async", encode_error)
    with pytest.raises(EncodeError):
        job.delay(object())
    monkeypatch.setattr(Task, "apply_async", original)

    job.delay(1)  # must be allowed to probe and close the circuit
    assert breaker.state is CircuitState.CLOSED
    assert queue_depth(celery_app, "celery") == 1


def test_publish_error_releases_dedup_key(configured, celery_app, monkeypatch):
    redis = configured

    @adaptive_task(deduplicate=True)
    def job(x):
        pass

    def encode_error(self, *a, **kw):
        raise EncodeError("cannot serialize")

    monkeypatch.setattr(Task, "apply_async", encode_error)
    with pytest.raises(EncodeError):
        job.delay(1)
    assert redis.keys("cadp:*") == []


def test_propagated_inline_failure_releases_dedup_key(celery_app, redis):
    celery_app.conf.task_eager_propagates = True
    celery_adaptive.setup(config={"FORCE_SYNC": True}, redis_client=redis)
    runs = []

    @adaptive_task(deduplicate=True)
    def job(x):
        runs.append(x)
        raise ValueError(x)

    for _ in range(2):
        with pytest.raises(ValueError):
            job.delay(1)
    assert runs == [1, 1]
    assert redis.keys("cadp:*") == []


def test_eager_mode_task_connection_error_is_not_a_broker_outage(
    configured, celery_app
):
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True
    runs = []

    @adaptive_task
    def call_payment_api():
        runs.append(1)
        raise ConnectionError("payment API down")

    for _ in range(3):
        with pytest.raises(ConnectionError):
            call_payment_api.delay()

    assert len(runs) == 3  # once per call, never re-run by a fallback
    assert get_breaker(call_payment_api._broker_key()).state is CircuitState.CLOSED


def test_eager_mode_still_returns_results(configured, celery_app):
    celery_app.conf.task_always_eager = True

    @adaptive_task(deduplicate=True)
    def add(x, y):
        return x + y

    assert add.delay(1, 2).get() == 3
    assert add.delay(1, 2).get() == 3  # key released after the eager run
