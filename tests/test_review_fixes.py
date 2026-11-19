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


# --- canvas and retries through fallbacks ----------------------------------


def test_sync_fallback_continues_chains(configured, broker_down):
    from celery import chain

    seen = []

    @adaptive_task
    def first(x):
        seen.append(("first", x))
        return x + 1

    @adaptive_task
    def second(x):
        seen.append(("second", x))
        return x * 10

    chain(first.s(1), second.s()).apply_async()
    assert seen == [("first", 1), ("second", 2)]


def test_sync_fallback_refuses_group_members(configured, broker_down):
    from celery import group

    from celery_adaptive.exceptions import BrokerUnavailableError

    @adaptive_task
    def member(x):
        return x

    with pytest.raises(BrokerUnavailableError, match="group or chord"):
        group(member.s(1), member.s(2)).apply_async()


def test_sync_fallback_never_runs_delayed_tasks_early(configured, broker_down):
    from celery_adaptive.exceptions import BrokerUnavailableError

    runs = []

    @adaptive_task(deduplicate=True)
    def later(x):
        runs.append(x)

    with pytest.raises(BrokerUnavailableError, match="countdown"):
        later.apply_async((1,), countdown=60)
    assert runs == []
    assert configured.keys("cadp:*") == []


def test_sync_fallback_keeps_retry_count(configured, broker_down):
    @adaptive_task(bind=True)
    def job(self):
        return self.request.retries

    assert job.apply_async(retries=2).get() == 2


def test_outbox_preserves_chain_through_replay(celery_app, redis, monkeypatch):
    from celery import chain

    from celery_adaptive.core import outbox

    from .conftest import running_worker

    celery_adaptive.setup(
        config={"FALLBACK": "outbox", "OUTBOX_AUTO_REPLAY": False},
        redis_client=redis,
    )

    @adaptive_task
    def first(x):
        return x + 1

    @adaptive_task
    def second(x):
        return x * 10

    original = Task.apply_async

    def down(self, *a, **kw):
        from kombu.exceptions import OperationalError

        raise OperationalError("refused")

    monkeypatch.setattr(Task, "apply_async", down)
    final = chain(first.s(1), second.s()).apply_async()
    monkeypatch.setattr(Task, "apply_async", original)

    assert outbox.replay(celery_app) == 1
    with running_worker(celery_app):
        assert final.get(timeout=10) == 20


def test_outbox_keeps_retry_count(celery_app, redis, broker_down):
    from kombu.utils.json import loads

    celery_adaptive.setup(
        config={"FALLBACK": "outbox", "OUTBOX_AUTO_REPLAY": False},
        redis_client=redis,
    )

    @adaptive_task
    def job():
        pass

    job.apply_async(retries=3, countdown=5)
    entry = loads(redis.lindex("cadp:outbox", 0))
    assert entry["options"]["retries"] == 3
    assert "eta" in entry["options"]
