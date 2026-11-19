import datetime as dt

import pytest
from celery.app.task import Task
from kombu.exceptions import OperationalError
from kombu.utils.json import loads

import celery_adaptive
from celery_adaptive import adaptive_task
from celery_adaptive.core import outbox
from celery_adaptive.core.circuit import get_breaker
from celery_adaptive.exceptions import BrokerUnavailableError

from .conftest import queue_depth, running_worker


@pytest.fixture
def outbox_mode(celery_app, redis):
    celery_adaptive.setup(
        config={"FALLBACK": "outbox", "OUTBOX_AUTO_REPLAY": False},
        redis_client=redis,
    )
    return redis


def _entries(redis):
    return [loads(raw) for raw in redis.lrange("cadp:outbox", 0, -1)]


def test_broker_down_stores_task_and_returns_pending_handle(outbox_mode, broker_down):
    @adaptive_task(priority="high")
    def charge(order_id):
        pass

    result = charge.delay(7)
    assert result.state == "PENDING"

    [entry] = _entries(outbox_mode)
    assert entry["id"] == result.id
    assert entry["task"] == charge.name
    assert entry["args"] == [7]
    assert entry["options"]["queue"] == "high_priority"
    assert outbox.pending(outbox_mode) == 1


def test_countdown_is_converted_to_absolute_eta(outbox_mode, broker_down):
    @adaptive_task
    def later():
        pass

    later.apply_async(countdown=60)
    eta = _entries(outbox_mode)[0]["options"]["eta"]
    assert isinstance(eta, dt.datetime)
    assert eta > dt.datetime.now(dt.UTC) + dt.timedelta(seconds=50)


def test_outbox_without_redis_raises(celery_app, broker_down):
    celery_adaptive.setup(config={"FALLBACK": "outbox"})

    @adaptive_task
    def job():
        pass

    with pytest.raises(BrokerUnavailableError, match="outbox"):
        job.delay()


def test_per_task_fallback_override(outbox_mode, broker_down):
    @adaptive_task(fallback="sync")
    def inline():
        return "inline"

    assert inline.delay().get() == "inline"
    assert outbox.pending(outbox_mode) == 0


def test_invalid_fallback_rejected(celery_app):
    from celery_adaptive.exceptions import ConfigurationError

    with pytest.raises(ConfigurationError, match="FALLBACK"):
        celery_adaptive.setup(config={"FALLBACK": "drop"})

    @adaptive_task(fallback="drop")
    def job():
        pass

    with pytest.raises(ConfigurationError, match="fallback"):
        job.delay()


def test_replay_publishes_in_order_with_original_ids(
    outbox_mode, celery_app, monkeypatch
):
    sent = []
    monkeypatch.setattr(
        celery_app, "send_task", lambda name, **kw: sent.append((name, kw))
    )
    for i in range(3):
        outbox.store("t", f"id-{i}", (i,), {}, {"queue": "q"})

    assert outbox.replay(celery_app) == 3
    assert [kw["task_id"] for _, kw in sent] == ["id-0", "id-1", "id-2"]
    assert sent[0][1]["queue"] == "q"
    assert outbox.pending(outbox_mode) == 0


def test_replay_stops_on_failure_and_keeps_order(outbox_mode, celery_app, monkeypatch):
    calls = []

    def flaky(name, **kw):
        calls.append(kw["task_id"])
        if len(calls) == 2:
            raise OperationalError("down again")

    monkeypatch.setattr(celery_app, "send_task", flaky)
    for i in range(3):
        outbox.store("t", f"id-{i}", (), {}, {})

    assert outbox.replay(celery_app) == 1
    assert [e["id"] for e in _entries(outbox_mode)] == ["id-1", "id-2"]


def test_replay_recovers_entries_from_crashed_replay(
    outbox_mode, celery_app, monkeypatch
):
    outbox.store("t", "id-0", (), {}, {})
    # Simulate a replay that moved the entry to processing and then crashed.
    outbox_mode.lmove("cadp:outbox", "cadp:outbox:processing", "LEFT", "RIGHT")
    assert outbox.pending(outbox_mode) == 1

    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append(kw))
    assert outbox.replay(celery_app) == 1
    assert sent[0]["task_id"] == "id-0"


def test_replay_limit(outbox_mode, celery_app, monkeypatch):
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: None)
    for i in range(5):
        outbox.store("t", f"id-{i}", (), {}, {})
    assert outbox.replay(celery_app, limit=2) == 2
    assert outbox.pending(outbox_mode) == 3


def test_broker_recovery_triggers_background_replay(celery_app, redis, monkeypatch):
    celery_adaptive.setup(config={"FALLBACK": "outbox"}, redis_client=redis)
    outbox.store("t", "stored-id", (), {}, {})

    replayed = []
    monkeypatch.setattr(
        outbox, "replay_in_background", lambda app: replayed.append(app)
    )

    @adaptive_task
    def job():
        pass

    get_breaker(job._broker_key()).trip()
    get_breaker(job._broker_key()).reset_timeout = 0  # allow a probe now
    job.delay()
    assert replayed == [celery_app]


def test_end_to_end_outage_then_recovery(outbox_mode, celery_app, monkeypatch):
    @adaptive_task
    def add(x, y):
        return x + y

    original = Task.apply_async

    def down(self, *a, **kw):
        raise OperationalError("Connection refused")

    monkeypatch.setattr(Task, "apply_async", down)
    pending_result = add.delay(2, 3)
    monkeypatch.setattr(Task, "apply_async", original)

    assert queue_depth(celery_app, "celery") == 0
    assert outbox.replay(celery_app) == 1

    with running_worker(celery_app):
        assert pending_result.get(timeout=10) == 5
