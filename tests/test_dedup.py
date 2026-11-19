import datetime as dt
import uuid

import pytest

import celery_adaptive
from celery_adaptive import adaptive_task
from celery_adaptive.core.idempotency import acquire, make_dedup_key, release

from .conftest import queue_depth, running_worker, wait_for

# --- key derivation --------------------------------------------------------


def test_key_is_stable_across_kwarg_order():
    assert make_dedup_key("t", (1,), {"a": 1, "b": 2}) == make_dedup_key(
        "t", (1,), {"b": 2, "a": 1}
    )


def test_key_differs_per_task_and_args():
    assert make_dedup_key("t", (1,), {}) != make_dedup_key("u", (1,), {})
    assert make_dedup_key("t", (1,), {}) != make_dedup_key("t", (2,), {})


def test_key_contains_task_name_for_debuggability():
    assert make_dedup_key("app.tasks.sync", (), {}).startswith("cadp:app.tasks.sync:")


def test_key_supports_common_types():
    make_dedup_key("t", (dt.datetime(2026, 1, 1), uuid.uuid4(), {1, 2}, b"x"), {})


def test_key_rejects_unhashable_objects_instead_of_using_repr():
    class Thing:
        pass

    with pytest.raises(TypeError, match="dedup_key"):
        make_dedup_key("t", (Thing(),), {})


def test_custom_key_func():
    def key(user_id, **_):
        return user_id

    assert make_dedup_key("t", (7,), {"ts": 1}, key) == make_dedup_key(
        "t", (7,), {"ts": 2}, key
    )


# --- lock primitives -------------------------------------------------------


def test_same_task_id_reacquires_for_retries(redis):
    assert acquire(redis, "k", "id-1", ttl=10) is None
    assert acquire(redis, "k", "id-1", ttl=10) is None


def test_release_only_by_holder(redis):
    acquire(redis, "k", "id-1", ttl=10)
    assert release(redis, "k", "id-2") is False
    assert redis.get("k") == b"id-1"
    assert release(redis, "k", "id-1") is True
    assert redis.get("k") is None


def test_ttl_applied(redis):
    acquire(redis, "k", "id-1", ttl=42)
    assert 0 < redis.ttl("k") <= 42


def test_fails_open_on_redis_errors():
    class Broken:
        def set(self, *a, **k):
            raise ConnectionError("down")

    assert acquire(Broken(), "k", "id", ttl=1) is None


# --- dispatch integration --------------------------------------------------


def test_duplicate_returns_handle_of_in_flight_task(configured, celery_app):
    @adaptive_task(deduplicate=True)
    def sync_user(user_id):
        pass

    first = sync_user.delay(1)
    second = sync_user.delay(1)
    other = sync_user.delay(2)

    assert second.id == first.id
    assert other.id != first.id
    assert queue_depth(celery_app, "celery") == 2


def test_custom_dedup_key_via_decorator(configured, celery_app):
    @adaptive_task(deduplicate=True, dedup_key=lambda user_id, **_: user_id)
    def sync_user(user_id, requested_at=None):
        pass

    a = sync_user.delay(1, requested_at="10:00")
    b = sync_user.delay(1, requested_at="10:05")
    assert a.id == b.id


def test_custom_dedup_key_via_base_class(configured, celery_app):
    from celery_adaptive import AdaptiveTask

    @celery_app.task(
        base=AdaptiveTask,
        adaptive_deduplicate=True,
        adaptive_dedup_key=lambda user_id, **_: user_id,
    )
    def sync_user(user_id, extra=None):
        pass

    assert sync_user.delay(1, extra=1).id == sync_user.delay(1, extra=2).id


def test_per_call_override_enables_dedup(configured, celery_app):
    @adaptive_task
    def job(x):
        pass

    a = job.apply_async((1,), adaptive={"deduplicate": True})
    b = job.apply_async((1,), adaptive={"deduplicate": True})
    assert a.id == b.id


def test_disable_deduplication_setting(celery_app, redis):
    celery_adaptive.setup(config={"DISABLE_DEDUPLICATION": True}, redis_client=redis)

    @adaptive_task(deduplicate=True)
    def job(x):
        pass

    assert job.delay(1).id != job.delay(1).id


def test_dedup_without_redis_dispatches_normally(celery_app):
    @adaptive_task(deduplicate=True)
    def job(x):
        pass

    assert job.delay(1).id != job.delay(1).id


def test_lock_released_after_inline_fallback_completes(configured, broker_down):
    redis = configured

    @adaptive_task(deduplicate=True)
    def job(x):
        return x

    assert job.delay(1).get() == 1
    assert redis.keys("cadp:*") == []
    assert job.delay(1).get() == 1  # not suppressed


def test_lock_released_when_dispatch_raises(celery_app, redis, broker_down):
    from celery_adaptive.exceptions import BrokerUnavailableError

    celery_adaptive.setup(config={"FALLBACK": "raise"}, redis_client=redis)

    @adaptive_task(deduplicate=True)
    def job(x):
        pass

    with pytest.raises(BrokerUnavailableError):
        job.delay(1)
    assert redis.keys("cadp:*") == []


def test_worker_releases_lock_when_task_finishes(configured, celery_app):
    redis = configured

    @adaptive_task(deduplicate=True)
    def job(x):
        return x * 2

    with running_worker(celery_app):
        first = job.delay(21)
        assert first.get(timeout=10) == 42
        # The result is stored just before after_return releases the key.
        assert wait_for(lambda: redis.keys("cadp:*") == [])
        second = job.delay(21)
        assert second.id != first.id
        assert second.get(timeout=10) == 42


def test_worker_releases_lock_when_task_fails(configured, celery_app):
    redis = configured

    @adaptive_task(deduplicate=True)
    def job(x):
        raise ValueError(x)

    with running_worker(celery_app):
        result = job.delay(1)
        with pytest.raises(ValueError):
            result.get(timeout=10)
        assert wait_for(lambda: redis.keys("cadp:*") == [])


def test_retry_keeps_lock_and_is_not_suppressed(configured, celery_app):
    redis = configured
    attempts = []

    @adaptive_task(deduplicate=True, bind=True, max_retries=2)
    def flaky(self, x):
        attempts.append(self.request.retries)
        if self.request.retries < 2:
            # The lock must survive retries, so duplicates stay suppressed.
            assert redis.keys("cadp:*")
            raise self.retry(countdown=0)
        return "done"

    with running_worker(celery_app):
        assert flaky.delay(1).get(timeout=10) == "done"
        assert wait_for(lambda: redis.keys("cadp:*") == [])
    assert attempts == [0, 1, 2]
