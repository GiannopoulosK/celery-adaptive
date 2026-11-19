import json
import time

import pytest
from celery.app.task import Task
from kombu.exceptions import OperationalError

import celery_adaptive
from celery_adaptive import batch_task
from celery_adaptive.batching import BatchCollector, flush_all
from celery_adaptive.batching.collector import WATERMARK_PREFIX

from .conftest import queue_depth, running_worker


def published_batches(app, queue="celery"):
    """Drain ``queue`` and return the payload lists of the batch messages."""
    batches = []
    with app.connection_for_write() as conn:
        simple = conn.SimpleQueue(queue)
        while True:
            try:
                message = simple.get(block=False)
            except simple.Empty:
                break
            _, kwargs, _ = message.payload
            batches.append(kwargs["payloads"])
            message.ack()
        simple.close()
    return batches


# --- collector --------------------------------------------------------------


def test_collector_add_reports_length_and_age(redis):
    collector = BatchCollector(redis)
    assert collector.add("t", {"a": 1}) == (1, 0)
    length, age = collector.add("t", {"a": 2})
    assert length == 2
    assert 0 <= age < 1000


def test_watermark_uses_redis_server_clock(redis):
    BatchCollector(redis).add("t", {})
    watermark_ms = int(redis.get(f"{WATERMARK_PREFIX}t"))
    seconds, micros = redis.time()
    server_ms = seconds * 1000 + micros // 1000
    assert abs(server_ms - watermark_ms) < 1000


def test_collect_is_fifo_and_bounded(redis):
    collector = BatchCollector(redis)
    for i in range(5):
        collector.add("t", {"i": i})
    raw = collector.collect("t", 3)
    assert [json.loads(r)["i"] for r in raw] == [0, 1, 2]
    assert collector.pending_count("t") == 2
    collector.collect("t", 10)
    assert redis.get(f"{WATERMARK_PREFIX}t") is None


def test_requeue_restores_order_and_marks_stale(redis):
    collector = BatchCollector(redis)
    for i in range(4):
        collector.add("t", {"i": i})
    raw = collector.collect("t", 2)
    collector.requeue("t", raw)
    assert [json.loads(r)["i"] for r in collector.collect("t", 10)] == [0, 1, 2, 3]


def test_pending_task_names(redis):
    collector = BatchCollector(redis)
    collector.add("b", {})
    collector.add("a", {})
    assert collector.pending_task_names() == ["a", "b"]


# --- batch tasks ------------------------------------------------------------


def test_batch_task_is_a_regular_celery_task(configured, celery_app):
    @batch_task(max_batch_size=10, queue="events")
    def record(payloads):
        return len(payloads)

    assert record.name.endswith(".record")
    assert record(payloads=[{}, {}]) == 2
    assert record.batch_max_size == 10


def test_push_flushes_on_size_to_configured_queue(configured, celery_app):
    @batch_task(max_batch_size=3, max_wait_ms=60_000, queue="events")
    def record(payloads):
        pass

    assert record.push(n=1) is False
    assert record.push(n=2) is False
    assert record.push(n=3) is True
    assert published_batches(celery_app, "events") == [[{"n": 1}, {"n": 2}, {"n": 3}]]
    assert record.pending() == 0


def test_push_flushes_on_age(configured, celery_app):
    @batch_task(max_batch_size=100, max_wait_ms=50)
    def record(payloads):
        pass

    record.push(n=1)
    time.sleep(0.06)
    assert record.push(n=2) is True
    assert published_batches(celery_app) == [[{"n": 1}, {"n": 2}]]


def test_flush_failure_keeps_payloads(configured, celery_app, monkeypatch):
    @batch_task(max_batch_size=2, max_wait_ms=60_000)
    def record(payloads):
        pass

    def down(self, *a, **kw):
        raise OperationalError("refused")

    monkeypatch.setattr(Task, "apply_async", down)
    record.push(n=1)
    record.push(n=2)  # flush attempt fails
    assert record.pending() == 2

    monkeypatch.undo()
    assert record.flush() == 2
    assert published_batches(celery_app) == [[{"n": 1}, {"n": 2}]]


def test_flush_splits_large_buffers(configured, celery_app):
    @batch_task(max_batch_size=2, max_wait_ms=60_000)
    def record(payloads):
        pass

    collector = BatchCollector(configured)
    for i in range(5):
        collector.add(record.name, {"i": i})
    assert record.flush() == 5
    assert [len(b) for b in published_batches(celery_app)] == [2, 2, 1]


def test_without_redis_publishes_immediately(celery_app):
    @batch_task(max_batch_size=100)
    def record(payloads):
        pass

    assert record.push(n=1) is True
    assert published_batches(celery_app) == [[{"n": 1}]]


def test_redis_errors_publish_immediately(celery_app):
    class Broken:
        def eval(self, *a, **k):
            raise ConnectionError("redis down")

    celery_adaptive.setup(redis_client=Broken())

    @batch_task(max_batch_size=100)
    def record(payloads):
        pass

    assert record.push(n=1) is True
    assert queue_depth(celery_app, "celery") == 1


def test_flush_all_only_stale(configured, celery_app):
    @batch_task(max_batch_size=100, max_wait_ms=0)
    def stale(payloads):
        pass

    @batch_task(max_batch_size=100, max_wait_ms=60_000)
    def fresh(payloads):
        pass

    BatchCollector(configured).add(stale.name, {"x": 1})
    fresh.push(y=1)

    assert flush_all(celery_app, only_stale=True) == {stale.name: 1}
    assert fresh.pending() == 1


def test_flush_batches_task_end_to_end(configured, celery_app):
    import celery_adaptive.tasks  # noqa: F401  registers maintenance tasks

    received = []

    @batch_task(max_batch_size=100, max_wait_ms=0)
    def record(payloads):
        received.extend(payloads)

    BatchCollector(configured).add(record.name, {"n": 1})
    BatchCollector(configured).add(record.name, {"n": 2})

    flush = celery_app.tasks["celery_adaptive.flush_batches"]
    with running_worker(celery_app):
        flush.delay()
        from .conftest import wait_for

        assert wait_for(lambda: len(received) == 2)
    assert received == [{"n": 1}, {"n": 2}]


def test_invalid_arguments():
    with pytest.raises(ValueError):
        batch_task(max_batch_size=0)
    with pytest.raises(TypeError):
        batch_task(base=Task)


def test_beat_schedule_names_registered_tasks():
    import celery_adaptive.tasks as tasks

    schedule = celery_adaptive.beat_schedule(flush_batches_every=2)
    names = {entry["task"] for entry in schedule.values()}
    assert names == {tasks.flush_batches.name, tasks.replay_outbox.name}
    assert schedule["celery-adaptive-flush-batches"]["schedule"] == 2
