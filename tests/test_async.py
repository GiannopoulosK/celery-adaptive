import asyncio
import threading

from celery.result import AsyncResult

from celery_adaptive import adaptive_task, batch_task

from .conftest import queue_depth


def test_adelay_publishes_off_the_event_loop(configured, celery_app, monkeypatch):
    @adaptive_task(priority="low")
    def job(x, y=None):
        pass

    loop_thread = []
    publish_thread = []
    original = job.apply_async

    def spy(*args, **kwargs):
        publish_thread.append(threading.get_ident())
        return original(*args, **kwargs)

    monkeypatch.setattr(job, "apply_async", spy)

    async def main():
        loop_thread.append(threading.get_ident())
        return await job.adelay(1, y=2)

    result = asyncio.run(main())
    assert isinstance(result, AsyncResult)
    assert publish_thread and publish_thread != loop_thread
    assert queue_depth(celery_app, "low_priority") == 1


def test_aapply_async_passes_options(configured, celery_app):
    @adaptive_task
    def job():
        pass

    asyncio.run(job.aapply_async(queue="special", adaptive={"deduplicate": False}))
    assert queue_depth(celery_app, "special") == 1


def test_adelay_returns_eager_result_on_fallback(configured, broker_down):
    @adaptive_task
    def add(x, y):
        return x + y

    assert asyncio.run(add.adelay(2, 2)).get() == 4


def test_apush(configured, celery_app):
    @batch_task(max_batch_size=1)
    def record(payloads):
        pass

    assert asyncio.run(record.apush(a=1)) is True
    assert queue_depth(celery_app, "celery") == 1
