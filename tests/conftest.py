import contextlib

import fakeredis
import pytest
from celery import (
    Celery,
    _state as celery_state,
)
from celery.contrib.testing.worker import start_worker
from kombu.transport import memory

import celery_adaptive
from celery_adaptive.conf import conf
from celery_adaptive.core.circuit import reset_breakers
from celery_adaptive.redis_client import reset_redis_client


@pytest.fixture
def celery_app():
    """An isolated Celery app on the in-memory transport, set as current.

    ``shared_task`` registers tasks on every app created afterwards, so a
    task defined in one test would shadow a same-named task in the next.
    Snapshot and restore the finalizer set, and clear the in-memory broker,
    to keep tests independent.
    """
    finalizers = set(celery_state._on_app_finalizers)
    memory.Channel.queues.clear()

    app = Celery("tests", broker="memory://", backend="cache+memory://")
    app.conf.task_store_eager_result = True
    app.set_current()
    app.set_default()
    yield app

    celery_state._on_app_finalizers.clear()
    celery_state._on_app_finalizers.update(finalizers)
    celery_state._deregister_app(app)
    memory.Channel.queues.clear()


@contextlib.contextmanager
def running_worker(app):
    """Run a real worker thread for ``app`` for the duration of the block."""
    with start_worker(app, perform_ping_check=False, loglevel="WARNING") as worker:
        yield worker


def queue_depth(app, queue: str) -> int:
    """Number of messages waiting in ``queue`` on the app's broker."""
    with app.connection_for_write() as conn:
        channel = conn.default_channel
        try:
            return channel.queue_declare(queue, passive=True).message_count
        except Exception:
            return 0


@pytest.fixture
def broker_down(monkeypatch):
    """Make every publish fail as if the broker were unreachable."""
    from celery.app.task import Task
    from kombu.exceptions import OperationalError

    calls = []

    def fail(self, *args, **kwargs):
        calls.append(self.name)
        raise OperationalError("[Errno 111] Connection refused")

    monkeypatch.setattr(Task, "apply_async", fail)
    return calls


@pytest.fixture
def redis():
    return fakeredis.FakeRedis()


@pytest.fixture
def configured(celery_app, redis):
    """Library configured with defaults and a fake Redis client."""
    celery_adaptive.setup(config={}, redis_client=redis)
    return redis


@pytest.fixture(autouse=True)
def _reset_state():
    conf.reset()
    reset_redis_client()
    reset_breakers()
    yield
    conf.reset()
    reset_redis_client()
    reset_breakers()
