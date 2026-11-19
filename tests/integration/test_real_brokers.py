"""
End-to-end tests against real Redis / RabbitMQ servers.

Skipped unless the servers are provided, as in CI:

    CELERY_ADAPTIVE_TEST_REDIS_URL=redis://localhost:6379/15
    CELERY_ADAPTIVE_TEST_AMQP_URL=amqp://guest:guest@localhost:5672//
    CELERY_ADAPTIVE_TEST_RABBITMQ_API=http://localhost:15672
"""

import os
import uuid

import pytest
from celery import Celery

import celery_adaptive
from celery_adaptive import adaptive_task, batch_task
from celery_adaptive.conf import conf
from celery_adaptive.core import outbox
from celery_adaptive.health.backends.broker import BrokerHealthBackend
from celery_adaptive.health.backends.rabbitmq import RabbitMQHealthBackend

from ..conftest import running_worker, wait_for

REDIS_URL = os.environ.get("CELERY_ADAPTIVE_TEST_REDIS_URL")
AMQP_URL = os.environ.get("CELERY_ADAPTIVE_TEST_AMQP_URL")
RABBITMQ_API = os.environ.get("CELERY_ADAPTIVE_TEST_RABBITMQ_API")

pytestmark = pytest.mark.integration

brokers = [
    pytest.param(
        REDIS_URL,
        id="redis",
        marks=pytest.mark.skipif(not REDIS_URL, reason="no Redis configured"),
    ),
    pytest.param(
        AMQP_URL,
        id="rabbitmq",
        marks=pytest.mark.skipif(not AMQP_URL, reason="no RabbitMQ configured"),
    ),
]


@pytest.fixture
def redis_client():
    if not REDIS_URL:
        pytest.skip("no Redis configured")
    import redis

    client = redis.Redis.from_url(REDIS_URL)
    client.flushdb()
    yield client
    client.flushdb()


@pytest.fixture(params=brokers)
def real_app(request, redis_client):
    from celery import _state

    app = Celery("it", broker=request.param, backend=REDIS_URL)
    app.set_current()
    celery_adaptive.setup(config={}, redis_client=redis_client, app=app)
    yield app
    _state._deregister_app(app)


def unique_queue() -> str:
    return f"it-{uuid.uuid4().hex[:8]}"


def test_task_round_trip_through_real_worker(real_app):
    @adaptive_task(app=real_app)
    def add(x, y):
        return x + y

    with running_worker(real_app):
        assert add.delay(20, 22).get(timeout=20) == 42


def test_dedup_lock_released_by_real_worker(real_app, redis_client):
    @adaptive_task(app=real_app, deduplicate=True)
    def slow_square(x):
        return x * x

    first = slow_square.delay(3)
    assert slow_square.delay(3).id == first.id  # suppressed while queued

    with running_worker(real_app):
        assert first.get(timeout=20) == 9
        assert wait_for(lambda: redis_client.keys(f"{conf.DEDUP_KEY_PREFIX}*") == [])


def test_broker_backend_reads_real_queue_depth(real_app):
    queue = unique_queue()

    @adaptive_task(app=real_app)
    def job():
        pass

    backend = BrokerHealthBackend(real_app, [queue])
    assert backend.check().depth_for(queue) == 0  # not declared yet

    for _ in range(3):
        job.apply_async(queue=queue)
    # RabbitMQ updates counters asynchronously after publish.
    assert wait_for(lambda: backend.check().depth_for(queue) == 3, timeout=10)
    assert backend.check().healthy

    with real_app.connection_for_write() as conn:
        conn.default_channel.queue_purge(queue)


def test_batching_lua_scripts_on_real_redis(real_app, redis_client):
    received = []

    @batch_task(app=real_app, max_batch_size=3, max_wait_ms=60_000)
    def events(payloads):
        received.extend(payloads)

    with running_worker(real_app):
        for i in range(3):
            events.push(i=i)
        assert wait_for(lambda: len(received) == 3, timeout=20)
    assert received == [{"i": 0}, {"i": 1}, {"i": 2}]


def test_outbox_replay_on_real_broker(real_app, redis_client):
    @adaptive_task(app=real_app)
    def add(x, y):
        return x + y

    outbox.store(add.name, "outbox-it-id", (1, 2), {}, {}, redis_client=redis_client)
    assert outbox.replay(real_app) == 1
    with running_worker(real_app):
        assert real_app.AsyncResult("outbox-it-id").get(timeout=20) == 3


@pytest.mark.skipif(not RABBITMQ_API, reason="no RabbitMQ management API")
def test_rabbitmq_management_backend():
    result = RabbitMQHealthBackend(base_url=RABBITMQ_API).check()
    assert result.healthy, result.error
