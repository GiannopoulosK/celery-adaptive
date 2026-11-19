import fakeredis
import pytest
from celery import Celery

import celery_adaptive
from celery_adaptive.conf import conf
from celery_adaptive.redis_client import reset_redis_client


@pytest.fixture
def celery_app():
    """An isolated Celery app on the in-memory transport, set as current."""
    app = Celery("tests", broker="memory://", backend="cache+memory://")
    app.conf.task_store_eager_result = True
    app.set_current()
    app.set_default()
    yield app


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
    yield
    conf.reset()
    reset_redis_client()
