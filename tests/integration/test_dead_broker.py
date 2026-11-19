"""
Real broker failures, no mocks: kombu connects to closed ports, so these
tests exercise the actual exception types and timings users will see.
"""

import time

import pytest
from celery import Celery
from celery.result import EagerResult

import celery_adaptive
from celery_adaptive import adaptive_task
from celery_adaptive.core.circuit import CircuitState, get_breaker
from celery_adaptive.exceptions import BrokerUnavailableError

DEAD_BROKERS = ["amqp://guest:guest@127.0.0.1:1//", "redis://127.0.0.1:1/0"]


@pytest.fixture(params=DEAD_BROKERS, ids=["amqp", "redis"])
def dead_app(request):
    from celery import _state

    app = Celery("dead", broker=request.param, backend="cache+memory://")
    app.conf.task_store_eager_result = True
    app.set_current()
    yield app
    _state._deregister_app(app)


def test_sync_fallback_against_unreachable_broker(dead_app):
    celery_adaptive.setup(config={"BROKER_TIMEOUT": 0.5}, app=dead_app)

    @adaptive_task(app=dead_app)
    def add(x, y):
        return x + y

    start = time.monotonic()
    result = add.delay(2, 3)
    assert isinstance(result, EagerResult)
    assert result.get() == 5
    assert time.monotonic() - start < 3


def test_circuit_opens_and_later_calls_are_instant(dead_app):
    celery_adaptive.setup(
        config={"BROKER_TIMEOUT": 0.5, "CIRCUIT_BREAKER_THRESHOLD": 1}, app=dead_app
    )

    @adaptive_task(app=dead_app)
    def ping():
        return "pong"

    ping.delay()
    assert get_breaker(ping._broker_key()).state is CircuitState.OPEN

    start = time.monotonic()
    for _ in range(20):
        assert ping.delay().get() == "pong"
    assert time.monotonic() - start < 0.5


def test_raise_strategy_against_unreachable_broker(dead_app):
    celery_adaptive.setup(config={"FALLBACK": "raise"}, app=dead_app)

    @adaptive_task(app=dead_app)
    def job():
        pass

    with pytest.raises(BrokerUnavailableError):
        job.delay()


def test_health_monitor_reports_down(dead_app):
    from celery_adaptive.conf import conf
    from celery_adaptive.health.checker import HealthMonitor

    celery_adaptive.setup(config={"BROKER_TIMEOUT": 0.5}, app=dead_app)
    monitor = HealthMonitor.from_conf(dead_app, conf.HEALTH, queue_map=conf.QUEUE_MAP)
    snapshot = monitor.check_now()
    assert snapshot.status == "down"
    assert get_breaker(monitor.breaker_key).state is CircuitState.OPEN
