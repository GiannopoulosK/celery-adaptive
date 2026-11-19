import os
import time

import pytest

import celery_adaptive
from celery_adaptive import adaptive_task
from celery_adaptive.conf import conf
from celery_adaptive.core.circuit import CircuitState, get_breaker
from celery_adaptive.health import routing
from celery_adaptive.health.backends.broker import BrokerHealthResult
from celery_adaptive.health.checker import HealthMonitor
from celery_adaptive.health.decisions import Action, DecisionEngine

from ..conftest import queue_depth, wait_for


class StubBackend:
    def __init__(self, result, delay=0.0):
        self.result = result
        self.delay = delay
        self.calls = 0

    def check(self):
        self.calls += 1
        time.sleep(self.delay)
        return self.result


def healthy(**depths):
    return BrokerHealthResult(True, queue_depths=depths)


@pytest.fixture(autouse=True)
def _stop_monitor():
    yield
    routing.reset_monitor()


def test_decide_never_blocks_on_slow_checks():
    backend = StubBackend(healthy(default=1), delay=2.0)
    monitor = HealthMonitor(broker=backend, interval=60)
    start = time.monotonic()
    decision = monitor.decide("default")
    assert time.monotonic() - start < 0.5
    assert decision.action is Action.ENQUEUE
    monitor.stop(timeout=0)


def test_background_thread_refreshes_snapshot():
    backend = StubBackend(healthy(default=1))
    monitor = HealthMonitor(broker=backend, interval=0.05)
    monitor.ensure_running()
    assert wait_for(lambda: backend.calls >= 3)
    assert monitor.snapshot.queue_depth("default") == 1
    monitor.stop()
    assert not monitor.running


def test_stale_snapshot_is_ignored():
    monitor = HealthMonitor(
        broker=StubBackend(healthy(default=10_000)),
        stale_after=0.01,
        engine=DecisionEngine(depth_critical=100, reroute={"default": "overflow"}),
    )
    monitor.check_now()
    assert monitor.fresh_snapshot() is not None
    time.sleep(0.02)
    assert monitor.fresh_snapshot() is None


def test_unreachable_broker_trips_circuit_breaker():
    monitor = HealthMonitor(
        broker=StubBackend(BrokerHealthResult(False, error="refused")),
        breaker_key="amqp://mq//",
    )
    monitor.check_now()
    assert get_breaker("amqp://mq//").state is CircuitState.OPEN


def test_monitor_restarts_thread_after_fork():
    monitor = HealthMonitor(broker=StubBackend(healthy()), interval=60)
    monitor.ensure_running()
    first = monitor._thread
    monitor._pid = os.getpid() + 1  # pretend we are a forked child
    monitor.ensure_running()
    assert monitor._thread is not first
    monitor.stop(timeout=1)
    first.join(0)


def test_from_conf_watches_mapped_and_reroute_queues(celery_app, redis):
    celery_adaptive.setup(
        config={"HEALTH": {"REROUTE": {"default": "overflow"}}}, redis_client=redis
    )
    monitor = HealthMonitor.from_conf(
        celery_app, conf.HEALTH, queue_map=conf.QUEUE_MAP, redis_client=redis
    )
    assert set(monitor.broker.queues) == {
        "high_priority",
        "default",
        "low_priority",
        "celery",
        "overflow",
    }
    assert monitor.redis is not None
    assert monitor.celery is None  # inspect is opt-in
    assert monitor.rabbitmq is None


def test_adaptive_task_reroutes_backed_up_queue(celery_app, monkeypatch):
    celery_adaptive.setup(
        config={
            "HEALTH": {
                "ENABLED": True,
                "QUEUE_DEPTH_CRITICAL": 2,
                "QUEUE_DEPTH_WARN": 1,
                "REROUTE": {"default": "overflow"},
            }
        }
    )

    @adaptive_task(priority="default")
    def job():
        pass

    monitor = routing.get_monitor(celery_app)
    monkeypatch.setattr(monitor, "ensure_running", lambda: None)

    for _ in range(2):
        job.delay()
    monitor.check_now()  # sees depth 2 on 'default'
    job.delay()

    assert queue_depth(celery_app, "default") == 2
    assert queue_depth(celery_app, "overflow") == 1


def test_health_disabled_by_default_starts_no_thread(configured):
    @adaptive_task
    def job():
        pass

    job.delay()
    assert routing._monitor is None
