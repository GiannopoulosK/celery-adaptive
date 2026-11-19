import threading

import pytest

import celery_adaptive
from celery_adaptive.core.circuit import (
    CircuitBreaker,
    CircuitState,
    get_breaker,
)


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def breaker(clock):
    return CircuitBreaker(failure_threshold=2, reset_timeout=10, clock=clock)


def test_opens_after_threshold_consecutive_failures(breaker):
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    assert breaker.allow() is False


def test_success_resets_failure_count(breaker):
    breaker.record_failure()
    breaker.record_success()
    breaker.record_failure()
    assert breaker.state is CircuitState.CLOSED


def test_half_open_allows_exactly_one_probe(breaker, clock):
    breaker.trip()
    clock.now = 10
    assert breaker.state is CircuitState.HALF_OPEN
    assert breaker.allow() is True
    assert breaker.allow() is False


def test_failed_probe_reopens(breaker, clock):
    breaker.trip()
    clock.now = 10
    assert breaker.allow()
    breaker.record_failure()
    assert breaker.state is CircuitState.OPEN
    clock.now = 15
    assert breaker.allow() is False


def test_successful_probe_closes(breaker, clock):
    breaker.trip()
    clock.now = 10
    assert breaker.allow()
    breaker.record_success()
    assert breaker.state is CircuitState.CLOSED
    assert breaker.allow()


def test_concurrent_half_open_lets_one_probe_through(breaker, clock):
    breaker.trip()
    clock.now = 10
    results = []
    threads = [
        threading.Thread(target=lambda: results.append(breaker.allow()))
        for _ in range(20)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results.count(True) == 1


def test_registry_uses_configured_thresholds():
    celery_adaptive.setup(
        config={"CIRCUIT_BREAKER_THRESHOLD": 7, "CIRCUIT_BREAKER_RESET": 5}
    )
    b = get_breaker("amqp://x")
    assert (b.failure_threshold, b.reset_timeout) == (7, 5)
    assert get_breaker("amqp://x") is b


def test_setup_resets_breakers():
    b = get_breaker("amqp://x")
    b.trip()
    celery_adaptive.setup(config={})
    assert get_breaker("amqp://x").state is CircuitState.CLOSED


def test_invalid_threshold():
    with pytest.raises(ValueError):
        CircuitBreaker(failure_threshold=0)
