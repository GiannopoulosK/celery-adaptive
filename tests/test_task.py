import pytest
from celery import chain
from celery.result import AsyncResult, EagerResult

import celery_adaptive
from celery_adaptive import AdaptiveTask, adaptive_task
from celery_adaptive.core.circuit import CircuitState, get_breaker
from celery_adaptive.exceptions import ConfigurationError

from .conftest import queue_depth


def test_delay_publishes_to_broker(configured, celery_app):
    @adaptive_task
    def add(x, y):
        return x + y

    result = add.delay(1, 2)
    assert isinstance(result, AsyncResult)
    assert not isinstance(result, EagerResult)
    assert queue_depth(celery_app, "celery") == 1


def test_task_keeps_celery_default_name(configured):
    @adaptive_task
    def add(x, y):
        return x + y

    assert add.name == f"{__name__}.add" or add.name.endswith(".add")
    assert not add.name.startswith("celery_adaptive.")


def test_priority_maps_to_queue(configured, celery_app):
    @adaptive_task(priority="high")
    def urgent():
        pass

    urgent.delay()
    assert queue_depth(celery_app, "high_priority") == 1


def test_explicit_queue_wins_over_priority(configured, celery_app):
    @adaptive_task(priority="high")
    def urgent():
        pass

    urgent.apply_async(queue="special")
    assert queue_depth(celery_app, "special") == 1
    assert queue_depth(celery_app, "high_priority") == 0


def test_per_call_override(configured, celery_app):
    @adaptive_task(priority="high")
    def job():
        pass

    job.apply_async(adaptive={"priority": "low"})
    assert queue_depth(celery_app, "low_priority") == 1


def test_unknown_override_rejected(configured):
    @adaptive_task
    def job():
        pass

    with pytest.raises(ConfigurationError, match="Unknown adaptive option"):
        job.apply_async(adaptive={"prio": "low"})


def test_invalid_priority_rejected_at_dispatch(configured):
    @adaptive_task(priority="urgent")
    def job():
        pass

    with pytest.raises(ConfigurationError, match="Invalid priority"):
        job.delay()


def test_signatures_and_chains_work(configured, celery_app):
    @adaptive_task(priority="low")
    def step(x):
        return x

    sig = step.s(1)
    assert sig.task == step.name
    chain(step.s(1), step.s()).apply_async()
    assert queue_depth(celery_app, "low_priority") == 1


def test_works_as_base_class_with_app_task(configured, celery_app):
    @celery_app.task(base=AdaptiveTask, adaptive_priority="high")
    def job():
        pass

    job.delay()
    assert queue_depth(celery_app, "high_priority") == 1


def test_caller_kwargs_not_mutated(configured):
    @adaptive_task
    def job(**kw):
        pass

    kwargs = {"a": 1}
    options = {"countdown": 5}
    job.apply_async((), kwargs, **options)
    assert kwargs == {"a": 1}
    assert options == {"countdown": 5}


def test_base_must_be_adaptive_task():
    from celery import Task

    with pytest.raises(TypeError):
        adaptive_task(base=Task)


# --- broker failures -------------------------------------------------------


def test_broker_down_falls_back_to_inline_eager_result(configured, broker_down):
    @adaptive_task
    def add(x, y):
        return x + y

    result = add.delay(2, 3)
    assert isinstance(result, EagerResult)
    assert result.get() == 5
    assert result.id


def test_inline_fallback_keeps_bind_context(configured, broker_down):
    @adaptive_task(bind=True)
    def whoami(self):
        return self.request.id

    result = whoami.apply_async(task_id="fixed-id")
    assert result.get() == "fixed-id"


def test_inline_fallback_captures_task_exceptions(configured, broker_down):
    @adaptive_task
    def fails():
        raise RuntimeError("task failed")

    result = fails.delay()
    assert result.failed()
    with pytest.raises(RuntimeError):
        result.get()


def test_setting_disables_fallback(celery_app, broker_down):
    from celery_adaptive.exceptions import BrokerUnavailableError

    celery_adaptive.setup(config={"FALLBACK": "raise"})

    @adaptive_task
    def job():
        pass

    with pytest.raises(BrokerUnavailableError):
        job.delay()


def test_circuit_opens_and_skips_publish(celery_app, broker_down):
    celery_adaptive.setup(config={"CIRCUIT_BREAKER_THRESHOLD": 2})

    @adaptive_task
    def job():
        return "ran"

    for _ in range(5):
        assert job.delay().get() == "ran"

    assert len(broker_down) == 2  # later calls skip the broker entirely
    assert get_breaker(job._broker_key()).state is CircuitState.OPEN


def test_successful_publish_keeps_circuit_closed(configured, celery_app):
    @adaptive_task
    def job():
        pass

    job.delay()
    assert get_breaker(job._broker_key()).state is CircuitState.CLOSED


def test_bounded_publish_retry_policy(configured, monkeypatch):
    from celery.app.task import Task

    seen = {}

    def capture(self, *args, **kwargs):
        seen.update(kwargs)

    monkeypatch.setattr(Task, "apply_async", capture)

    @adaptive_task
    def job():
        pass

    job.delay()
    assert seen["retry_policy"]["max_retries"] == 1

    job.apply_async(retry_policy={"max_retries": 5})
    assert seen["retry_policy"]["max_retries"] == 5


def test_force_sync(celery_app):
    celery_adaptive.setup(config={"FORCE_SYNC": True})

    @adaptive_task
    def add(x, y):
        return x + y

    result = add.delay(1, 1)
    assert isinstance(result, EagerResult)
    assert result.get() == 2
    assert queue_depth(celery_app, "celery") == 0
