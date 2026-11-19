from celery import Celery

from celery_adaptive.health.backends.broker import BrokerHealthBackend

from ..conftest import queue_depth


def test_reports_depth_of_watched_queues(celery_app):
    @celery_app.task
    def job():
        pass

    for _ in range(3):
        job.apply_async(queue="work")
    assert queue_depth(celery_app, "work") == 3

    result = BrokerHealthBackend(celery_app, ["work", "never-declared"]).check()
    assert result.healthy
    assert result.depth_for("work") == 3
    assert result.depth_for("never-declared") == 0
    assert result.depth_for("not-watched") is None
    assert result.latency_ms is not None


def test_unreachable_broker_is_unhealthy():
    app = Celery("down", broker="amqp://guest@127.0.0.1:1//")
    app.conf.broker_connection_timeout = 0.5
    result = BrokerHealthBackend(app, ["celery"]).check()
    assert not result.healthy
    assert result.error


def test_duplicate_queue_names_are_checked_once(celery_app):
    backend = BrokerHealthBackend(celery_app, ["a", "b", "a"])
    assert backend.queues == ["a", "b"]
