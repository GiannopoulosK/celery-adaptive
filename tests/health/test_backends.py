from unittest import mock

from celery_adaptive.health.backends.celery import CeleryHealthBackend
from celery_adaptive.health.backends.rabbitmq import RabbitMQHealthBackend
from celery_adaptive.health.backends.redis import RedisHealthBackend


def test_redis_backend_reports_latency(redis):
    result = RedisHealthBackend(redis).check()
    assert result.healthy
    assert result.latency_ms is not None
    assert result.degraded is False


def test_redis_backend_flags_latency_above_threshold(redis):
    result = RedisHealthBackend(redis, latency_warn_ms=0).check()
    assert result.degraded


def test_redis_backend_memory_percentage():
    client = mock.Mock()
    client.info.side_effect = lambda section: {
        "memory": {"used_memory": 90, "maxmemory": 100},
        "clients": {"connected_clients": 4},
    }[section]
    result = RedisHealthBackend(client, memory_warn_pct=80).check()
    assert result.memory_used_pct == 90.0
    assert result.connected_clients == 4
    assert result.degraded


def test_redis_backend_unreachable():
    client = mock.Mock()
    client.ping.side_effect = ConnectionError("refused")
    result = RedisHealthBackend(client).check()
    assert not result.healthy
    assert "refused" in result.error


def _app_with_inspect(stats, active):
    inspect = mock.Mock()
    inspect.stats.return_value = stats
    inspect.active.return_value = active
    app = mock.Mock()
    app.control.inspect.return_value = inspect
    return app


def test_celery_backend_reads_pool_size():
    app = _app_with_inspect(
        {"w1": {"pool": {"max-concurrency": 4}}, "w2": {"pool": {}}},
        {"w1": [{}] * 4, "w2": []},
    )
    result = CeleryHealthBackend(app, timeout=0.5).check()
    assert result.healthy
    assert result.worker_count == 2
    assert result.total_active == 4
    w1 = next(w for w in result.workers if w.hostname == "w1")
    assert w1.saturated
    assert not result.saturated  # w2 still has capacity
    app.control.inspect.assert_called_once_with(destination=None, timeout=0.5)


def test_celery_backend_no_workers():
    result = CeleryHealthBackend(_app_with_inspect(None, None)).check()
    assert not result.healthy
    assert "No Celery workers" in result.error


def test_rabbitmq_backend_parses_queues_and_quotes_vhost():
    response = mock.Mock()
    response.json.return_value = [
        {"name": "default", "messages": 5, "consumers": 0},
        {"name": "ignored", "messages": 1, "consumers": 1},
        {
            "name": "high",
            "messages": None,
            "consumers": 2,
            "message_stats": {"publish_details": {"rate": 1.5}},
        },
    ]
    with mock.patch("requests.get", return_value=response) as get:
        result = RabbitMQHealthBackend(
            "http://mq:15672/", vhost="/", queue_names=["default", "high"]
        ).check()

    assert get.call_args.args[0] == "http://mq:15672/api/queues/%2F"
    assert result.healthy
    assert result.depth_for("default") == 5
    assert result.depth_for("high") == 0
    assert result.depth_for("ignored") is None
    assert result.any_backed_up


def test_rabbitmq_backend_http_error():
    with mock.patch("requests.get", side_effect=OSError("timeout")):
        result = RabbitMQHealthBackend().check()
    assert not result.healthy
