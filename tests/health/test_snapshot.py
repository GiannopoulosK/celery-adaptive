from celery_adaptive.health.backends.broker import BrokerHealthResult
from celery_adaptive.health.backends.celery import CeleryHealthResult, WorkerStats
from celery_adaptive.health.backends.rabbitmq import QueueStats, RabbitMQHealthResult
from celery_adaptive.health.backends.redis import RedisHealthResult
from celery_adaptive.health.checker import HealthSnapshot


def test_ok_when_everything_healthy():
    snap = HealthSnapshot(
        broker=BrokerHealthResult(True, queue_depths={"default": 3}),
        redis=RedisHealthResult(True, latency_ms=1.0),
    )
    assert snap.status == "ok"
    assert snap.healthy
    assert snap.issues == []


def test_down_when_broker_unreachable():
    snap = HealthSnapshot(broker=BrokerHealthResult(False, error="refused"))
    assert snap.status == "down"
    assert "broker unreachable: refused" in snap.issues


def test_degraded_on_backlog_and_redis_latency():
    snap = HealthSnapshot(
        broker=BrokerHealthResult(True, queue_depths={"default": 900}),
        redis=RedisHealthResult(True, latency_ms=80.0, degraded=True),
        depth_warn=500,
    )
    assert snap.status == "degraded"
    assert any("backed up (900)" in i for i in snap.issues)
    assert any("redis degraded" in i for i in snap.issues)


def test_worker_saturation_uses_pool_size():
    busy = CeleryHealthResult(True, workers=[WorkerStats("w1", 8, 8)])
    partly = CeleryHealthResult(True, workers=[WorkerStats("w1", 1, 8)])
    assert busy.saturated
    # Regression: one active task on an 8-slot worker is not "overloaded".
    assert not partly.saturated
    assert HealthSnapshot(celery=busy).status == "degraded"
    assert HealthSnapshot(celery=partly).status == "ok"


def test_queue_depth_prefers_broker_and_falls_back_to_management_api():
    snap = HealthSnapshot(
        broker=BrokerHealthResult(True, queue_depths={"a": 1}),
        rabbitmq=RabbitMQHealthResult(
            True, queues=[QueueStats("a", 99, 1, 0.0), QueueStats("b", 7, 1, 0.0)]
        ),
    )
    assert snap.queue_depth("a") == 1
    assert snap.queue_depth("b") == 7
    assert snap.queue_depth("c") is None


def test_to_dict_is_json_serializable():
    import json

    snap = HealthSnapshot(
        broker=BrokerHealthResult(True, queue_depths={"a": 1}),
        celery=CeleryHealthResult(True, workers=[WorkerStats("w", 1, 2)]),
    )
    data = json.loads(json.dumps(snap.to_dict()))
    assert data["status"] == "ok"
    assert data["queue_depths"] == {"a": 1}
    assert data["celery"]["saturated"] is False
