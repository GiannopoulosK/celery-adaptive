import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from celery_adaptive import adaptive_task  # noqa: E402
from celery_adaptive.conf import conf  # noqa: E402
from celery_adaptive.contrib.fastapi import health_router, init_app, lifespan  # noqa: E402
from celery_adaptive.health import routing  # noqa: E402
from celery_adaptive.health.backends.broker import (  # noqa: E402
    BrokerHealthBackend,
    BrokerHealthResult,
)

from ..conftest import queue_depth  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_monitor():
    routing.reset_monitor()
    yield
    routing.reset_monitor()


def test_init_app_configures_and_mounts_routes(celery_app, redis):
    app = init_app(
        FastAPI(),
        config={"DEDUPLICATION_TTL": 7},
        redis_client=redis,
        celery_app=celery_app,
    )
    assert conf.DEDUPLICATION_TTL == 7

    client = TestClient(app)
    response = client.get("/celery-adaptive/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["outbox_pending"] == 0

    metrics = client.get("/celery-adaptive/metrics")
    assert metrics.status_code == 200
    assert "celery_adaptive" in metrics.text


def test_health_503_when_broker_down(configured, celery_app, monkeypatch):
    monkeypatch.setattr(
        BrokerHealthBackend,
        "check",
        lambda self: BrokerHealthResult(False, error="refused"),
    )
    app = FastAPI()
    app.include_router(health_router())
    response = TestClient(app).get("/health")
    assert response.status_code == 503
    assert response.json()["status"] == "down"


def test_strict_mode_fails_on_degraded(configured, celery_app, monkeypatch):
    monkeypatch.setattr(
        BrokerHealthBackend,
        "check",
        lambda self: BrokerHealthResult(True, queue_depths={"default": 10_000}),
    )
    app = FastAPI()
    app.include_router(health_router(prefix="/ops", include_metrics=False))
    client = TestClient(app)
    assert client.get("/ops/health").status_code == 200
    assert client.get("/ops/health?strict=true").status_code == 503
    assert client.get("/ops/metrics").status_code == 404


def test_lifespan_starts_and_stops_monitor(celery_app):
    app = FastAPI(
        lifespan=lifespan(config={"HEALTH": {"ENABLED": True}}, celery_app=celery_app)
    )
    with TestClient(app):
        assert conf.HEALTH["ENABLED"] is True
        monitor = routing._monitor
        assert monitor is not None and monitor.running
    assert routing._monitor is None
    assert not monitor.running


def test_async_endpoint_dispatches_with_adelay(configured, celery_app):
    @adaptive_task(priority="high")
    def send_receipt(order_id):
        pass

    app = FastAPI()

    @app.post("/orders/{order_id}/receipt")
    async def receipt(order_id: int):
        result = await send_receipt.adelay(order_id)
        return {"task_id": result.id}

    response = TestClient(app).post("/orders/5/receipt")
    assert response.status_code == 200
    assert response.json()["task_id"]
    assert queue_depth(celery_app, "high_priority") == 1
