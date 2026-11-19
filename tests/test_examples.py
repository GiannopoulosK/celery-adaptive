"""Keep the examples working: import them against the in-memory broker."""

import importlib.util
import sys
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient  # noqa: E402

from celery_adaptive.health import routing  # noqa: E402

from .conftest import queue_depth  # noqa: E402

EXAMPLES = Path(__file__).resolve().parents[1] / "examples"


@pytest.fixture
def fastapi_example(monkeypatch, redis):
    from celery import _state

    monkeypatch.setenv("BROKER_URL", "memory://")
    monkeypatch.setenv("RESULT_BACKEND", "cache+memory://")
    spec = importlib.util.spec_from_file_location(
        "fastapi_example", EXAMPLES / "fastapi_app" / "main.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules["fastapi_example"] = module
    finalizers = set(_state._on_app_finalizers)
    spec.loader.exec_module(module)

    from celery_adaptive.redis_client import set_redis_client

    set_redis_client(redis)
    monkeypatch.setattr(routing.get_monitor(), "ensure_running", lambda: None)
    yield module
    routing.reset_monitor()
    _state._on_app_finalizers.clear()
    _state._on_app_finalizers.update(finalizers)
    _state._deregister_app(module.celery_app)
    sys.modules.pop("fastapi_example", None)


def test_fastapi_example(fastapi_example):
    client = TestClient(fastapi_example.api)

    first = client.post("/orders/1/receipt").json()
    second = client.post("/orders/1/receipt").json()
    assert first["task_id"] == second["task_id"]  # deduplicated
    assert queue_depth(fastapi_example.celery_app, "critical") == 1

    assert client.post("/pages/home/view").json() == {"buffered": True}
    assert fastapi_example.record_page_views.pending() == 1

    health = client.get("/celery-adaptive/health")
    assert health.status_code == 200
    assert health.json()["status"] in {"ok", "degraded"}


def test_django_example_snippets_are_valid_python():
    for path in (EXAMPLES / "django_project").glob("*.py"):
        compile(path.read_text(), str(path), "exec")
