import json

import pytest

from celery_adaptive import cli
from celery_adaptive.batching import BatchCollector
from celery_adaptive.core import outbox
from celery_adaptive.health.backends.broker import (
    BrokerHealthBackend,
    BrokerHealthResult as Result,
)

APP = "tests.fixtures.cli_app:app"


@pytest.fixture
def cli_app(configured):
    from celery import _state

    from tests.fixtures.cli_app import app

    yield app
    # Keep later tests' shared tasks from registering on this module-level app.
    _state._deregister_app(app)


def test_load_app_by_module_and_attribute(cli_app):
    assert cli.load_app(APP) is cli_app
    assert cli.load_app("tests.fixtures.cli_app") is cli_app


def test_load_app_rejects_non_celery_objects():
    with pytest.raises(SystemExit):
        cli.load_app("tests.fixtures.cli_app:Celery")


def test_missing_app_argument(monkeypatch, capsys):
    monkeypatch.delenv("CELERY_ADAPTIVE_APP", raising=False)
    assert cli.main(["health"]) == 64
    assert "-A" in capsys.readouterr().err


def test_health_text_ok(cli_app, capsys):
    assert cli.main(["-A", APP, "health"]) == 0
    out = capsys.readouterr().out
    assert "health: OK" in out
    assert "broker" in out
    assert "outbox" in out


def test_health_json(cli_app, capsys):
    assert cli.main(["-A", APP, "health", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["status"] == "ok"
    assert data["outbox_pending"] == 0
    assert "default" in data["queue_depths"]


def test_health_exit_code_when_broker_down(cli_app, monkeypatch, capsys):
    monkeypatch.setattr(
        BrokerHealthBackend, "check", lambda self: Result(False, error="refused")
    )
    assert cli.main(["-A", APP, "health"]) == 2
    assert "DOWN" in capsys.readouterr().out


def test_health_exit_code_when_degraded(cli_app, monkeypatch, capsys):
    monkeypatch.setattr(
        BrokerHealthBackend,
        "check",
        lambda self: Result(True, latency_ms=1.0, queue_depths={"default": 10_000}),
    )
    assert cli.main(["-A", APP, "health"]) == 1
    assert "backed up" in capsys.readouterr().out


def test_replay_outbox(cli_app, monkeypatch, capsys):
    sent = []
    monkeypatch.setattr(cli_app, "send_task", lambda name, **kw: sent.append(name))
    outbox.store("some.task", "id-1", (), {}, {})
    assert cli.main(["-A", APP, "replay-outbox"]) == 0
    assert sent == ["some.task"]
    assert "Replayed 1 task(s); 0 remaining." in capsys.readouterr().out


def test_flush_batches(cli_app, configured, capsys):
    from celery_adaptive import batch_task

    @batch_task(app=cli_app, max_batch_size=100)
    def cli_events(payloads):
        pass

    BatchCollector(configured).add(cli_events.name, {"a": 1})

    assert cli.main(["-A", APP, "flush-batches", "--dry-run"]) == 0
    assert f"       1  {cli_events.name}" in capsys.readouterr().out

    assert cli.main(["-A", APP, "flush-batches"]) == 0
    assert "1 payload(s) published." in capsys.readouterr().out
