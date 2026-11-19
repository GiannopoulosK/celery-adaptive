import json

import pytest

django = pytest.importorskip("django")

from django.conf import settings  # noqa: E402

if not settings.configured:
    settings.configure(
        INSTALLED_APPS=["celery_adaptive.contrib.django"],
        CELERY_ADAPTIVE={"DEDUPLICATION_TTL": 42},
        DATABASES={
            "default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}
        },
        CACHES={
            "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"},
            "redis": {
                "BACKEND": "django.core.cache.backends.redis.RedisCache",
                "LOCATION": "redis://localhost:6390/2",
            },
        },
        ROOT_URLCONF="tests.contrib.django_urls",
        ALLOWED_HOSTS=["testserver"],
    )
    django.setup()

from django.core.management import call_command  # noqa: E402
from django.core.management.base import CommandError  # noqa: E402
from django.db import transaction  # noqa: E402
from django.test import Client  # noqa: E402

from celery_adaptive.contrib.django import DjangoAdaptiveTask, adaptive_task  # noqa: E402
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


def test_app_ready_configures_library_from_settings():
    from celery_adaptive.conf import conf
    from celery_adaptive.contrib.django.conf import configure_from_django_settings

    configure_from_django_settings()
    assert conf.DEDUPLICATION_TTL == 42


def test_locmem_cache_is_not_registered_as_redis():
    from celery_adaptive.contrib.django.cache import register_django_redis_client

    assert register_django_redis_client() is False


def test_builtin_redis_cache_client_is_reused():
    from celery_adaptive.contrib.django.cache import register_django_redis_client
    from celery_adaptive.redis_client import get_redis_client

    assert register_django_redis_client("redis") is True
    client = get_redis_client()
    assert client.connection_pool.connection_kwargs["db"] == 2


def test_redis_url_setting_takes_precedence():
    import celery_adaptive
    from celery_adaptive.contrib.django.cache import register_django_redis_client

    celery_adaptive.setup(config={"REDIS_URL": "redis://elsewhere:6379/0"})
    assert register_django_redis_client("redis") is False


# --- on_commit dispatch ----------------------------------------------------


def test_django_adaptive_task_uses_django_base(configured):
    @adaptive_task(priority="high")
    def notify(user_id):
        pass

    assert isinstance(notify._get_current_object(), DjangoAdaptiveTask)


def test_delay_on_commit_waits_for_commit(configured, celery_app):
    @adaptive_task(priority="high")
    def notify(user_id):
        pass

    with transaction.atomic():
        result = notify.delay_on_commit(1)
        assert queue_depth(celery_app, "high_priority") == 0
    assert queue_depth(celery_app, "high_priority") == 1
    assert result.id


def test_delay_on_commit_skipped_on_rollback(configured, celery_app):
    @adaptive_task
    def notify(user_id):
        pass

    with pytest.raises(RuntimeError), transaction.atomic():
        notify.delay_on_commit(1)
        raise RuntimeError("rollback")
    assert queue_depth(celery_app, "celery") == 0


def test_apply_async_on_commit_keeps_options_and_task_id(configured, celery_app):
    @adaptive_task
    def notify(user_id):
        pass

    result = notify.apply_async_on_commit((1,), task_id="fixed", queue="special")
    assert result.id == "fixed"
    assert queue_depth(celery_app, "special") == 1  # no transaction: immediate


# --- views ----------------------------------------------------------------


def test_health_view_ok(configured, celery_app):
    response = Client().get("/celery-adaptive/health/")
    assert response.status_code == 200
    body = json.loads(response.content)
    assert body["status"] == "ok"
    assert body["outbox_pending"] == 0
    assert "circuit_breakers" in body


def test_health_view_503_when_broker_down(configured, celery_app, monkeypatch):
    monkeypatch.setattr(
        BrokerHealthBackend,
        "check",
        lambda self: BrokerHealthResult(False, error="refused"),
    )
    response = Client().get("/celery-adaptive/health/")
    assert response.status_code == 503
    assert json.loads(response.content)["status"] == "down"


def test_health_view_strict_fails_when_degraded(configured, celery_app, monkeypatch):
    monkeypatch.setattr(
        BrokerHealthBackend,
        "check",
        lambda self: BrokerHealthResult(True, queue_depths={"default": 10_000}),
    )
    assert Client().get("/celery-adaptive/health/").status_code == 200
    assert Client().get("/celery-adaptive/health/?strict=1").status_code == 503


def test_metrics_view_returns_prometheus_payload():
    response = Client().get("/celery-adaptive/metrics/")
    assert response.status_code == 200
    assert b"celery_adaptive" in response.content


# --- management commands --------------------------------------------------


def test_queue_health_command(configured, celery_app, capsys):
    call_command("queue_health")
    assert "health: OK" in capsys.readouterr().out


def test_queue_health_command_fails_when_down(configured, celery_app, monkeypatch):
    monkeypatch.setattr(
        BrokerHealthBackend,
        "check",
        lambda self: BrokerHealthResult(False, error="refused"),
    )
    with pytest.raises(CommandError) as exc:
        call_command("queue_health", "--json")
    assert exc.value.returncode == 2


def test_replay_outbox_command(configured, celery_app, monkeypatch, capsys):
    from celery_adaptive.core import outbox

    sent = []
    monkeypatch.setattr(celery_app, "send_task", lambda name, **kw: sent.append(name))
    outbox.store("t", "id", (), {}, {})
    call_command("replay_outbox")
    assert sent == ["t"]
    assert "Replayed 1 task(s)" in capsys.readouterr().out


def test_flush_batches_command(configured, celery_app, capsys):
    from celery_adaptive import batch_task

    @batch_task(max_batch_size=100, max_wait_ms=60_000)
    def django_events(payloads):
        pass

    django_events.push(a=1)
    call_command("flush_batches", "--dry-run")
    assert django_events.name in capsys.readouterr().out
    call_command("flush_batches")
    assert "Flushed 1 payload(s) in total." in capsys.readouterr().out
