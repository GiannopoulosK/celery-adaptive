import json
import re
from importlib.resources import files

import pytest
from prometheus_client import REGISTRY

import celery_adaptive
from celery_adaptive import adaptive_task, batch_task
from celery_adaptive.core.circuit import get_breaker


def sample(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


@pytest.fixture
def task_name(request):
    return request.node.name


def test_publish_is_counted_per_queue(configured, celery_app):
    @adaptive_task(priority="high")
    def metric_publish():
        pass

    before = sample(
        "celery_adaptive_tasks_published_total",
        task_name=metric_publish.name,
        queue="high_priority",
    )
    metric_publish.delay()
    after = sample(
        "celery_adaptive_tasks_published_total",
        task_name=metric_publish.name,
        queue="high_priority",
    )
    assert after == before + 1


def test_fallback_and_circuit_state_recorded(celery_app, broker_down):
    celery_adaptive.setup(config={"CIRCUIT_BREAKER_THRESHOLD": 1})

    @adaptive_task
    def metric_fallback():
        pass

    metric_fallback.delay()
    assert (
        sample(
            "celery_adaptive_fallbacks_total",
            task_name=metric_fallback.name,
            strategy="sync",
        )
        >= 1
    )
    broker = metric_fallback._broker_key()
    assert sample("celery_adaptive_circuit_state", broker=broker) == 2
    get_breaker(broker).record_success()
    assert sample("celery_adaptive_circuit_state", broker=broker) == 0


def test_dedup_hits_recorded(configured, celery_app):
    @adaptive_task(deduplicate=True)
    def metric_dedup(x):
        pass

    metric_dedup.delay(1)
    metric_dedup.delay(1)
    assert sample("celery_adaptive_dedup_hits_total", task_name=metric_dedup.name) == 1


def test_batch_flush_recorded(configured, celery_app):
    @batch_task(max_batch_size=2, max_wait_ms=60_000)
    def metric_batch(payloads):
        pass

    metric_batch.push(a=1)
    metric_batch.push(a=2)
    assert (
        sample(
            "celery_adaptive_batch_flushes_total",
            task_name=metric_batch.name,
            trigger="size",
        )
        == 1
    )
    assert sample("celery_adaptive_batch_size_sum", task_name=metric_batch.name) == 2


def test_dashboard_only_queries_existing_metrics():
    dashboard = json.loads(
        files("celery_adaptive.metrics").joinpath("dashboard.json").read_text()
    )
    exprs = [
        target["expr"]
        for panel in dashboard["panels"]
        for target in panel.get("targets", [])
    ]
    used = set(re.findall(r"celery_adaptive_[a-z_]+", " ".join(exprs)))
    suffixes = {
        "counter": ("_total",),
        "histogram": ("_sum", "_count", "_bucket"),
        "gauge": ("",),
    }
    exported = {
        metric.name + suffix
        for metric in REGISTRY.collect()
        if metric.name.startswith("celery_adaptive")
        for suffix in suffixes.get(metric.type, ("",))
    }
    assert used, "dashboard has no queries"
    assert used <= exported, f"unknown metrics in dashboard: {used - exported}"
