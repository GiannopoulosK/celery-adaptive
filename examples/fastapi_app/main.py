"""
FastAPI + Celery example for celery-adaptive.

    docker compose up -d                      # Redis + RabbitMQ
    pip install "celery-adaptive[all]" uvicorn
    celery -A main:celery_app worker -B -l info   # worker + beat
    uvicorn main:api --reload

Then stop RabbitMQ (docker compose stop rabbitmq) and keep posting orders:
receipts are stored in the Redis outbox and replayed when it comes back.
"""

from __future__ import annotations

import os

from celery import Celery
from fastapi import FastAPI

import celery_adaptive
from celery_adaptive import adaptive_task, batch_task
from celery_adaptive.contrib.fastapi import init_app

BROKER_URL = os.environ.get("BROKER_URL", "amqp://guest:guest@localhost:5672//")
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    "example",
    broker=BROKER_URL,
    backend=os.environ.get("RESULT_BACKEND", REDIS_URL),
    include=["celery_adaptive.tasks"],  # flush_batches / replay_outbox
)
celery_app.conf.beat_schedule = celery_adaptive.beat_schedule()

CONFIG = {
    "REDIS_URL": REDIS_URL,
    "FALLBACK": "outbox",  # park tasks in Redis while the broker is down
    "QUEUE_MAP": {"high": "critical", "default": "default", "low": "bulk"},
    "HEALTH": {"ENABLED": True, "REROUTE": {"default": "overflow"}},
}

api = init_app(
    FastAPI(title="celery-adaptive example"), config=CONFIG, celery_app=celery_app
)


@adaptive_task(app=celery_app, priority="high", deduplicate=True)
def send_receipt(order_id: int) -> str:
    return f"receipt sent for order {order_id}"


@batch_task(app=celery_app, max_batch_size=200, max_wait_ms=2000, queue="bulk")
def record_page_views(payloads: list[dict]) -> int:
    # One bulk insert instead of one task per page view.
    return len(payloads)


@api.post("/orders/{order_id}/receipt")
async def order_receipt(order_id: int):
    # Duplicate clicks while the first receipt is in flight return the same id.
    result = await send_receipt.adelay(order_id)
    return {"task_id": result.id, "state": result.state}


@api.post("/pages/{page}/view")
async def page_view(page: str):
    await record_page_views.apush(page=page)
    return {"buffered": True}
