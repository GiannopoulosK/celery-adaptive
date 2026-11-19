"""
FastAPI integration.

Quick start::

    from fastapi import FastAPI
    from celery_adaptive.contrib.fastapi import init_app

    app = FastAPI()
    init_app(app, celery_app=celery_app, config={"REDIS_URL": "redis://..."})

    @app.post("/orders/{order_id}/receipt")
    async def send_receipt(order_id: int):
        result = await send_receipt_task.adelay(order_id)  # never blocks the loop
        return {"task_id": result.id}

This mounts ``GET /celery-adaptive/health`` and ``GET /celery-adaptive/metrics``.
For full control, use :func:`health_router` and :func:`lifespan` directly.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

from fastapi import APIRouter, FastAPI, Query
from fastapi.responses import JSONResponse, Response

import celery_adaptive
from celery_adaptive.conf import conf
from celery_adaptive.health.endpoint import health_response
from celery_adaptive.health.routing import get_monitor, reset_monitor
from celery_adaptive.metrics import prometheus

__all__ = ["health_router", "init_app", "lifespan"]


def health_router(
    *,
    prefix: str = "",
    tags: Sequence[str] = ("celery-adaptive",),
    include_metrics: bool = True,
) -> APIRouter:
    """
    Router exposing ``/health`` (JSON report) and optionally ``/metrics``.

    ``/health`` answers 503 when the broker is down, and with
    ``?strict=true`` also when degraded, for readiness probes. Handlers are
    sync functions, so FastAPI runs the checks in its threadpool.
    """
    router = APIRouter(prefix=prefix, tags=list(tags))

    @router.get("/health", summary="celery-adaptive health report")
    def health(
        strict: bool = Query(False, description="Treat 'degraded' as unhealthy."),
    ) -> JSONResponse:
        status, body = health_response(strict=strict)
        return JSONResponse(body, status_code=status)

    if include_metrics:

        @router.get("/metrics", summary="Prometheus metrics")
        def metrics() -> Response:
            if not prometheus._PROMETHEUS_AVAILABLE:
                return Response(
                    "Metrics disabled: pip install celery-adaptive[metrics]",
                    status_code=503,
                )
            content, content_type = prometheus.get_latest_metrics()
            return Response(content, media_type=content_type)

    return router


def lifespan(
    *,
    config: dict[str, Any] | None = None,
    redis_client=None,
    celery_app=None,
):
    """
    Lifespan that configures celery-adaptive on startup and stops the health
    monitor on shutdown::

        app = FastAPI(lifespan=lifespan(config={...}, celery_app=celery_app))

    To combine with your own lifespan, enter it from yours::

        @asynccontextmanager
        async def my_lifespan(app):
            async with lifespan(config={...})(app):
                yield
    """

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        celery_adaptive.setup(config=config, redis_client=redis_client, app=celery_app)
        if conf.HEALTH["ENABLED"]:
            get_monitor(celery_app).ensure_running()
        try:
            yield
        finally:
            reset_monitor()

    return _lifespan


def init_app(
    app: FastAPI,
    *,
    config: dict[str, Any] | None = None,
    redis_client=None,
    celery_app=None,
    prefix: str = "/celery-adaptive",
    include_metrics: bool = True,
) -> FastAPI:
    """Configure celery-adaptive now and mount :func:`health_router` on ``app``."""
    celery_adaptive.setup(config=config, redis_client=redis_client, app=celery_app)
    app.include_router(health_router(prefix=prefix, include_metrics=include_metrics))
    return app
