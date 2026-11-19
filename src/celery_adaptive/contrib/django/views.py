"""Health and metrics views. Include ``celery_adaptive.contrib.django.urls``."""

from __future__ import annotations

from django.http import HttpResponse, JsonResponse

from celery_adaptive.health.endpoint import health_response
from celery_adaptive.metrics import prometheus


def health_view(request):
    """
    JSON health report. 503 when the broker is down; with ``?strict=1``
    also when degraded (for readiness probes).
    """
    strict = request.GET.get("strict", "").lower() in ("1", "true", "yes")
    status, body = health_response(strict=strict)
    return JsonResponse(body, status=status)


def metrics_view(request):
    """Prometheus exposition of the default registry."""
    if not prometheus._PROMETHEUS_AVAILABLE:
        return HttpResponse(
            "Metrics disabled: pip install celery-adaptive[metrics]", status=503
        )
    content, content_type = prometheus.get_latest_metrics()
    return HttpResponse(content, content_type=content_type)
