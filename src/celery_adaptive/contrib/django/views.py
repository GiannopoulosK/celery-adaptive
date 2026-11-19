from django.http import HttpResponse

from celery_adaptive.metrics import prometheus


def metrics_view(request):
    if not prometheus._PROMETHEUS_AVAILABLE:
        return HttpResponse("Metrics disabled (prometheus_client missing)", status=503)

    content, content_type = prometheus.get_latest_metrics()
    return HttpResponse(content, content_type=content_type)
