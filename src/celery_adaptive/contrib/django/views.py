from django.http import HttpResponse

from celery_adaptive.metrics.prometheus import _AVAILABLE, get_latest_metrics


def metrics_view(request):
    if not _AVAILABLE:
        return HttpResponse("Metrics disabled (prometheus_client missing)", status=503)

    content, content_type = get_latest_metrics()
    return HttpResponse(content, content_type=content_type)
