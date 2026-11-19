"""
URLconf for the health and metrics views::

    path("celery-adaptive/", include("celery_adaptive.contrib.django.urls")),
"""

from django.urls import path

from .views import health_view, metrics_view

app_name = "celery_adaptive"

urlpatterns = [
    path("health/", health_view, name="health"),
    path("metrics/", metrics_view, name="metrics"),
]
