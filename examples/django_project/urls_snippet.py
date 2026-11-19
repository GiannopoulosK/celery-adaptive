# urls.py (excerpt)
from django.urls import include, path

urlpatterns = [
    # ...
    path("ops/celery/", include("celery_adaptive.contrib.django.urls")),
    # -> /ops/celery/health/ and /ops/celery/metrics/
]
