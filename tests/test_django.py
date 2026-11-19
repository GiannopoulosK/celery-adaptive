import pytest

django = pytest.importorskip("django")

from django.conf import settings  # noqa: E402

if not settings.configured:
    settings.configure(
        INSTALLED_APPS=["celery_adaptive.contrib.django"],
        CELERY_ADAPTIVE={"DEDUPLICATION_TTL": 42},
        CACHES={
            "default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache"}
        },
        ROOT_URLCONF=__name__,
    )
    django.setup()

urlpatterns: list = []


def test_app_ready_configures_library_from_settings():
    from celery_adaptive.conf import conf
    from celery_adaptive.contrib.django.conf import configure_from_django_settings

    configure_from_django_settings()
    assert conf.DEDUPLICATION_TTL == 42


def test_metrics_view_returns_prometheus_payload():
    from django.test import RequestFactory

    from celery_adaptive.contrib.django.views import metrics_view

    response = metrics_view(RequestFactory().get("/metrics"))
    assert response.status_code == 200
    assert b"celery_adaptive" in response.content
