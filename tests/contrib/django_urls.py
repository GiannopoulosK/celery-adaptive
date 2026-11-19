from django.urls import include, path

urlpatterns = [
    path("celery-adaptive/", include("celery_adaptive.contrib.django.urls")),
]
