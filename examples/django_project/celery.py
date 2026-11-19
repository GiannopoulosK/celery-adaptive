# proj/celery.py
import os

from celery import Celery

import celery_adaptive

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "proj.settings")

app = Celery("proj")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
app.conf.include = [*app.conf.include, "celery_adaptive.tasks"]
app.conf.beat_schedule = {
    **(app.conf.beat_schedule or {}),
    **celery_adaptive.beat_schedule(),
}

# Fail fast (about 1s) instead of 4s on AMQP or forever on the Redis transport.
celery_adaptive.tune_broker_timeouts(app)
