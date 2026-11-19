# Django example

The relevant pieces of a Django project using celery-adaptive. Copy them
into the standard Celery-with-Django layout
(https://docs.celeryq.dev/en/stable/django/first-steps-with-django.html).

- `settings_snippet.py`: `INSTALLED_APPS` and `CELERY_ADAPTIVE`
- `celery.py`: the Celery app, with bounded broker timeouts and beat entries
- `tasks.py`: adaptive and batch tasks, including `delay_on_commit`
- `urls_snippet.py`: health and metrics endpoints

Management commands:

    python manage.py queue_health [--json] [--watch]
    python manage.py flush_batches [--dry-run]
    python manage.py replay_outbox
