"""Celery app imported by the CLI tests via ``-A tests.fixtures.cli_app:app``."""

from celery import Celery

app = Celery("cli", broker="memory://")
