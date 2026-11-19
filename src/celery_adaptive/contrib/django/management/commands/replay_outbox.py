from celery import current_app
from django.core.management.base import BaseCommand

from celery_adaptive.core import outbox


class Command(BaseCommand):
    help = "Publish tasks stored in the outbox while the broker was down."

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=None)

    def handle(self, *args, **options):
        count = outbox.replay(current_app, limit=options["limit"])
        self.stdout.write(
            f"Replayed {count} task(s); {outbox.pending()} remaining in the outbox."
        )
