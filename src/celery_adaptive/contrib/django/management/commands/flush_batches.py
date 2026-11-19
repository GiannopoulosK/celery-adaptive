from celery import current_app
from django.core.management.base import BaseCommand

from celery_adaptive.batching import batch_tasks, flush_all


class Command(BaseCommand):
    help = "Flush buffered batch payloads to Celery."

    def add_arguments(self, parser):
        parser.add_argument("--task", default=None, help="Exact batch task name.")
        parser.add_argument(
            "--dry-run", action="store_true", help="Show pending counts only."
        )

    def handle(self, *args, **options):
        if options["dry_run"]:
            for task in batch_tasks(current_app):
                if options["task"] in (None, task.name):
                    self.stdout.write(f"{task.pending():>6}  {task.name}")
            return

        flushed = flush_all(current_app, name_filter=options["task"])
        for name, count in flushed.items():
            self.stdout.write(self.style.SUCCESS(f"Flushed {count:>6}  {name}"))
        self.stdout.write(f"Flushed {sum(flushed.values())} payload(s) in total.")
