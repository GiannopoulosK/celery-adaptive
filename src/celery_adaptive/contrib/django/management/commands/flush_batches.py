from celery import current_app
from django.core.management.base import BaseCommand

from celery_adaptive.batching.collector import BatchCollector
from celery_adaptive.redis_client import get_redis_client


class Command(BaseCommand):
    help = "Flush all pending batch buffers to Celery."

    def add_arguments(self, parser):
        parser.add_argument("--task", type=str, default=None, help="Task name filter.")
        parser.add_argument("--dry-run", action="store_true", help="Don't dispatch.")

    def handle(self, *args, **options):
        redis = get_redis_client()
        if not redis:
            self.stderr.write(self.style.ERROR("Could not connect to Redis."))
            return

        collector = BatchCollector(redis_client=redis)
        pending = collector.all_pending_tasks()

        if options["task"]:
            pending = [t for t in pending if options["task"] in t]

        if not pending:
            self.stdout.write("No pending batches found.")
            return

        total = 0
        for task_name in pending:
            count = collector.pending_count(task_name)
            if count == 0:
                continue

            if options["dry_run"]:
                self.stdout.write(
                    f"  [DRY-RUN] Would flush {count:>4} items -> {task_name}"
                )
                total += count
                continue

            payloads = collector.collect(task_name)
            if payloads:
                current_app.send_task(task_name, kwargs={"payloads": payloads})
                self.stdout.write(
                    self.style.SUCCESS(
                        f"  Flushed {len(payloads):>4} items -> {task_name}"
                    )
                )
                total += len(payloads)

        verb = "Would flush" if options["dry_run"] else "Flushed"
        self.stdout.write(f"\n{verb} {total} items total.")
