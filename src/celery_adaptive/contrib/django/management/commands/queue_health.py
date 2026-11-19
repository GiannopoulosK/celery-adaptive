import json
import time

from django.core.management.base import BaseCommand, CommandError

from celery_adaptive.core import outbox
from celery_adaptive.health.routing import get_monitor
from celery_adaptive.redis_client import get_redis_client
from celery_adaptive.report import EXIT_CODES, render_snapshot


class Command(BaseCommand):
    help = "Check broker, Redis and queue health (exit code 1 degraded, 2 down)."

    def add_arguments(self, parser):
        parser.add_argument("--json", action="store_true", help="Print JSON.")
        parser.add_argument("--watch", action="store_true", help="Refresh until ^C.")
        parser.add_argument("--interval", type=float, default=5.0)

    def handle(self, *args, **options):
        monitor = get_monitor()
        redis = get_redis_client()
        try:
            while True:
                snapshot = monitor.check_now()
                pending = outbox.pending(redis) if redis else None
                if options["json"]:
                    data = {**snapshot.to_dict(), "outbox_pending": pending}
                    self.stdout.write(json.dumps(data, indent=2, default=str))
                else:
                    self.stdout.write(render_snapshot(snapshot, outbox_pending=pending))
                if not options["watch"]:
                    break
                time.sleep(options["interval"])
        except KeyboardInterrupt:
            return

        code = EXIT_CODES[snapshot.status]
        if code:
            raise CommandError(f"Health is {snapshot.status}.", returncode=code)
