import time

from django.core.management.base import BaseCommand

from celery_adaptive.health.routing import get_monitor


class Command(BaseCommand):
    help = "Display live queue health status."

    def add_arguments(self, parser):
        parser.add_argument("--watch", action="store_true", help="Auto-refresh.")
        parser.add_argument("--interval", type=int, default=5, help="Refresh interval.")

    def handle(self, *args, **options):
        monitor = get_monitor()
        try:
            while True:
                snapshot = monitor.check_now()
                self.stdout.write(f"status: {snapshot.status}")
                for issue in snapshot.issues:
                    self.stdout.write(f"  - {issue}")
                if not options["watch"]:
                    break
                time.sleep(options["interval"])
        except KeyboardInterrupt:
            self.stdout.write("\nStopped.")
