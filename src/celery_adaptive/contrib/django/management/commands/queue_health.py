import time

from django.core.management.base import BaseCommand

from celery_adaptive.conf import conf
from celery_adaptive.health.checker import HealthChecker
from celery_adaptive.health.decisions import Action
from celery_adaptive.redis_client import get_redis_client


class Command(BaseCommand):
    help = "Display live queue health status."

    def add_arguments(self, parser):
        parser.add_argument("--watch", action="store_true", help="Auto-refresh.")
        parser.add_argument("--interval", type=int, default=5, help="Refresh interval.")

    def handle(self, *args, **options):
        # Initialize from our global adaptive config
        checker = HealthChecker.from_conf(
            health_conf=getattr(conf, "HEALTH", {}), redis_client=get_redis_client()
        )

        try:
            while True:
                snapshot = checker.get_snapshot(force=True)
                self._print_snapshot(snapshot, checker)
                if not options["watch"]:
                    break
                time.sleep(options["interval"])
        except KeyboardInterrupt:
            self.stdout.write("\nStopped.")

    def _print_snapshot(self, snapshot, checker):
        self.stdout.write("\n" + "─" * 60)
        self.stdout.write(
            self.style.HTTP_INFO("  Celery Adaptive · System Health Status")
        )
        self.stdout.write("─" * 60)

        # ... (Your logic for printing Redis/Rabbit/Celery stats remains similar,
        # just ensure you use the updated CeleryAdaptive branding)

        # Decision section
        decision = checker.decide()
        status_color = {
            Action.ENQUEUE: self.style.SUCCESS,
            Action.DELAY: self.style.WARNING,
            Action.SYNC: self.style.ERROR,
        }.get(decision.action, str)

        self.stdout.write(
            f"  Decision: {status_color(decision.action.name)} - {decision.reason}"
        )
        self.stdout.write("─" * 60 + "\n")
