"""
``celery-adaptive`` command-line interface.

    celery-adaptive -A proj.celery:app health [--json] [--watch]
    celery-adaptive -A proj.celery:app flush-batches [--task NAME] [--dry-run]
    celery-adaptive -A proj.celery:app replay-outbox [--limit N]

``-A`` (or ``CELERY_ADAPTIVE_APP``) points at your Celery app, the same way
``celery -A`` does. Importing that module should call
``celery_adaptive.setup()``. The ``health`` command exits with 0 (ok),
1 (degraded) or 2 (down), so it can serve as a probe or cron check.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
import time
from collections.abc import Sequence

from .report import EXIT_CODES, render_snapshot


def load_app(path: str):
    """Import a Celery app from ``module:attr`` or ``module`` (like celery -A)."""
    from celery import Celery

    module_name, _, attr = path.partition(":")
    sys.path.insert(0, os.getcwd())
    module = importlib.import_module(module_name)
    if attr:
        app = getattr(module, attr)
    else:
        app = next(
            (
                getattr(module, name)
                for name in ("app", "celery")
                if isinstance(getattr(module, name, None), Celery)
            ),
            None,
        )
        if app is None:
            raise SystemExit(f"No Celery app found in module '{module_name}'.")
    if not isinstance(app, Celery):
        raise SystemExit(f"'{path}' is not a Celery application.")
    return app


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="celery-adaptive",
        description="Inspect and operate celery-adaptive.",
    )
    parser.add_argument(
        "-A",
        "--app",
        default=os.environ.get("CELERY_ADAPTIVE_APP"),
        help="Celery app as module:attr (or set CELERY_ADAPTIVE_APP).",
    )
    parser.add_argument("--redis-url", help="Override the Redis URL.")
    commands = parser.add_subparsers(dest="command", required=True)

    health = commands.add_parser("health", help="Check broker, Redis and queues.")
    health.add_argument("--json", action="store_true", help="Print JSON.")
    health.add_argument("--watch", action="store_true", help="Refresh until Ctrl+C.")
    health.add_argument("--interval", type=float, default=5.0)
    health.add_argument(
        "--inspect", action="store_true", help="Also query workers (celery inspect)."
    )

    flush = commands.add_parser("flush-batches", help="Publish buffered batches.")
    flush.add_argument("--task", help="Exact batch task name.")
    flush.add_argument("--dry-run", action="store_true")

    replay = commands.add_parser("replay-outbox", help="Publish outbox tasks.")
    replay.add_argument("--limit", type=int)
    return parser


def _cmd_health(app, args) -> int:
    from .conf import conf
    from .core import outbox
    from .health.checker import HealthMonitor
    from .redis_client import get_redis_client

    health = dict(conf.HEALTH)
    if args.inspect:
        health["CELERY_INSPECT"] = True
    redis = get_redis_client()
    monitor = HealthMonitor.from_conf(
        app, health, queue_map=conf.QUEUE_MAP, redis_client=redis
    )

    while True:
        snapshot = monitor.check_now()
        pending = _safe(lambda: outbox.pending(redis)) if redis else None
        if args.json:
            data = snapshot.to_dict()
            data["outbox_pending"] = pending
            print(json.dumps(data, indent=2, default=str))
        else:
            print(render_snapshot(snapshot, outbox_pending=pending))
        if not args.watch:
            return EXIT_CODES[snapshot.status]
        time.sleep(args.interval)
        print()


def _cmd_flush(app, args) -> int:
    from .batching import batch_tasks, flush_all

    app.loader.import_default_modules()
    if args.dry_run:
        for task in batch_tasks(app):
            if args.task in (None, task.name):
                print(f"{task.pending():>8}  {task.name}")
        return 0
    flushed = flush_all(app, name_filter=args.task)
    for name, count in flushed.items():
        print(f"flushed {count:>6}  {name}")
    print(f"{sum(flushed.values())} payload(s) published.")
    return 0


def _cmd_replay(app, args) -> int:
    from .core import outbox

    count = outbox.replay(app, limit=args.limit)
    remaining = outbox.pending()
    print(f"Replayed {count} task(s); {remaining} remaining.")
    return 0 if remaining == 0 or args.limit else 1


def _safe(fn):
    try:
        return fn()
    except Exception:
        return None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if not args.app:
        print("error: pass -A module:app or set CELERY_ADAPTIVE_APP", file=sys.stderr)
        return 64

    app = load_app(args.app)
    if args.redis_url:
        import redis

        from .redis_client import set_redis_client

        set_redis_client(redis.Redis.from_url(args.redis_url))

    handlers = {
        "health": _cmd_health,
        "flush-batches": _cmd_flush,
        "replay-outbox": _cmd_replay,
    }
    try:
        return handlers[args.command](app, args)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
