import functools
import logging
from collections.abc import Callable

from celery import current_app
from kombu.exceptions import OperationalError

from ..conf import conf
from ..exceptions import BrokerUnavailableError, ConfigurationError
from ..redis_client import get_redis_client
from .retry import get_retry_kwargs

logger = logging.getLogger(__name__)


def _execute_sync(func, args, kwargs, bind_instance=None):
    if bind_instance is not None:
        return func(bind_instance, *args, **kwargs)
    return func(*args, **kwargs)


def adaptive_task(  # noqa: C901
    *,
    priority: str = "default",
    deduplicate: bool = False,
    fallback_sync: bool = True,
    bind: bool = False,
    celery: dict | None = None,
) -> Callable:
    """
    Adaptive Celery task decorator.
    Wraps standard Celery tasks with health-aware routing and deduplication.
    """

    def decorator(func: Callable) -> Callable:  # noqa: C901
        celery_task = current_app.task(
            bind=bind,
            max_retries=conf.RETRY_MAX_ATTEMPTS,
            name=f"celery_adaptive.{func.__module__}.{func.__qualname__}",
            **(celery or {}),
        )(func)

        def _resolve_options(call_overrides: dict | None):
            opts = {
                "priority": priority,
                "deduplicate": deduplicate,
                "fallback_sync": fallback_sync,
            }
            if call_overrides:
                opts.update(call_overrides)

            if opts["priority"] not in conf.QUEUE_MAP:
                raise ConfigurationError(
                    f"Invalid priority '{opts['priority']}'. "
                    f"Valid: {sorted(conf.QUEUE_MAP)}"
                )

            opts["queue"] = conf.QUEUE_MAP[opts["priority"]]
            return opts

        def _enqueue_call(args, kwargs, options):
            task_name = celery_task.name
            bind_instance = celery_task if bind else None

            adaptive_overrides = kwargs.pop("_adaptive", None)
            resolved = _resolve_options(adaptive_overrides)

            # Deduplication
            if resolved["deduplicate"] and not conf.DISABLE_DEDUPLICATION:
                redis = get_redis_client()
                if redis is not None:
                    from .idempotency import acquire_dedup_lock

                    if not acquire_dedup_lock(redis, task_name, args, kwargs):
                        logger.debug(
                            "Task '%s' suppressed by deduplication.", task_name
                        )
                        return None
                else:
                    logger.warning(
                        "Redis unavailable — deduplication skipped for '%s'.",
                        task_name,
                    )

            # 2. Force sync
            if conf.FORCE_SYNC:
                logger.debug("FORCE_SYNC active — executing '%s' inline.", task_name)
                return _execute_sync(func, args, kwargs, bind_instance)

            # 3. Adaptive Routing & Dispatch
            try:
                from ..health.routing import route_task

                return route_task(
                    celery_task=celery_task,
                    queue=resolved["queue"],
                    args=args,
                    kwargs=kwargs,
                    options=options,
                    fallback_sync=resolved["fallback_sync"],
                    original_func=func,
                )

            except (OperationalError, Exception) as exc:
                logger.warning("Broker unreachable for task '%s': %s", task_name, exc)

                if resolved["fallback_sync"] and conf.SYNC_FALLBACK:
                    logger.info(
                        "Broker error. Falling back to sync for '%s'.", task_name
                    )
                    return _execute_sync(func, args, kwargs, bind_instance)

                if isinstance(exc, OperationalError):
                    raise BrokerUnavailableError(
                        f"Broker down for '{task_name}'."
                    ) from exc

                raise

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Calling the function directly executes it sync (standard Celery behavior)
            return func(*args, **kwargs)

        def delay(*args, **kwargs):
            return _enqueue_call(args, kwargs, {})

        def apply_async(args=None, kwargs=None, **options):
            return _enqueue_call(args or (), kwargs or {}, options)

        wrapper.delay = delay
        wrapper.apply_async = apply_async
        wrapper.celery_task = celery_task
        wrapper.get_retry_kwargs = get_retry_kwargs

        return wrapper

    return decorator
