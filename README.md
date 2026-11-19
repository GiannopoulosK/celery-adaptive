# celery-adaptive

[![CI](https://github.com/GiannopoulosK/celery-adaptive/actions/workflows/ci.yml/badge.svg)](https://github.com/GiannopoulosK/celery-adaptive/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13-blue)
![Celery](https://img.shields.io/badge/celery-5.3%2B-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

**Failure-aware task dispatch for Celery.** `celery-adaptive` sits between your
web app and the broker and decides what happens when calling `.delay()` is not
as simple as it looks. For example, the broker may be down, an identical task
may already be queued, a queue may be backed up, or you may be inside a
database transaction.

```python
from celery_adaptive import adaptive_task

@adaptive_task(priority="high", deduplicate=True, fallback="outbox")
def send_receipt(order_id):
    ...

send_receipt.delay(42)  # behaves like Celery, with the safety net below
```

## Why

Plain Celery leaves several production problems to the caller:

| Problem | Plain Celery | celery-adaptive |
|---|---|---|
| Broker unreachable | `.delay()` raises after 4s on AMQP, or **hangs indefinitely** on the Redis transport | Bounded timeout, then a circuit breaker skips the broker entirely; the task runs inline, is parked in an outbox, or raises (your choice) |
| Double-clicks, retries, webhooks firing twice | Every call publishes a task | Identical in-flight calls return the original task's `AsyncResult` |
| Dispatching inside a DB transaction | The worker may run before the commit (or after a rollback) | `delay_on_commit()` publishes only after a successful commit |
| One task per event (page views, audit logs) | Thousands of tiny tasks | Buffer in Redis, publish in batches by size or age |
| A backed-up queue | New work waits behind the backlog | Health monitor reroutes to an overflow queue |
| Async web frameworks | `.delay()` blocks the event loop | `await task.adelay()` |

All of this is built on a real `celery.Task` subclass, so signatures, chains,
groups, chords, `bind=True`, `self.retry()`, `autoretry_for`, task routes and
eager mode keep working.

## Installation

```bash
pip install celery-adaptive            # core (only depends on Celery)
pip install "celery-adaptive[all]"     # + redis, django, fastapi, prometheus, requests
```

Extras: `redis`, `django`, `fastapi`, `metrics` (Prometheus), `rabbitmq` (management API
backend). Requires Python 3.11+ and Celery 5.3+. Deduplication, batching and the outbox
need Redis 6.2+.

## Quickstart

```python
# proj/celery.py
from celery import Celery
import celery_adaptive

app = Celery("proj", broker="amqp://localhost", include=["celery_adaptive.tasks"])
app.conf.beat_schedule = celery_adaptive.beat_schedule()   # flush batches, replay outbox

celery_adaptive.setup(
    app=app,                     # bound broker timeouts by BROKER_TIMEOUT (1s)
    config={
        "REDIS_URL": "redis://localhost:6379/1",
        "FALLBACK": "outbox",
        "QUEUE_MAP": {"high": "critical", "default": "default", "low": "bulk"},
    },
)
```

```python
# proj/tasks.py
from celery_adaptive import adaptive_task

@adaptive_task(priority="high", deduplicate=True, bind=True, max_retries=5)
def charge(self, order_id): ...

result = charge.delay(17)                       # AsyncResult (or EagerResult if run inline)
charge.apply_async((17,), adaptive={"fallback": "raise"})  # per-call overrides
charge.s(17) | notify.s()                       # canvas works as usual
```

`setup()` is optional: without it the library runs on defaults. You can also use the
task class directly: `@app.task(base=AdaptiveTask, adaptive_priority="high")`.

## How a dispatch is decided

```mermaid
flowchart TD
    A["task.delay(...)"] --> B[Resolve queue from priority]
    B --> C{deduplicate?}
    C -- identical task in flight --> R1[Return its AsyncResult]
    C -- no / first call --> D{FORCE_SYNC?}
    D -- yes --> R2[Run inline, EagerResult]
    D -- no --> E{HEALTH enabled?}
    E -- queue backed up and overflow configured --> F[Reroute to overflow queue]
    E -- otherwise --> G
    F --> G{Circuit breaker open?}
    G -- open --> H
    G -- closed / probe --> I[Publish with bounded retry]
    I -- ok --> R3[AsyncResult]
    I -- connection error --> H{Fallback strategy}
    H -- sync --> R2
    H -- outbox --> R4[Store in Redis, replay later, AsyncResult]
    H -- raise --> R5[BrokerUnavailableError]
```

Only broker *connectivity* errors trigger a fallback. Bugs, serialization errors and bad
arguments always propagate.

## Features

### Broker outages: timeouts, circuit breaker and fallbacks

- **Bounded timeouts.** `setup(app=app)` (or `tune_broker_timeouts(app)`) caps
  `broker_connection_timeout` and, for Redis brokers, sets socket timeouts. Without
  this, a publish to an unreachable Redis host blocks forever.
- **Circuit breaker.** After `CIRCUIT_BREAKER_THRESHOLD` consecutive failures, publishes
  are skipped for `CIRCUIT_BREAKER_RESET` seconds, then a single probe is let through.
  During an outage, calls go straight to the fallback instead of each waiting for a
  timeout.
- **Fallback strategies** (`FALLBACK`, or per task/call):

| Strategy | Result | Use when |
|---|---|---|
| `"sync"` (default) | Runs the task in the caller's process and returns an `EagerResult` with a real request context | Tasks are quick and must happen |
| `"outbox"` | Stores the task in Redis and returns its `AsyncResult`. It is replayed with the **same task id** when the broker recovers, so the handle you returned to the client stays valid | Tasks are slow or must not run in the web process |
| `"raise"` | Raises `BrokerUnavailableError` | The caller should decide |

The outbox is replayed automatically when the circuit closes (`OUTBOX_AUTO_REPLAY`), by
the `celery_adaptive.replay_outbox` beat task, or with `celery-adaptive replay-outbox`.
Replay is crash-safe (entries move through a processing list) and order-preserving. Use
a Redis that is **not** your broker for the outbox, or both will go down together.

### Deduplication of in-flight tasks

```python
@adaptive_task(deduplicate=True, dedup_key=lambda user_id, **_: user_id)
def sync_user(user_id, requested_at=None): ...

a = sync_user.delay(7, requested_at="10:00")
b = sync_user.delay(7, requested_at="10:01")
assert a.id == b.id
```

The first call stores its task id under a key derived from the task name and arguments.
Later identical calls return that task's `AsyncResult` instead of publishing again. The
worker releases the key when the task succeeds or fails. Retries keep it, and
`DEDUPLICATION_TTL` is only a safety net. Arguments that cannot be keyed reliably (e.g.
objects without a stable representation) raise a `TypeError` that asks for a `dedup_key`;
they are never keyed by memory address. Redis errors fail open.

### Batching

```python
from celery_adaptive import batch_task

@batch_task(max_batch_size=500, max_wait_ms=2000, queue="bulk")
def record_events(payloads):
    Event.objects.bulk_create(Event(**p) for p in payloads)

record_events.push(user_id=1, kind="click")      # buffered in Redis
await record_events.apush(user_id=2, kind="view")
```

A batch is published when `max_batch_size` payloads are buffered, or on the next push once
the oldest is `max_wait_ms` old. Idle buffers are drained by the `celery_adaptive.flush_batches`
beat task. All bookkeeping happens in atomic Lua scripts that use the Redis server clock, so
producers on different hosts agree. If publishing fails, payloads go back into the buffer.

### Health monitoring and rerouting

```python
"HEALTH": {
    "ENABLED": True,
    "QUEUE_DEPTH_CRITICAL": 5000,
    "REROUTE": {"default": "overflow"},   # overflow served by extra/autoscaled workers
}
```

A background thread (fork-safe for gunicorn and prefork workers) checks broker
reachability and queue depth every `INTERVAL` seconds via a passive `queue_declare`. This
works for RabbitMQ and Redis without the management plugin. Dispatch only reads the
latest snapshot, so health checks never add latency to `.delay()`. When a queue reaches
`QUEUE_DEPTH_CRITICAL` and its overflow queue is less backed up, new tasks are rerouted.
When the broker check fails, the monitor trips the circuit breaker before any request
has to time out.

Optional backends: Redis latency/memory (`REDIS`), worker pool saturation via
`celery inspect` (`CELERY_INSPECT`), and the RabbitMQ management API (`RABBITMQ_URL`).

### Async (FastAPI, Starlette, aiohttp)

`await task.adelay(...)`, `await task.aapply_async(...)` and `await batch.apush(...)` run
dispatch in a worker thread, so the event loop is never blocked, including when a
fallback runs the task inline.

## Django

```python
INSTALLED_APPS = [..., "celery_adaptive.contrib.django"]

CELERY_ADAPTIVE = {
    "FALLBACK": "outbox",
    "HEALTH": {"ENABLED": True},
}

# urls.py
path("ops/celery/", include("celery_adaptive.contrib.django.urls")),  # health/ + metrics/
```

```python
from celery_adaptive.contrib.django import adaptive_task

@adaptive_task(priority="high")
def send_receipt(order_id): ...

with transaction.atomic():
    order = Order.objects.create(...)
    send_receipt.delay_on_commit(order.id)   # published only after COMMIT
```

- Settings come from `settings.CELERY_ADAPTIVE` and are validated at startup.
- The Redis client is reused from your cache when it is django-redis or Django's
  `RedisCache` (unless `REDIS_URL` is set).
- Management commands: `queue_health [--json] [--watch]` (exits 1 when degraded and 2
  when down), `flush_batches [--dry-run]`, `replay_outbox`.

See [examples/django_project](examples/django_project).

## FastAPI

```python
from celery_adaptive.contrib.fastapi import init_app

api = init_app(FastAPI(), celery_app=celery_app, config={"REDIS_URL": "redis://..."})

@api.post("/orders/{order_id}/receipt")
async def receipt(order_id: int):
    result = await send_receipt.adelay(order_id)
    return {"task_id": result.id}
```

`init_app` mounts `GET /celery-adaptive/health` and `GET /celery-adaptive/metrics`. For
more control use `health_router(prefix=...)` and `lifespan(...)`, which configures on
startup and stops the health monitor on shutdown. A runnable app is in
[examples/fastapi_app](examples/fastapi_app).

## Health endpoint and CLI

The health endpoints (Django and FastAPI) return a JSON report with status
`ok`, `degraded` or `down`, the list of issues, queue depths, the circuit
breaker states and the outbox backlog. They answer **503** when the broker is down, and
with `?strict=1` also when degraded, which is useful for readiness probes.

```console
$ celery-adaptive -A proj.celery:app health
celery-adaptive health: DEGRADED  (12:34:33)

  broker            up (1.84ms)
  redis             up (0.41ms, memory 37.5%)

  queues
    critical         0
    default       5210  <- backed up
    overflow        12

  outbox            0 task(s) waiting

  issues
    - queue 'default' backed up (5210)
```

```bash
celery-adaptive -A proj.celery:app health --json --inspect   # exit code 0/1/2
celery-adaptive -A proj.celery:app flush-batches --dry-run
celery-adaptive -A proj.celery:app replay-outbox --limit 1000
```

## Metrics

With `celery-adaptive[metrics]` installed, these Prometheus series are exported (no-ops
otherwise):

| Metric | Labels |
|---|---|
| `celery_adaptive_tasks_published_total` | `task_name`, `queue` |
| `celery_adaptive_fallbacks_total` | `task_name`, `strategy` |
| `celery_adaptive_dedup_hits_total` | `task_name` |
| `celery_adaptive_circuit_state` (0 closed, 1 half-open, 2 open) | `broker` |
| `celery_adaptive_outbox_replayed_total` | |
| `celery_adaptive_routing_decisions_total` | `action`, `queue` |
| `celery_adaptive_batch_size` (histogram), `celery_adaptive_batch_flushes_total` | `task_name`, `trigger` |
| `celery_adaptive_broker_up`, `celery_adaptive_queue_depth` | `queue` |
| `celery_adaptive_redis_latency_ms`, `celery_adaptive_worker_active_tasks` | `worker` |

A Grafana dashboard ships with the package at `celery_adaptive/metrics/dashboard.json`,
and a test ensures it only queries metrics that exist.

## Configuration reference

| Setting | Default | Description |
|---|---|---|
| `REDIS_URL` | `None` | Redis for dedup, batching and the outbox (or pass `redis_client=`) |
| `FALLBACK` | `"sync"` | `"sync"`, `"outbox"` or `"raise"` when the broker is unreachable |
| `BROKER_TIMEOUT` | `1.0` | Seconds; used by `setup(app=...)` and the lazy Redis client |
| `CIRCUIT_BREAKER_THRESHOLD` | `3` | Consecutive publish failures before the circuit opens |
| `CIRCUIT_BREAKER_RESET` | `30.0` | Seconds before a probe publish is allowed |
| `QUEUE_MAP` | `high`→`high_priority`, `default`→`default`, `low`→`low_priority` | Priority name → queue |
| `DEDUPLICATION_TTL` | `300` | Safety expiry (s) for dedup keys of tasks that never finish |
| `DEDUP_KEY_PREFIX` | `"cadp:"` | Redis key prefix for dedup keys |
| `DISABLE_DEDUPLICATION` | `False` | Global kill switch |
| `OUTBOX_KEY` | `"cadp:outbox"` | Redis list holding outbox entries |
| `OUTBOX_AUTO_REPLAY` | `True` | Replay in the background when the circuit closes |
| `FORCE_SYNC` | `False` | Run every adaptive task inline (tests, local dev) |
| `LOG_DECISIONS` | `False` | Log every health routing decision at INFO |
| `HEALTH` | `{}` | See below |

| `HEALTH` key | Default | Description |
|---|---|---|
| `ENABLED` | `False` | Start the background monitor and use it for routing |
| `INTERVAL` / `STALE_AFTER` | `5.0` / `30.0` | Check interval; ignore older snapshots |
| `WATCHED_QUEUES` | `None` | Defaults to `QUEUE_MAP` queues, the default queue and `REROUTE` queues |
| `QUEUE_DEPTH_WARN` / `QUEUE_DEPTH_CRITICAL` | `500` / `2000` | Degraded status / reroute threshold |
| `REROUTE` | `{}` | `{"source": "overflow"}` |
| `BROKER` / `REDIS` | `True` / `True` | Built-in backends |
| `REDIS_LATENCY_WARN_MS` / `REDIS_MEMORY_WARN_PCT` | `50.0` / `80.0` | Redis degraded thresholds |
| `CELERY_INSPECT` / `INSPECT_TIMEOUT` | `False` / `1.0` | Worker saturation via broadcast |
| `RABBITMQ_URL`, `RABBITMQ_USER`, `RABBITMQ_PASSWORD`, `RABBITMQ_VHOST` | `None`, `guest`, `guest`, `/` | Management API backend |

Unknown keys and wrong types raise `ConfigurationError` at startup.

## Production notes

- **`"sync"` runs tasks in your web process.** That is great for short tasks and dangerous
  for long ones. Prefer `"outbox"` for anything slow.
- **The outbox and dedup locks live in Redis.** If Redis is also your broker, an outage takes
  both down. Dedup fails open (the task runs); the outbox then raises.
- **Health monitoring starts one daemon thread per process** when `HEALTH.ENABLED` is set.
  Each check costs one broker connection plus a passive declare per watched queue.
- **Subclasses overriding `after_return`** must call `super()`, which releases dedup keys.
- **Prometheus in multi-process servers** (gunicorn, uWSGI) needs prometheus_client's
  multiprocess mode.

## Development

```bash
pip install -e ".[dev]"
pytest                                   # unit + end-to-end tests with an in-process worker
ruff check . && ruff format --check .

docker compose up -d                     # Redis + RabbitMQ for the integration tests
export CELERY_ADAPTIVE_TEST_REDIS_URL=redis://localhost:6379/15
export CELERY_ADAPTIVE_TEST_AMQP_URL=amqp://guest:guest@localhost:5672//
export CELERY_ADAPTIVE_TEST_RABBITMQ_API=http://localhost:15672
pytest -m integration
```

CI runs the suite on Python 3.11–3.13, plus a lowest-supported-versions job, against real
Redis and RabbitMQ.

## License

MIT
