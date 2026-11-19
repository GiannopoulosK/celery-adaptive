# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

First public release.

### Added

- `AdaptiveTask`, a `celery.Task` subclass, and the `adaptive_task` decorator:
  priority-to-queue mapping, per-call `adaptive={...}` overrides, full canvas support.
- Per-broker circuit breaker with bounded publish retries.
- `setup(app=...)` / `tune_broker_timeouts()` to bound broker connection time.
- Fallback strategies `sync`, `outbox` and `raise`; crash-safe Redis outbox replayed
  with the original task ids, automatically on broker recovery.
- In-flight deduplication that returns the original `AsyncResult`, with custom
  `dedup_key` functions and release on task completion.
- `batch_task` with atomic, server-clocked Redis buffering, `push`/`apush`/`flush`, and
  the `flush_batches` beat task.
- Background, fork-safe `HealthMonitor` with broker-native queue depth, Redis,
  worker-saturation and RabbitMQ management backends, and overflow rerouting.
- Async helpers `adelay`, `aapply_async`, `apush`.
- Django integration: `delay_on_commit`, health/metrics views and URLconf,
  `queue_health`, `flush_batches` and `replay_outbox` commands, Redis client reuse from
  django-redis or Django's `RedisCache`.
- FastAPI integration: `health_router`, `lifespan`, `init_app`.
- `celery-adaptive` CLI: `health`, `flush-batches`, `replay-outbox`.
- Prometheus metrics and a matching Grafana dashboard.

### Fixed (relative to the pre-release prototype)

- Deduplication discarded the first call of every task.
- A broken import in the metrics package made every `.delay()` run inline.
- Every dispatch error, including bugs, was treated as a broker outage.
- The Django app crashed at startup; the `HEALTH` settings were rejected.
- Health checks blocked requests for up to 6 seconds and forced inline execution when
  workers, not the broker, were unavailable.
- Batching compared per-process monotonic clocks across hosts and lost payloads when
  publishing failed; the stale-batch flush task was never registered.
- The Grafana dashboard queried metrics that did not exist; it was not shipped in wheels.
