# Celery Adaptive 🧠⚡

**Celery Adaptive** is a health-aware wrapper for Celery that makes your background tasks smarter. It monitors your infrastructure (Redis, RabbitMQ) and automatically adjusts task routing based on system pressure.

## Features
- 🚀 **Adaptive Routing:** Automatically switch to `Sync` execution if the broker is down.
- 📦 **Smart Batching:** Buffer tasks in Redis and flush based on size or time thresholds.
- 🩺 **Infrastructure Health:** Built-in health checks for Redis latency and RabbitMQ queue depth.
- 📊 **Prometheus Ready:** Export metrics for routing decisions and system health.
- 🔄 **Idempotency:** Prevent duplicate task execution out of the box.

## Installation
```bash
pip install celery-adaptive[all]