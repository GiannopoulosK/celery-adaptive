# FastAPI example

Shows async dispatch, deduplication, batching, the outbox fallback and the
health endpoint together. See the docstring at the top of `main.py` for how
to run it, then open:

- `POST /orders/1/receipt` twice quickly: both return the same task id
- `POST /pages/home/view` a few times: buffered, then published as one batch
- `GET /celery-adaptive/health`: broker, Redis and queue status
- `GET /celery-adaptive/metrics`: Prometheus metrics
