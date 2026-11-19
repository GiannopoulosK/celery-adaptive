# orders/tasks.py
from celery_adaptive import batch_task
from celery_adaptive.contrib.django import adaptive_task


@adaptive_task(priority="high", deduplicate=True, autoretry_for=(ConnectionError,))
def send_receipt(order_id: int) -> None: ...


@batch_task(max_batch_size=500, max_wait_ms=2000, queue="bulk")
def record_events(payloads: list[dict]) -> None:
    # Event.objects.bulk_create(Event(**p) for p in payloads)
    ...


# orders/views.py
#
# with transaction.atomic():
#     order = Order.objects.create(...)
#     send_receipt.delay_on_commit(order.id)   # published only after COMMIT
#
# record_events.push(user_id=request.user.id, kind="checkout")
