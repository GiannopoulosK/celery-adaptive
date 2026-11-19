"""Regression tests for bugs found in the initial release candidate."""

from celery_adaptive.core.idempotency import acquire_dedup_lock


def test_dedup_lock_allows_first_call_and_blocks_duplicate(configured):
    redis = configured
    assert acquire_dedup_lock(redis, "t", (1, 2), {}) is True
    assert acquire_dedup_lock(redis, "t", (1, 2), {}) is False
    assert acquire_dedup_lock(redis, "t", (3, 4), {}) is True
