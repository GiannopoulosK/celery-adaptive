import celery_adaptive


def test_package_imports():
    assert celery_adaptive.adaptive_task is not None


def test_setup_with_defaults(configured):
    assert celery_adaptive.settings.is_configured
