import pytest

import celery_adaptive
from celery_adaptive.conf import DEFAULTS, conf
from celery_adaptive.exceptions import ConfigurationError
from celery_adaptive.redis_client import get_redis_client


def test_defaults_apply_without_setup():
    assert not conf.is_configured
    assert DEFAULTS["QUEUE_MAP"] == conf.QUEUE_MAP
    assert conf.DEDUPLICATION_TTL == 300


def test_setup_overrides_and_deep_merges_queue_map():
    celery_adaptive.setup(config={"QUEUE_MAP": {"low": "bulk"}, "FORCE_SYNC": True})
    assert conf.FORCE_SYNC is True
    assert conf.QUEUE_MAP == {
        "high": "high_priority",
        "default": "default",
        "low": "bulk",
    }


def test_unknown_key_rejected():
    with pytest.raises(ConfigurationError, match="unknown keys"):
        celery_adaptive.setup(config={"NOPE": 1})


@pytest.mark.parametrize(
    "key,value",
    [
        ("DEDUPLICATION_TTL", "300"),
        ("DEDUPLICATION_TTL", True),  # bool must not pass as int
        ("FORCE_SYNC", 1),
        ("QUEUE_MAP", []),
        ("REDIS_URL", 5),
    ],
)
def test_type_validation(key, value):
    with pytest.raises(ConfigurationError, match=key):
        celery_adaptive.setup(config={key: value})


def test_non_positive_values_rejected():
    with pytest.raises(ConfigurationError, match="BROKER_TIMEOUT"):
        celery_adaptive.setup(config={"BROKER_TIMEOUT": 0})


def test_unknown_attribute_raises_attribute_error():
    with pytest.raises(AttributeError):
        conf.DOES_NOT_EXIST  # noqa: B018
    assert getattr(conf, "DOES_NOT_EXIST", "fallback") == "fallback"


def test_config_is_read_only():
    celery_adaptive.setup(config={})
    with pytest.raises(TypeError):
        conf._active()["FORCE_SYNC"] = True


def test_redis_client_created_lazily_from_url():
    celery_adaptive.setup(config={"REDIS_URL": "redis://localhost:6399/3"})
    client = get_redis_client()
    assert client is not None
    assert client.connection_pool.connection_kwargs["db"] == 3
    assert get_redis_client() is client


def test_no_redis_without_client_or_url():
    assert get_redis_client() is None
