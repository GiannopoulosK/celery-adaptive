from celery import Celery

import celery_adaptive
from celery_adaptive.core.broker import tune_broker_timeouts


def test_setup_with_app_bounds_redis_timeouts():
    app = Celery("t", broker="redis://example:6379/0")
    celery_adaptive.setup(config={"BROKER_TIMEOUT": 0.5}, app=app)

    assert app.conf.broker_connection_timeout == 0.5
    assert app.conf.broker_transport_options["socket_connect_timeout"] == 0.5
    # Stays above the 1s BRPOP polling interval used by idle workers.
    assert app.conf.broker_transport_options["socket_timeout"] == 5.0


def test_amqp_only_gets_connection_timeout():
    app = Celery("t", broker="amqp://guest@example//")
    tune_broker_timeouts(app, 1.5)
    assert app.conf.broker_connection_timeout == 1.5
    assert "socket_connect_timeout" not in app.conf.broker_transport_options


def test_user_values_are_respected():
    app = Celery("t", broker="rediss://example:6379/0")
    app.conf.broker_connection_timeout = 10
    app.conf.broker_transport_options = {"socket_timeout": 30, "visibility_timeout": 9}
    tune_broker_timeouts(app, 1.0)

    assert app.conf.broker_connection_timeout == 10
    assert app.conf.broker_transport_options == {
        "socket_timeout": 30,
        "visibility_timeout": 9,
        "socket_connect_timeout": 1.0,
    }
