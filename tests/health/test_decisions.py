from celery_adaptive.health.backends.broker import BrokerHealthResult
from celery_adaptive.health.checker import HealthSnapshot
from celery_adaptive.health.decisions import Action, DecisionEngine


def snapshot(**depths):
    return HealthSnapshot(broker=BrokerHealthResult(healthy=True, queue_depths=depths))


engine = DecisionEngine(depth_critical=100, reroute={"default": "overflow"})


def test_enqueue_when_no_reroute_configured():
    assert engine.decide(snapshot(other=10_000), "other").action is Action.ENQUEUE


def test_enqueue_without_health_data():
    decision = engine.decide(None, "default")
    assert decision.action is Action.ENQUEUE
    assert "no recent health data" in decision.reason


def test_enqueue_below_critical_depth():
    assert engine.decide(snapshot(default=99), "default").action is Action.ENQUEUE


def test_reroute_at_critical_depth():
    decision = engine.decide(snapshot(default=100, overflow=3), "default")
    assert decision.action is Action.REROUTE
    assert decision.target_queue == "overflow"
    assert decision.should_reroute


def test_reroute_when_overflow_depth_unknown():
    decision = engine.decide(snapshot(default=500), "default")
    assert decision.target_queue == "overflow"


def test_no_reroute_when_overflow_is_no_better():
    decision = engine.decide(snapshot(default=500, overflow=800), "default")
    assert decision.action is Action.ENQUEUE
    assert "no better" in decision.reason


def test_enqueue_when_depth_unknown():
    decision = engine.decide(snapshot(), "default")
    assert decision.action is Action.ENQUEUE


def test_unhealthy_broker_means_unknown_depth():
    snap = HealthSnapshot(broker=BrokerHealthResult(healthy=False, error="down"))
    assert engine.decide(snap, "default").action is Action.ENQUEUE
