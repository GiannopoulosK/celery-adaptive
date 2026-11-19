"""
Circuit breaker for broker publishing.

When the broker is unreachable, every publish attempt blocks for the
connection timeout before failing. The breaker remembers recent failures
so callers can skip straight to their fallback instead of paying that
timeout on every task:

* **closed**: publishes go through. ``failure_threshold`` consecutive
  failures open the circuit.
* **open**: publishes are skipped until ``reset_timeout`` elapses.
* **half-open**: a single probe publish is let through. Success closes the
  circuit, failure re-opens it.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from enum import StrEnum

logger = logging.getLogger(__name__)


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        name: str = "broker",
        failure_threshold: int = 3,
        reset_timeout: float = 30.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self.name = name
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self._clock = clock
        self._lock = threading.Lock()
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_half_open()
            return self._state

    def _maybe_half_open(self) -> None:
        if (
            self._state is CircuitState.OPEN
            and self._clock() - self._opened_at >= self.reset_timeout
        ):
            self._state = CircuitState.HALF_OPEN
            self._probe_in_flight = False
            self._report()

    def allow(self) -> bool:
        """Return True if a publish attempt should be made."""
        with self._lock:
            self._maybe_half_open()
            if self._state is CircuitState.CLOSED:
                return True
            if self._state is CircuitState.HALF_OPEN and not self._probe_in_flight:
                self._probe_in_flight = True
                return True
            return False

    def record_success(self) -> bool:
        """Record a successful publish. Returns True if this closed the circuit."""
        with self._lock:
            recovered = self._state is not CircuitState.CLOSED
            if recovered:
                logger.info("Circuit '%s' closed: broker reachable again.", self.name)
            self._state = CircuitState.CLOSED
            self._failures = 0
            self._probe_in_flight = False
        if recovered:
            self._report()
        return recovered

    def release_probe(self) -> None:
        """Give back a half-open probe slot without recording an outcome.

        Used when a publish attempt failed for reasons unrelated to broker
        connectivity (e.g. a serialization error), so the next call can probe.
        """
        with self._lock:
            self._probe_in_flight = False

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            if self._state is CircuitState.HALF_OPEN or (
                self._failures >= self.failure_threshold
            ):
                self._open()

    def trip(self) -> None:
        """Force the circuit open, e.g. when a health check sees the broker down."""
        with self._lock:
            self._open()

    def _open(self) -> None:
        if self._state is not CircuitState.OPEN:
            logger.warning(
                "Circuit '%s' opened after %d failure(s); skipping publishes "
                "for %.0fs.",
                self.name,
                self._failures,
                self.reset_timeout,
            )
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
        self._probe_in_flight = False
        self._report()

    def _report(self) -> None:
        from ..metrics.exporters import record_circuit_state

        record_circuit_state(self.name, self._state.value)

    def __repr__(self) -> str:
        return f"<CircuitBreaker {self.name!r} state={self.state.value}>"


_breakers: dict[str, CircuitBreaker] = {}
_registry_lock = threading.Lock()


def get_breaker(key: str) -> CircuitBreaker:
    """Return the process-wide breaker for ``key`` (normally a broker URL)."""
    breaker = _breakers.get(key)
    if breaker is not None:
        return breaker
    from ..conf import conf

    with _registry_lock:
        breaker = _breakers.get(key)
        if breaker is None:
            breaker = CircuitBreaker(
                name=key,
                failure_threshold=conf.CIRCUIT_BREAKER_THRESHOLD,
                reset_timeout=conf.CIRCUIT_BREAKER_RESET,
            )
            _breakers[key] = breaker
    return breaker


def all_breakers() -> dict[str, CircuitBreaker]:
    return dict(_breakers)


def reset_breakers() -> None:
    """Forget all breakers. Called by setup() and in tests."""
    with _registry_lock:
        _breakers.clear()
