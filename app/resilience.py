"""Per-process circuit breaker and the door that stops a new call.

Both live in memory. A second Uvicorn worker or a second container has
its own breaker and its own admission count. The shared limit is still
the Redis token bucket in front of A.

429 is the bucket's job. This breaker opens on timeouts, connect
failures, and HTTP 5xx. Continuous 429s open it only as a backstop, so
a wrong bucket cannot stampede A. A 400 is the provider answering and
does not count as a failure.
"""

import logging
import time
from collections.abc import Callable

from app.metrics import record_admission_rejected, set_admission, set_circuit
from app.providers import ProviderCallError

logger = logging.getLogger("gateway")

_FAILURES = frozenset(
    {"upstream_timeout", "upstream_unavailable", "upstream_invalid_response"}
)


def classify(status_code: int | None, error: str | None) -> str:
    """Return ``failure``, ``rate_limit``, ``success``, or ``ignore``.

    This is the only place that decides what a breaker counts. Timeouts,
    connect failures, and HTTP 5xx are failures. HTTP 429 is the
    rate-limit backstop, not a 5xx failure. Any other response,
    including 400, means the provider answered. A full bulkhead, an
    open circuit, and a Redis outage are ignored so a local rejection
    cannot open the breaker.
    """
    if error in _FAILURES:
        return "failure"
    if error is not None:
        return "ignore"
    if status_code is None:
        return "ignore"
    if status_code == 429:
        return "rate_limit"
    if 500 <= status_code <= 599:
        return "failure"
    return "success"


class Breaker:
    """Closed, then open, then one half-open probe.

    Five failures in a row open the circuit. One success clears the
    streak. While open, ``admits`` stays false until ``open_seconds``
    have passed. The next acquired call is the probe: success closes,
    failure or 429 opens again. One probe per process, not a burst.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        rate_limit_threshold: int = 5,
        open_seconds: float = 20.0,
        now: Callable[[], float] | None = None,
    ) -> None:
        self.name = name
        self.state = "closed"
        self.failures = 0
        self.rate_limits = 0
        self._failure_threshold = failure_threshold
        self._rate_limit_threshold = rate_limit_threshold
        self._open_seconds = open_seconds
        self._now = now or time.monotonic
        self._opened_at = 0.0
        self._probing = False

    def admits(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "half_open":
            return not self._probing
        return self._now() - self._opened_at >= self._open_seconds

    def acquire(self) -> bool:
        """Take the right to call. False means the router must skip."""
        if not self.admits():
            return False
        if self.state == "open":
            self._become("half_open")
        if self.state == "half_open":
            self._probing = True
        return True

    def observe(self, status_code: int | None, error: str | None) -> None:
        kind = classify(status_code, error)
        if self.state == "half_open":
            self._probing = False
            if kind == "success":
                self.failures = 0
                self.rate_limits = 0
                self._become("closed")
            elif kind in {"failure", "rate_limit"}:
                self._trip()
            return
        if kind == "success":
            self.failures = 0
            self.rate_limits = 0
            return
        if kind == "failure":
            self.failures += 1
            if self.failures >= self._failure_threshold:
                self._trip()
            return
        if kind == "rate_limit":
            self.rate_limits += 1
            if self.rate_limits >= self._rate_limit_threshold:
                self._trip()

    def _trip(self) -> None:
        self.failures = 0
        self.rate_limits = 0
        self._probing = False
        self._opened_at = self._now()
        self._become("open")

    def _become(self, new: str) -> None:
        if self.state == new:
            return
        logger.warning("circuit provider=%s from=%s to=%s", self.name, self.state, new)
        self.state = new
        set_circuit(self.name, new)


class Admission:
    """How many calls this process has already accepted and not finished.

    Little's Law: concurrency is about RPS times seconds in flight. When
    this count is at the cap, the call returns immediately. It does not
    wait for a slot, and it does not call a provider.
    """

    def __init__(self, max_inflight: int, retry_after: int = 1) -> None:
        self.max_inflight = max_inflight
        self.inflight = 0
        self._retry_after = retry_after

    def enter(self) -> None:
        if self.inflight >= self.max_inflight:
            record_admission_rejected()
            raise ProviderCallError("admission_rejected", 429, retry_after=self._retry_after)
        self.inflight += 1
        set_admission(self.inflight)

    def leave(self) -> None:
        self.inflight -= 1
        set_admission(self.inflight)
