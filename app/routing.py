"""Choose a provider and allow one failover inside a deadline.

In-flight caps and circuit breakers live on this object. They are per
process: another worker or another container has its own. The shared
limit is the Redis token bucket, asked before A is called. A provider
is eligible when its circuit admits it, its bulkhead has a free slot,
and, for A, the bucket has a token.
"""

import asyncio
import logging
import time
from typing import Any, Protocol

import httpx
from fastapi.responses import JSONResponse

from app.metrics import record_provider, set_inflight
from app.providers import ProviderCallError
from app.resilience import Admission, Breaker

logger = logging.getLogger("gateway")

# A 500 is not in this set. It is the provider's answer and is returned.
# Timeouts and these statuses get one attempt on a different provider.
FAILOVER_STATUSES = frozenset({429, 502, 503, 504})
FAILOVER_ERRORS = frozenset(
    {"upstream_timeout", "upstream_unavailable", "upstream_saturated"}
)


class OutboundBudget(Protocol):
    """``take`` returns ``ok`` when A may be called, otherwise ``empty``."""

    async def take(self, provider: str) -> str: ...


class UpstreamCall(Protocol):
    name: str

    async def process(
        self,
        body: dict[str, Any],
        trace_id: str,
        timeout: httpx.Timeout | None = None,
    ) -> JSONResponse: ...


class Upstream:
    def __init__(
        self,
        client: UpstreamCall,
        max_inflight: int,
        timeout: httpx.Timeout,
        breaker: Breaker | None = None,
    ) -> None:
        self.client = client
        self.name = client.name
        self.max_inflight = max_inflight
        self.timeout = timeout
        self.breaker = breaker if breaker is not None else Breaker(client.name)
        self.inflight = 0

    @property
    def has_capacity(self) -> bool:
        return self.inflight < self.max_inflight

    def eligible(self, excluded: set[str]) -> bool:
        return self.name not in excluded and self.has_capacity and self.breaker.admits()

    async def call(self, body: dict[str, Any], trace_id: str, budget: float) -> JSONResponse:
        if not self.has_capacity:
            raise ProviderCallError("upstream_saturated", 503)
        if not self.breaker.acquire():
            raise ProviderCallError("circuit_open", 503)
        self.inflight += 1
        set_inflight(self.name, self.inflight)
        started = time.perf_counter()
        status_code: int | None = None
        error: str | None = None
        try:
            try:
                response = await asyncio.wait_for(
                    self.client.process(body, trace_id, timeout=_capped(self.timeout, budget)),
                    timeout=budget,
                )
            except TimeoutError as exc:
                record_provider(self.name, "timeout")
                logger.warning(
                    "provider_call trace_id=%s provider=%s status=timeout latency_ms=%.1f error=Deadline",
                    trace_id,
                    self.name,
                    (time.perf_counter() - started) * 1000,
                )
                error = "upstream_timeout"
                raise ProviderCallError("upstream_timeout", 504) from exc
            status_code = response.status_code
            return response
        except ProviderCallError as exc:
            if error is None:
                error = exc.error
            raise
        finally:
            self.inflight -= 1
            set_inflight(self.name, self.inflight)
            self.breaker.observe(status_code, error)


class Router:
    def __init__(
        self,
        upstreams: list[Upstream],
        deadline_seconds: float,
        min_attempt_seconds: float,
        budget: OutboundBudget | None = None,
        admission: Admission | None = None,
    ) -> None:
        by_name = {upstream.name: upstream for upstream in upstreams}
        self._a = by_name["a"]
        self._b = by_name["b"]
        self._c = by_name["c"]
        self._deadline = deadline_seconds
        self._min_attempt = min_attempt_seconds
        self._budget = budget
        self._admission = admission
        self._prefer_b = True

    def choose(self, excluded: set[str]) -> Upstream | None:
        """Prefer A. Otherwise alternate B and C so neither stays idle."""
        if self._a.eligible(excluded):
            return self._a
        order = (self._b, self._c) if self._prefer_b else (self._c, self._b)
        for candidate in order:
            if candidate.eligible(excluded):
                self._prefer_b = candidate is self._c
                return candidate
        return None

    async def execute(
        self,
        body: dict[str, Any],
        trace_id: str,
        trail: list[dict[str, Any]] | None = None,
    ) -> JSONResponse:
        if self._admission is not None:
            try:
                self._admission.enter()
            except ProviderCallError:
                logger.info(
                    "admission_rejected trace_id=%s inflight=%s",
                    trace_id,
                    self._admission.inflight,
                )
                raise
        try:
            return await self._attempt(body, trace_id, trail)
        finally:
            if self._admission is not None:
                self._admission.leave()

    async def _attempt(
        self,
        body: dict[str, Any],
        trace_id: str,
        trail: list[dict[str, Any]] | None = None,
    ) -> JSONResponse:
        deadline = time.monotonic() + self._deadline
        excluded: set[str] = set()
        last_response: JSONResponse | None = None
        last_error: ProviderCallError | None = None

        attempt = 0
        while attempt < 2:
            remaining = deadline - time.monotonic()
            if remaining < self._min_attempt:
                logger.warning(
                    "failover skipped trace_id=%s reason=deadline remaining_ms=%.1f",
                    trace_id,
                    remaining * 1000,
                )
                break
            upstream = self.choose(excluded)
            if upstream is None:
                logger.warning(
                    "failover skipped trace_id=%s reason=no_candidate excluded=%s",
                    trace_id,
                    ",".join(sorted(excluded)) or "-",
                )
                break
            if upstream.name == "a" and self._budget is not None:
                decision = await self._budget.take("a")
                if decision != "ok":
                    excluded.add("a")
                    logger.info("bucket_empty trace_id=%s provider=a", trace_id)
                    continue
            logger.info(
                "attempt trace_id=%s n=%s provider=%s remaining_ms=%.1f inflight=%s",
                trace_id,
                attempt + 1,
                upstream.name,
                remaining * 1000,
                upstream.inflight,
            )
            call_started = time.perf_counter()
            try:
                response = await upstream.call(body, trace_id, remaining)
            except ProviderCallError as exc:
                if exc.error == "circuit_open":
                    excluded.add(upstream.name)
                    logger.info("circuit_open trace_id=%s provider=%s", trace_id, upstream.name)
                    continue
                _remember(trail, upstream.name, None, exc.error, call_started)
                last_error = exc
                attempt += 1
                if attempt == 1 and exc.error in FAILOVER_ERRORS:
                    excluded.add(upstream.name)
                    logger.warning(
                        "failover trace_id=%s from=%s reason=%s",
                        trace_id,
                        upstream.name,
                        exc.error,
                    )
                    continue
                break
            _remember(trail, upstream.name, response.status_code, None, call_started)
            attempt += 1
            if attempt == 1 and response.status_code in FAILOVER_STATUSES:
                excluded.add(upstream.name)
                last_response = response
                logger.warning(
                    "failover trace_id=%s from=%s reason=%s",
                    trace_id,
                    upstream.name,
                    response.status_code,
                )
                continue
            return response

        if last_error is not None:
            raise last_error
        if last_response is not None:
            return last_response
        raise ProviderCallError("upstream_unavailable", 503, retry_after=1)


def _remember(
    trail: list[dict[str, Any]] | None,
    provider: str,
    status: int | None,
    error: str | None,
    started: float,
) -> None:
    if trail is None:
        return
    trail.append(
        {
            "provider": provider,
            "status": status,
            "latency_ms": round((time.perf_counter() - started) * 1000, 1),
            "error": error,
        }
    )


def _capped(timeout: httpx.Timeout, budget: float) -> httpx.Timeout:
    def cap(value: float | None) -> float:
        if value is None:
            return budget
        return min(float(value), budget)

    return httpx.Timeout(
        connect=cap(timeout.connect),
        read=cap(timeout.read),
        write=cap(timeout.write),
        pool=cap(timeout.pool),
    )
