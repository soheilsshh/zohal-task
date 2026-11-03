"""Choose a provider and allow one failover inside a deadline.

In-flight caps live on this object. They are per process: another
Uvicorn worker or another container has its own counters. The shared
limit is the Redis token bucket, and it is asked before A is called.
"""

import asyncio
import logging
import time
from typing import Any, Protocol

import httpx
from fastapi.responses import JSONResponse

from app.providers import ProviderCallError

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
    ) -> None:
        self.client = client
        self.name = client.name
        self.max_inflight = max_inflight
        self.timeout = timeout
        self.inflight = 0

    @property
    def has_capacity(self) -> bool:
        return self.inflight < self.max_inflight

    async def call(self, body: dict[str, Any], trace_id: str, budget: float) -> JSONResponse:
        if not self.has_capacity:
            raise ProviderCallError("upstream_saturated", 503)
        self.inflight += 1
        started = time.perf_counter()
        try:
            try:
                return await asyncio.wait_for(
                    self.client.process(body, trace_id, timeout=_capped(self.timeout, budget)),
                    timeout=budget,
                )
            except TimeoutError as exc:
                logger.warning(
                    "provider_call trace_id=%s provider=%s status=timeout latency_ms=%.1f error=Deadline",
                    trace_id,
                    self.name,
                    (time.perf_counter() - started) * 1000,
                )
                raise ProviderCallError("upstream_timeout", 504) from exc
        finally:
            self.inflight -= 1


class Router:
    def __init__(
        self,
        upstreams: list[Upstream],
        deadline_seconds: float,
        min_attempt_seconds: float,
        budget: OutboundBudget | None = None,
    ) -> None:
        by_name = {upstream.name: upstream for upstream in upstreams}
        self._a = by_name["a"]
        self._b = by_name["b"]
        self._c = by_name["c"]
        self._deadline = deadline_seconds
        self._min_attempt = min_attempt_seconds
        self._budget = budget
        self._prefer_b = True

    def choose(self, excluded: set[str]) -> Upstream | None:
        """Prefer A. Otherwise alternate B and C so neither stays idle."""
        if "a" not in excluded and self._a.has_capacity:
            return self._a
        order = (self._b, self._c) if self._prefer_b else (self._c, self._b)
        for candidate in order:
            if candidate.name not in excluded and candidate.has_capacity:
                self._prefer_b = candidate is self._c
                return candidate
        return None

    async def execute(self, body: dict[str, Any], trace_id: str) -> JSONResponse:
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
            try:
                response = await upstream.call(body, trace_id, remaining)
            except ProviderCallError as exc:
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
        raise ProviderCallError("upstream_unavailable", 503)


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
