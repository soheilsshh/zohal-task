"""Routing rules with in-memory fakes. No network."""

import asyncio
import json

import httpx
import pytest
from fastapi.responses import JSONResponse

from app.providers import ProviderCallError
from app.routing import Router, Upstream


class Scripted:
    def __init__(self, name: str, outcomes: list[object]) -> None:
        self.name = name
        self._outcomes = list(outcomes)
        self.calls: list[str] = []
        self.active = 0

    async def process(self, body: dict, trace_id: str, timeout: httpx.Timeout | None = None) -> JSONResponse:
        self.active += 1
        self.calls.append(trace_id)
        try:
            outcome = self._outcomes.pop(0)
            if outcome == "hang":
                await asyncio.sleep(5)
            if outcome == "timeout":
                raise ProviderCallError("upstream_timeout", 504)
            if outcome == "connect":
                raise ProviderCallError("upstream_unavailable", 502)
            if isinstance(outcome, int):
                return JSONResponse(status_code=outcome, content={"provider": self.name})
            raise AssertionError(f"unexpected outcome {outcome!r}")
        finally:
            self.active -= 1


def make_router(
    a: Scripted,
    b: Scripted,
    c: Scripted,
    deadline: float = 3.0,
    caps: tuple[int, int, int] = (100, 100, 100),
    budget: object | None = None,
    breakers: tuple[object | None, object | None, object | None] = (None, None, None),
    admission: object | None = None,
) -> tuple[Router, list[Upstream]]:
    timeout = httpx.Timeout(1.0)
    upstreams = [
        Upstream(client, cap, timeout, breaker=breaker)
        for client, cap, breaker in zip((a, b, c), caps, breakers, strict=True)
    ]
    return Router(upstreams, deadline, min_attempt_seconds=0.05, budget=budget, admission=admission), upstreams


def body_of(response: JSONResponse) -> dict:
    return json.loads(response.body)


async def test_prefers_a_when_a_succeeds() -> None:
    a, b, c = Scripted("a", [200]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c)

    response = await router.execute({}, "trace-1")

    assert response.status_code == 200
    assert body_of(response)["provider"] == "a"
    assert b.calls == []
    assert c.calls == []


async def test_timeout_on_a_fails_over_to_b_once() -> None:
    a, b, c = Scripted("a", ["timeout"]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c)

    response = await router.execute({}, "trace-2")

    assert body_of(response)["provider"] == "b"
    assert a.calls == ["trace-2"]
    assert b.calls == ["trace-2"]
    assert c.calls == []
    assert a.active == 0
    assert b.active == 0


async def test_429_from_a_does_not_call_a_again() -> None:
    a = Scripted("a", [429, 200])
    b, c = Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c)

    response = await router.execute({}, "trace-3")

    assert body_of(response)["provider"] == "b"
    assert a.calls == ["trace-3"]


async def test_second_failure_does_not_call_the_third_provider() -> None:
    a, b, c = Scripted("a", [429]), Scripted("b", [503]), Scripted("c", [200])
    router, _ = make_router(a, b, c)

    response = await router.execute({}, "trace-4")

    assert response.status_code == 503
    assert body_of(response)["provider"] == "b"
    assert c.calls == []


async def test_500_is_returned_without_failover() -> None:
    a, b, c = Scripted("a", [500]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c)

    response = await router.execute({}, "trace-5")

    assert response.status_code == 500
    assert b.calls == []
    assert c.calls == []


async def test_hang_releases_the_inflight_slot_and_does_not_start_b() -> None:
    a, b, c = Scripted("a", ["hang"]), Scripted("b", [200]), Scripted("c", [200])
    router, upstreams = make_router(a, b, c, deadline=0.2, caps=(1, 1, 1))

    with pytest.raises(ProviderCallError) as caught:
        await router.execute({}, "trace-6")

    assert caught.value.error == "upstream_timeout"
    assert upstreams[0].inflight == 0
    assert a.active == 0
    assert b.calls == []


async def test_choose_skips_excluded_a_and_alternates_the_others() -> None:
    a, b, c = Scripted("a", []), Scripted("b", []), Scripted("c", [])
    router, _ = make_router(a, b, c)

    assert router.choose(set()).name == "a"
    assert router.choose({"a"}).name == "b"
    assert router.choose({"a"}).name == "c"


async def test_b_starts_only_after_a_has_finished() -> None:
    a = Scripted("a", ["timeout"])
    b = Scripted("b", [200])
    c = Scripted("c", [200])
    seen: list[int] = []
    original = b.process

    async def process(body, trace_id, timeout=None):
        seen.append(a.active)
        return await original(body, trace_id, timeout)

    b.process = process  # type: ignore[method-assign]
    router, _ = make_router(a, b, c)

    await router.execute({}, "trace-7")

    assert seen == [0]


async def test_failover_records_both_attempts() -> None:
    a, b, c = Scripted("a", [429]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c)
    trail: list[dict] = []

    response = await router.execute({}, "trace-audit", trail)

    assert body_of(response)["provider"] == "b"
    assert [item["provider"] for item in trail] == ["a", "b"]
    assert trail[0]["status"] == 429 and trail[0]["error"] is None
    assert trail[1]["status"] == 200 and trail[1]["error"] is None
    assert trail[0]["latency_ms"] >= 0


class _Budget:
    def __init__(self, decision: str) -> None:
        self._decision = decision

    async def take(self, provider: str) -> str:
        if self._decision == "down":
            raise ProviderCallError("rate_limit_unavailable", 503)
        return self._decision


async def test_empty_bucket_skips_a_and_calls_b_once() -> None:
    a, b, c = Scripted("a", [200]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c, budget=_Budget("empty"))

    response = await router.execute({}, "trace-bucket")

    assert body_of(response)["provider"] == "b"
    assert a.calls == []
    assert b.calls == ["trace-bucket"]
    assert c.calls == []


async def test_redis_down_rejects_before_any_provider() -> None:
    a, b, c = Scripted("a", [200]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c, budget=_Budget("down"))

    with pytest.raises(ProviderCallError) as caught:
        await router.execute({}, "trace-redis")

    assert caught.value.error == "rate_limit_unavailable"
    assert a.calls == []
    assert b.calls == []
    assert c.calls == []
