"""Circuit breaker, bulkhead, and the admission door. No network."""

import asyncio
import time
from contextlib import suppress

import pytest

from app.providers import ProviderCallError
from app.resilience import Admission, Breaker
from tests.test_routing import Scripted, body_of, make_router


def _breaker(threshold: int = 1, rate_limit: int = 50, open_seconds: float = 20, now=None) -> Breaker:
    return Breaker(
        "a",
        failure_threshold=threshold,
        rate_limit_threshold=rate_limit,
        open_seconds=open_seconds,
        now=now,
    )


def test_open_circuit_stays_shut_until_the_cooldown() -> None:
    clock = [0.0]
    breaker = _breaker(now=lambda: clock[0])

    breaker.observe(500, None)

    assert breaker.state == "open"
    assert breaker.admits() is False
    clock[0] = 19
    assert breaker.admits() is False
    clock[0] = 20
    assert breaker.admits() is True


async def test_open_circuit_is_not_chosen() -> None:
    breaker = _breaker()
    a, b, c = Scripted("a", [500, 200]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c, breakers=(breaker, None, None))

    failed = await router.execute({}, "t1")
    followed = await router.execute({}, "t2")

    assert failed.status_code == 500
    assert breaker.state == "open"
    assert body_of(followed)["provider"] == "b"
    assert a.calls == ["t1"]


async def test_client_error_does_not_open_the_circuit() -> None:
    breaker = _breaker()
    a, b, c = Scripted("a", [400, 200]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c, breakers=(breaker, None, None))

    rejected = await router.execute({}, "t1")
    again = await router.execute({}, "t2")

    assert rejected.status_code == 400
    assert breaker.state == "closed"
    assert body_of(again)["provider"] == "a"
    assert b.calls == []


async def test_continuous_429_opens_the_backstop() -> None:
    breaker = _breaker(threshold=50, rate_limit=2)
    a = Scripted("a", [429, 429, 200])
    b, c = Scripted("b", [200, 200, 200]), Scripted("c", [200])
    router, _ = make_router(a, b, c, breakers=(breaker, None, None))

    await router.execute({}, "t1")
    assert breaker.state == "closed"
    assert breaker.rate_limits == 1

    await router.execute({}, "t2")
    assert breaker.state == "open"

    followed = await router.execute({}, "t3")

    assert body_of(followed)["provider"] == "b"
    assert a.calls == ["t1", "t2"]


async def test_half_open_success_closes_the_circuit() -> None:
    clock = [0.0]
    breaker = _breaker(now=lambda: clock[0])
    a, b, c = Scripted("a", [500, 200]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c, breakers=(breaker, None, None))

    await router.execute({}, "t1")
    await router.execute({}, "t2")
    clock[0] = 20
    probed = await router.execute({}, "t3")

    assert body_of(probed)["provider"] == "a"
    assert breaker.state == "closed"
    assert a.calls == ["t1", "t3"]


async def test_half_open_failure_opens_the_circuit_again() -> None:
    clock = [0.0]
    breaker = _breaker(now=lambda: clock[0])
    a, b, c = Scripted("a", [500, 500]), Scripted("b", [200]), Scripted("c", [200])
    router, _ = make_router(a, b, c, breakers=(breaker, None, None))

    await router.execute({}, "t1")
    clock[0] = 20
    probed = await router.execute({}, "t2")

    assert probed.status_code == 500
    assert breaker.state == "open"
    assert a.calls == ["t1", "t2"]


async def test_connect_error_releases_the_slot_and_counts_a_failure() -> None:
    a, b, c = Scripted("a", ["connect"]), Scripted("b", [200]), Scripted("c", [200])
    router, upstreams = make_router(a, b, c)

    response = await router.execute({}, "t1")

    assert body_of(response)["provider"] == "b"
    assert upstreams[0].inflight == 0
    assert a.active == 0
    assert upstreams[0].breaker.failures == 1
    assert upstreams[0].breaker.state == "closed"


async def test_full_bulkhead_rejects_the_overlap_without_waiting() -> None:
    a, b, c = Scripted("a", ["hang"]), Scripted("b", ["hang"]), Scripted("c", ["hang"])
    router, upstreams = make_router(a, b, c, caps=(1, 0, 0))
    first = asyncio.create_task(router.execute({}, "hold"))
    try:
        for _ in range(50):
            if a.active == 1:
                break
            await asyncio.sleep(0.01)
        assert a.active == 1
        started = time.perf_counter()
        with pytest.raises(ProviderCallError) as caught:
            await router.execute({}, "second")
        elapsed = time.perf_counter() - started
    finally:
        first.cancel()
        with suppress(asyncio.CancelledError):
            await first

    assert caught.value.error == "upstream_unavailable"
    assert caught.value.retry_after == 1
    assert elapsed < 0.2
    assert b.calls == []
    assert c.calls == []
    assert upstreams[0].inflight == 0


async def test_admission_cap_rejects_the_overlap_without_waiting() -> None:
    a, b, c = Scripted("a", ["hang"]), Scripted("b", [200]), Scripted("c", [200])
    admission = Admission(1)
    router, _ = make_router(a, b, c, admission=admission)
    first = asyncio.create_task(router.execute({}, "hold"))
    try:
        for _ in range(50):
            if a.active == 1:
                break
            await asyncio.sleep(0.01)
        assert a.active == 1
        started = time.perf_counter()
        with pytest.raises(ProviderCallError) as caught:
            await router.execute({}, "second")
        elapsed = time.perf_counter() - started
    finally:
        first.cancel()
        with suppress(asyncio.CancelledError):
            await first

    assert caught.value.error == "admission_rejected"
    assert caught.value.status_code == 429
    assert caught.value.retry_after == 1
    assert elapsed < 0.2
    assert a.calls == ["hold"]
    assert b.calls == []
    assert admission.inflight == 0
