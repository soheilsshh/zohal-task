"""Same Idempotency-Key, one provider call. The store is in memory."""

import json

import pytest
from fastapi.responses import JSONResponse

from app.idempotency import IdempotencyInProgress, Pending, ResultStore, Stored, once
from app.providers import ProviderCallError


class MemoryStore(ResultStore):
    def __init__(self) -> None:
        self._items: dict[str, str | tuple[int, dict]] = {}

    async def read(self, key: str) -> Stored | Pending | None:
        item = self._items.get(key)
        if item is None:
            return None
        if item == "pending":
            return Pending()
        status, body = item
        return Stored(JSONResponse(status_code=status, content=body))

    async def claim(self, key: str) -> bool:
        if key in self._items:
            return False
        self._items[key] = "pending"
        return True

    async def finish(self, key: str, response: JSONResponse) -> None:
        self._items[key] = (response.status_code, json.loads(response.body))

    async def release(self, key: str) -> None:
        self._items.pop(key, None)


class Provider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self) -> JSONResponse:
        self.calls.append("a")
        return JSONResponse(status_code=200, content={"provider": "a", "echo": {"order_id": "1001"}})


async def test_same_key_calls_the_provider_once() -> None:
    store = MemoryStore()
    provider = Provider()

    first = await once(store, "order-1001", "trace-1", provider)
    second = await once(store, "order-1001", "trace-2", provider)

    assert provider.calls == ["a"]
    assert first.status_code == 200
    assert json.loads(second.body) == {"provider": "a", "echo": {"order_id": "1001"}}


async def test_pending_key_does_not_call_the_provider() -> None:
    store = MemoryStore()
    await store.claim("order-1001")
    provider = Provider()

    with pytest.raises(IdempotencyInProgress):
        await once(store, "order-1001", "trace-3", provider)

    assert provider.calls == []


async def test_rate_limit_failure_drops_the_claim_so_a_retry_can_call() -> None:
    store = MemoryStore()
    provider = Provider()

    async def denied() -> JSONResponse:
        raise ProviderCallError("rate_limit_unavailable", 503)

    with pytest.raises(ProviderCallError):
        await once(store, "order-1001", "trace-4", denied)

    response = await once(store, "order-1001", "trace-5", provider)

    assert provider.calls == ["a"]
    assert response.status_code == 200
    assert await store.read("order-1001") is not None
