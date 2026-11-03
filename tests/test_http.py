"""The route rejects a bad body before any provider is built.

The lifespan is replaced so the test does not open Redis or the broker.
"""

from contextlib import asynccontextmanager

from fastapi.testclient import TestClient

from app.main import create_app


class _Provider:
    def __init__(self) -> None:
        self.calls: list[str] = []

    async def execute(self, body: dict, trace_id: str, trail: list | None = None):
        self.calls.append(trace_id)
        raise AssertionError("a provider was called")


def test_invalid_json_never_calls_a_provider() -> None:
    provider = _Provider()

    @asynccontextmanager
    async def lifespan(app):
        app.state.router = provider
        yield

    app = create_app()
    app.router.lifespan_context = lifespan
    with TestClient(app) as client:
        broken = client.post(
            "/v1/process",
            content=b"{",
            headers={"content-type": "application/json"},
        )
        extra = client.post("/v1/process", json={"payload": {"order_id": "1001"}, "extra": 1})
        text = client.post("/v1/process", json={"payload": "1001"})

    assert broken.status_code == 422
    assert extra.status_code == 422
    assert text.status_code == 422
    assert provider.calls == []
