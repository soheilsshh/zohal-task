"""One ``Idempotency-Key`` produces one provider call.

The first request claims the key. A repeat reads the stored response
and does not call ``call``. A second request that arrives while the
claim is still open gets ``IdempotencyInProgress``. The pending claim
expires on its own if this process dies before ``finish``.

A failed save after the provider has already answered does not change
that answer. The next retry can call the provider again. That window
is the cost of answering the client before Redis confirms the write.
"""

import json
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from fastapi.responses import JSONResponse

from app.providers import ProviderCallError
from app.schemas import ErrorResponse

logger = logging.getLogger("gateway")

_KEY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class InvalidIdempotencyKey(Exception):
    """The header is present and is not a safe token."""


class IdempotencyInProgress(Exception):
    """Another request holds this key and has not stored a response yet."""


class ResultStore:
    async def read(self, key: str) -> "Stored | Pending | None":
        raise NotImplementedError

    async def claim(self, key: str) -> bool:
        raise NotImplementedError

    async def finish(self, key: str, response: JSONResponse) -> None:
        raise NotImplementedError

    async def release(self, key: str) -> None:
        raise NotImplementedError


@dataclass(frozen=True)
class Stored:
    response: JSONResponse


@dataclass(frozen=True)
class Pending:
    pass


def normalize_key(raw: str | None) -> str | None:
    """Return a key Redis can store, or ``None`` when the header is absent.

    A blank header is absent. A header with spaces or other characters
    is rejected so it cannot change the Redis key shape.
    """
    if raw is None:
        return None
    candidate = raw.strip()
    if candidate == "":
        return None
    if _KEY.fullmatch(candidate) is None:
        raise InvalidIdempotencyKey()
    return candidate


async def once(
    store: ResultStore,
    key: str | None,
    trace_id: str,
    call: Callable[[], Awaitable[JSONResponse]],
) -> JSONResponse:
    """Run ``call`` once for this key. No key means always run ``call``."""
    if key is None:
        return await call()

    found = await store.read(key)
    if isinstance(found, Stored):
        logger.info("idempotency_hit trace_id=%s", trace_id)
        return found.response
    if isinstance(found, Pending):
        raise IdempotencyInProgress()
    if not await store.claim(key):
        found = await store.read(key)
        if isinstance(found, Stored):
            logger.info("idempotency_hit trace_id=%s", trace_id)
            return found.response
        raise IdempotencyInProgress()

    try:
        response = await call()
    except ProviderCallError as exc:
        if exc.error in {"rate_limit_unavailable", "admission_rejected"}:
            await _release(store, key, trace_id)
            raise
        await _finish(store, key, _error_json(trace_id, exc), trace_id)
        raise
    await _finish(store, key, response, trace_id)
    return response


def _error_json(trace_id: str, exc: ProviderCallError) -> JSONResponse:
    payload = ErrorResponse(trace_id=trace_id, error=exc.error)
    return JSONResponse(status_code=exc.status_code, content=payload.model_dump())


async def _finish(store: ResultStore, key: str, response: JSONResponse, trace_id: str) -> None:
    try:
        await store.finish(key, response)
    except Exception:
        logger.error("idempotency_store_failed trace_id=%s", trace_id)


async def _release(store: ResultStore, key: str, trace_id: str) -> None:
    try:
        await store.release(key)
    except Exception:
        logger.error("idempotency_release_failed trace_id=%s", trace_id)


def dump_response(response: JSONResponse) -> str:
    """JSON text stored under the idempotency key."""
    retry_after = response.headers.get("retry-after")
    payload = {
        "state": "done",
        "status": response.status_code,
        "body": response.body.decode("utf-8"),
    }
    if retry_after is not None:
        payload["retry_after"] = retry_after
    return json.dumps(payload, separators=(",", ":"))


def load_response(raw: str) -> Stored | Pending:
    """Rebuild a stored response. A pending claim stays a claim."""
    data = json.loads(raw)
    if data.get("state") == "pending":
        return Pending()
    if data.get("state") != "done":
        raise ValueError("idempotency record is not done or pending")
    headers: dict[str, str] = {}
    retry_after = data.get("retry_after")
    if isinstance(retry_after, str) and retry_after:
        headers["retry-after"] = retry_after
    return Stored(
        JSONResponse(
            status_code=int(data["status"]),
            content=json.loads(data["body"]),
            headers=headers,
        )
    )
