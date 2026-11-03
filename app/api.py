"""HTTP routes.

``main`` builds the app, binds the trace id, and registers handlers.
This module only decides what each route returns.
"""

import asyncio
import logging
import re
import time
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.audit import audit_message, error_class_of, provider_of
from app.idempotency import IdempotencyInProgress, InvalidIdempotencyKey, normalize_key, once
from app.limits import RedisGate
from app.providers import ProviderCallError
from app.routing import Router
from app.schemas import ErrorResponse, ProcessRequest, ProviderResult
from app.tasks import publish_audit

logger = logging.getLogger("gateway")

_IDENTITY = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

router = APIRouter()


@router.get("/", include_in_schema=False)
async def swagger() -> RedirectResponse:
    """Swagger UI lives at ``/docs``. ``/`` only sends a browser there."""
    return RedirectResponse(url="/docs", status_code=307)


@router.get("/healthz", tags=["ops"])
async def healthz() -> dict[str, str]:
    """Process liveness.

    Answers whether this process can serve HTTP. It must not call a
    provider, Redis, or a database. Dependency checks belong on a
    readiness route, which does not exist yet.
    """
    return {"status": "ok"}


@router.post(
    "/v1/process",
    tags=["process"],
    responses={
        200: {"model": ProviderResult},
        409: {"model": ErrorResponse},
        422: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
        502: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
        504: {"model": ErrorResponse},
    },
)
async def process(
    body: ProcessRequest,
    request: Request,
    idempotency_key: Annotated[
        str | None,
        Header(
            alias="Idempotency-Key",
            description="Repeat of this key returns the stored response and does not call a provider.",
        ),
    ] = None,
    x_client_id: Annotated[
        str | None,
        Header(alias="X-Client-Id", description="Caller identity for the inbound limit. Falls back to the peer address."),
    ] = None,
) -> JSONResponse:
    """Prefer A when its bucket has a token. One failover may try B or C.

    The audit message is published before the response is sent. The
    worker inserts the row. This request does not wait for PostgreSQL.
    The same ``Idempotency-Key`` returns the stored response and does
    not call a provider.
    """
    trace_id = request.state.trace_id
    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    trail: list[dict] = []
    status = 500
    raised: str | None = None
    response: JSONResponse | None = None
    try:
        try:
            key = normalize_key(idempotency_key)
        except InvalidIdempotencyKey:
            status = 422
            raised = "invalid_idempotency_key"
            response = JSONResponse(
                status_code=422,
                content=ErrorResponse(trace_id=trace_id, error=raised).model_dump(),
            )
            return response

        gate: RedisGate = request.app.state.gate
        host = request.client.host if request.client is not None else None
        identity = _client_identity(x_client_id, host)
        if not await gate.allow_inbound(identity):
            status = 429
            raised = "rate_limited"
            logger.info("inbound_limited trace_id=%s client=%s", trace_id, identity)
            retry_after = request.app.state.settings.inbound_window_seconds
            response = JSONResponse(
                status_code=429,
                content=ErrorResponse(trace_id=trace_id, error=raised).model_dump(),
                headers={"Retry-After": str(retry_after)},
            )
            return response

        gateway: Router = request.app.state.router

        async def call() -> JSONResponse:
            return await gateway.execute(body.model_dump(exclude_none=True), trace_id, trail)

        try:
            response = await once(gate, key, trace_id, call)
        except IdempotencyInProgress:
            status = 409
            raised = "idempotency_in_progress"
            response = JSONResponse(
                status_code=409,
                content=ErrorResponse(trace_id=trace_id, error=raised).model_dump(),
            )
            return response
        status = response.status_code
        return response
    except ProviderCallError as exc:
        status = exc.status_code
        raised = exc.error
        raise
    finally:
        message = audit_message(
            trace_id=trace_id,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            provider=provider_of(trail, response),
            final_status=status,
            latency_ms=(time.perf_counter() - started) * 1000,
            attempts=list(trail),
            error_class=error_class_of(status, response, raised),
        )
        try:
            await asyncio.to_thread(publish_audit, message)
        except Exception as exc:
            logger.error(
                "audit_publish_failed trace_id=%s error=%s",
                trace_id,
                type(exc).__name__,
            )


def _client_identity(client_id: str | None, host: str | None) -> str:
    """Prefer a declared id. An address is the fallback, and a shared NAT shares that limit."""
    if client_id is not None and _IDENTITY.fullmatch(client_id.strip()):
        return "id:" + client_id.strip()
    if host is not None and _IDENTITY.fullmatch(host.strip()):
        return "ip:" + host.strip()
    return "ip:unknown"
