"""ASGI entrypoint.

Uvicorn loads the module-level ``app`` object (``app.main:app``).
``create_app`` is the single place that builds it.
"""

import logging
import re
import time
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response

from app.api import router
from app.limits import ProviderABudget, RedisGate, open_redis
from app.log import configure_logging, trace_id
from app.metrics import record_http, set_admission, set_circuit, set_inflight
from app.providers import ProviderCallError, ProviderClient, build_client
from app.resilience import Admission, Breaker
from app.routing import Router, Upstream
from app.schemas import ErrorResponse, ValidationDetail
from app.settings import get_settings

logger = logging.getLogger("gateway")

# A trace id is written into logs. Reject characters that would split a
# JSON line or a header, and cap the length a client can force us to store.
_TRACE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")

_HTTP_ERRORS = {
    404: "not_found",
    405: "method_not_allowed",
}


def resolve_trace_id(raw: str | None) -> str:
    """Use the caller's id when it is safe. Otherwise generate one.

    A missing or malformed ``X-Request-ID`` does not fail the request.
    The response still carries an id the caller can quote back.
    """
    if raw is not None:
        candidate = raw.strip()
        if _TRACE_ID.fullmatch(candidate):
            return candidate
    return uuid4().hex


_SKIP_METRICS = {"/healthz", "/readyz", "/metrics"}


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    settings = get_settings()
    app.state.settings = settings
    clients: list[httpx.AsyncClient] = []
    gate: RedisGate | None = None
    try:
        upstreams: list[Upstream] = []
        for name, url in (
            ("a", settings.provider_a_url),
            ("b", settings.provider_b_url),
            ("c", settings.provider_c_url),
        ):
            timeout = _provider_timeout(settings, name)
            max_inflight = getattr(settings, f"provider_{name}_max_inflight")
            client = build_client(timeout, max_inflight)
            clients.append(client)
            upstreams.append(
                Upstream(
                    ProviderClient(client, str(url), name=name),
                    max_inflight=max_inflight,
                    timeout=timeout,
                    breaker=Breaker(
                        name,
                        failure_threshold=settings.circuit_failure_threshold,
                        rate_limit_threshold=settings.circuit_rate_limit_threshold,
                        open_seconds=settings.circuit_open_seconds,
                    ),
                )
            )
            logger.info("provider client opened name=%s id=%s url=%s", name, id(client), url)
        for upstream in upstreams:
            set_inflight(upstream.name, 0)
            set_circuit(upstream.name, upstream.breaker.state)
        set_admission(0)
        gate = RedisGate(open_redis(settings.redis_url), settings)
        app.state.gate = gate
        await gate.ping()
        app.state.router = Router(
            upstreams,
            deadline_seconds=settings.deadline_seconds,
            min_attempt_seconds=settings.min_attempt_seconds,
            budget=ProviderABudget(gate),
            admission=Admission(settings.admission_max_inflight),
        )
        yield
    finally:
        for client in clients:
            await client.aclose()
            logger.info("provider client closed id=%s", id(client))
        if gate is not None:
            await gate.aclose()
            logger.info("redis closed")


def _error_response(
    status_code: int,
    payload: ErrorResponse,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=payload.model_dump(), headers=headers)


def create_app() -> FastAPI:
    application = FastAPI(
        title="Zohal Gateway",
        version="0.1.0",
        description=(
            "Synchronous gateway. Swagger UI is this page. "
            "POST /v1/process prefers provider A when its circuit admits it, "
            "its bucket has a token, and a bulkhead slot is free. "
            "It may fail over once, and it replays a stored Idempotency-Key."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    application.include_router(router)

    @application.middleware("http")
    async def bind_trace_id(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request.state.trace_id = resolve_trace_id(request.headers.get("x-request-id"))
        token = trace_id.set(request.state.trace_id)
        started = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            response.headers["X-Request-ID"] = request.state.trace_id
            return response
        finally:
            if request.url.path not in _SKIP_METRICS:
                record_http(status, time.perf_counter() - started)
            trace_id.reset(token)

    @application.exception_handler(RequestValidationError)
    async def validation_error(
        request: Request,
        exc: RequestValidationError,
    ) -> JSONResponse:
        payload = ErrorResponse(
            trace_id=_trace_id_from(request),
            error="validation_error",
            details=[
                ValidationDetail(
                    loc=[str(part) for part in err.get("loc", ())],
                    msg=str(err.get("msg", "")),
                    type=str(err.get("type", "")),
                )
                for err in exc.errors()
            ],
        )
        return _error_response(422, payload)

    @application.exception_handler(StarletteHTTPException)
    async def http_error(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        payload = ErrorResponse(
            trace_id=_trace_id_from(request),
            error=_HTTP_ERRORS.get(exc.status_code, "http_error"),
        )
        return _error_response(exc.status_code, payload)

    @application.exception_handler(ProviderCallError)
    async def provider_error(request: Request, exc: ProviderCallError) -> JSONResponse:
        payload = ErrorResponse(trace_id=_trace_id_from(request), error=exc.error)
        headers = None
        if exc.retry_after is not None:
            headers = {"Retry-After": str(exc.retry_after)}
        return _error_response(exc.status_code, payload, headers)

    return application


def _trace_id_from(request: Request) -> str:
    trace_id = getattr(request.state, "trace_id", None)
    if isinstance(trace_id, str) and trace_id:
        return trace_id
    return resolve_trace_id(None)


app = create_app()


def _provider_timeout(settings: object, name: str) -> httpx.Timeout:
    return httpx.Timeout(
        connect=getattr(settings, f"provider_{name}_connect_timeout_seconds"),
        read=getattr(settings, f"provider_{name}_read_timeout_seconds"),
        write=getattr(settings, f"provider_{name}_write_timeout_seconds"),
        pool=getattr(settings, f"provider_{name}_pool_timeout_seconds"),
    )
