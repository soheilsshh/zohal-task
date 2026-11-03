"""ASGI entrypoint.

Uvicorn loads the module-level ``app`` object (``app.main:app``).
``create_app`` is the single place that builds it.
"""

import logging
import re
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import Response

from app.api import router
from app.schemas import ErrorResponse, ValidationDetail
from app.settings import get_settings

logger = logging.getLogger("gateway")

# A trace id is written into logs later. Reject characters that would
# split a log line or a header, and cap the length a client can force us
# to store.
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


def _configure_logging() -> None:
    if logger.handlers:
        return
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    _configure_logging()
    settings = get_settings()
    app.state.settings = settings
    logger.info(
        "provider targets configured a=%s b=%s c=%s",
        settings.provider_a_url,
        settings.provider_b_url,
        settings.provider_c_url,
    )
    yield


def _error_response(status_code: int, payload: ErrorResponse) -> JSONResponse:
    return JSONResponse(status_code=status_code, content=payload.model_dump())


def create_app() -> FastAPI:
    application = FastAPI(
        title="Zohal Gateway",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(router)

    @application.middleware("http")
    async def bind_trace_id(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request.state.trace_id = resolve_trace_id(request.headers.get("x-request-id"))
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.trace_id
        return response

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

    return application


def _trace_id_from(request: Request) -> str:
    trace_id = getattr(request.state, "trace_id", None)
    if isinstance(trace_id, str) and trace_id:
        return trace_id
    return resolve_trace_id(None)


app = create_app()
