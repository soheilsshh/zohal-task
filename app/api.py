"""HTTP routes.

``main`` builds the app, binds the trace id, and registers handlers.
This module only decides what each route returns.
"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from app.providers import ProviderClient
from app.schemas import ErrorResponse, ProcessRequest, ProviderResult

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
        502: {"model": ErrorResponse},
        504: {"model": ErrorResponse},
    },
)
async def process(body: ProcessRequest, request: Request) -> JSONResponse:
    """Forward the JSON object to provider A and return A's status and body.

    Network failure and timeout become this service's error body.
    An HTTP response from A, including 4xx and 5xx, is passed through.
    """
    client: ProviderClient = request.app.state.provider_a
    return await client.process(
        body.model_dump(exclude_none=True),
        request.state.trace_id,
    )
