"""HTTP routes.

``main`` builds the app, binds the trace id, and registers handlers.
This module only decides what each route returns.
"""

from fastapi import APIRouter, Request

from app.schemas import ProcessRequest, ProcessResponse

router = APIRouter()


@router.get("/healthz")
async def healthz() -> dict[str, str]:
    """Process liveness.

    Answers whether this process can serve HTTP. It must not call a
    provider, Redis, or a database. Dependency checks belong on a
    readiness route, which does not exist yet.
    """
    return {"status": "ok"}


@router.post("/v1/process")
async def process(body: ProcessRequest, request: Request) -> ProcessResponse:
    """Accept a JSON object and echo it.

    No upstream call is made. The handler exists so the request cycle,
    the trace id, and validation are real before a provider is added.
    """
    return ProcessResponse(trace_id=request.state.trace_id, body=body.root)
