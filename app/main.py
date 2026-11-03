"""ASGI entrypoint.

Uvicorn loads the module-level ``app`` object (``app.main:app``).
``create_app`` is the single place that builds it, so later phases can
add startup and shutdown there without a second copy of the route table.
"""

from fastapi import FastAPI


def create_app() -> FastAPI:
    application = FastAPI(
        title="Zohal Gateway",
        version="0.1.0",
    )

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Process liveness.

        Answers whether this process can serve HTTP. It must not call a
        provider, Redis, or a database. Dependency checks belong on a
        readiness route, which does not exist yet.
        """
        return {"status": "ok"}

    return application


app = create_app()
