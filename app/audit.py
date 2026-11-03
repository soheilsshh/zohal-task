"""Narrow audit row for one handled request.

Moved to the worker in phase 8. The insert stays on the request path
so the row is visible in psql before the response is sent. The body is
not stored here.
"""

import logging
import time
from datetime import datetime
from urllib.parse import urlparse

from fastapi.responses import JSONResponse
from sqlalchemy import DateTime, Float, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

logger = logging.getLogger("gateway")


class Base(DeclarativeBase):
    pass


class AuditRequest(Base):
    """One row per trace id. A repeated trace id does not create a second row."""

    __tablename__ = "audit_requests"

    trace_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    provider: Mapped[str | None] = mapped_column(String(16))
    final_status: Mapped[int] = mapped_column(Integer)
    latency_ms: Mapped[float] = mapped_column(Float)
    attempts: Mapped[list] = mapped_column(JSONB)
    error_class: Mapped[str | None] = mapped_column(String(64))


def create_engine(url: str) -> AsyncEngine:
    return create_async_engine(url, pool_size=5, max_overflow=5, pool_pre_ping=True)


def database_target(url: str) -> str:
    """Host, port, and database name. The URL may carry a password."""
    parsed = urlparse(url)
    host = parsed.hostname or "unknown"
    port = parsed.port or 5432
    name = parsed.path.lstrip("/") or "unknown"
    return f"{host}:{port}/{name}"


def provider_of(trail: list[dict], response: JSONResponse | None) -> str | None:
    """The provider that produced the outcome. A replay has an empty trail and a stored body."""
    if trail:
        provider = trail[-1].get("provider")
        if isinstance(provider, str):
            return provider
    body = _body(response)
    if isinstance(body, dict) and isinstance(body.get("provider"), str):
        return body["provider"]
    return None


def error_class_of(status: int, response: JSONResponse | None, raised: str | None) -> str | None:
    """Our error name, or ``upstream_<status>`` when the provider's own status is the answer."""
    if raised is not None:
        return raised
    if status < 400:
        return None
    body = _body(response)
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return body["error"]
    return f"upstream_{status}"


def _body(response: JSONResponse | None) -> object:
    if response is None:
        return None
    try:
        import json

        return json.loads(response.body)
    except (ValueError, TypeError):
        return None


class AuditStore:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine
        self._sessions = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async def ping(self) -> None:
        try:
            async with self._engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except Exception as exc:
            logger.error("postgres_unavailable error=%s", type(exc).__name__)
            return
        logger.info("postgres opened")

    async def record(
        self,
        *,
        trace_id: str,
        started_at: datetime,
        finished_at: datetime,
        provider: str | None,
        final_status: int,
        latency_ms: float,
        attempts: list[dict],
        error_class: str | None,
        database_url: str,
    ) -> None:
        """Insert one row. A duplicate trace id is ignored.

        Moved to the worker in phase 8.
        """
        started = time.perf_counter()
        statement = insert(AuditRequest).values(
            trace_id=trace_id,
            started_at=started_at,
            finished_at=finished_at,
            provider=provider,
            final_status=final_status,
            latency_ms=round(latency_ms, 1),
            attempts=attempts,
            error_class=error_class,
        ).on_conflict_do_nothing(index_elements=["trace_id"])
        try:
            async with self._sessions() as session:
                await session.execute(statement)
                await session.commit()
        except Exception as exc:
            logger.error("audit_insert_failed trace_id=%s error=%s", trace_id, type(exc).__name__)
            return
        logger.info(
            "audit_insert trace_id=%s status=%s provider=%s latency_ms=%.1f insert_ms=%.1f target=%s",
            trace_id,
            final_status,
            provider or "-",
            latency_ms,
            (time.perf_counter() - started) * 1000,
            database_target(database_url),
        )

    async def aclose(self) -> None:
        await self._engine.dispose()
