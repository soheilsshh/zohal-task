"""Narrow audit row for one handled request.

The API publishes the row. The Celery worker inserts it. A duplicate
``trace_id`` is ignored, so a redelivered message does not create a
second row. The body is not stored here.
"""

import logging
import time
from datetime import datetime

from fastapi.responses import JSONResponse
from sqlalchemy import DateTime, Float, Integer, String, create_engine
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

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


def create_sync_engine(url: str):
    """Engine for the worker. The API process does not open one."""
    return create_engine(url, pool_size=2, max_overflow=0, pool_pre_ping=True)


def audit_message(
    *,
    trace_id: str,
    started_at: datetime,
    finished_at: datetime,
    provider: str | None,
    final_status: int,
    latency_ms: float,
    attempts: list[dict],
    error_class: str | None,
) -> dict:
    """JSON-safe payload. The worker is a different process and cannot share objects."""
    return {
        "trace_id": trace_id,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "provider": provider,
        "final_status": final_status,
        "latency_ms": round(latency_ms, 1),
        "attempts": attempts,
        "error_class": error_class,
    }


def insert_audit_row(engine, message: dict) -> None:
    """Insert one row and commit. The caller retries when this raises.

    ``ON CONFLICT DO NOTHING`` makes a second delivery of the same trace id a success.
    """
    started = time.perf_counter()
    statement = (
        insert(AuditRequest)
        .values(
            trace_id=message["trace_id"],
            started_at=datetime.fromisoformat(message["started_at"]),
            finished_at=datetime.fromisoformat(message["finished_at"]),
            provider=message["provider"],
            final_status=message["final_status"],
            latency_ms=message["latency_ms"],
            attempts=message["attempts"],
            error_class=message["error_class"],
        )
        .on_conflict_do_nothing(index_elements=["trace_id"])
    )
    with Session(engine) as session:
        session.execute(statement)
        session.commit()
    logger.info(
        "audit_insert trace_id=%s status=%s provider=%s latency_ms=%s insert_ms=%.1f",
        message["trace_id"],
        message["final_status"],
        message["provider"] or "-",
        message["latency_ms"],
        (time.perf_counter() - started) * 1000,
    )


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
