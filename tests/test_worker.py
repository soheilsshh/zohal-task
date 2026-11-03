"""A second delivery of the same trace id does not create a second record."""

from datetime import datetime, timezone

from pymongo.errors import DuplicateKeyError
from sqlalchemy.dialects import postgresql

from app.audit import audit_insert_statement
from app.bodies import insert_body


def _message(response_id: str) -> dict:
    return {
        "trace_id": "same",
        "request": {"dropped": False, "bytes": 2, "content": {"payload": {"order_id": "1001"}}},
        "response": {"dropped": False, "bytes": 2, "content": {"id": response_id}},
    }


class _Bodies:
    def __init__(self) -> None:
        self.docs: list[dict] = []

    def insert_one(self, document: dict) -> None:
        if any(existing["trace_id"] == document["trace_id"] for existing in self.docs):
            raise DuplicateKeyError("E11000 duplicate key")
        self.docs.append(document)


def test_redelivery_keeps_the_first_body() -> None:
    collection = _Bodies()

    insert_body(collection, _message("first"))
    insert_body(collection, _message("second"))

    assert len(collection.docs) == 1
    assert collection.docs[0]["response"]["content"]["id"] == "first"


def test_audit_insert_ignores_a_duplicate_trace_id() -> None:
    now = datetime(2026, 11, 3, tzinfo=timezone.utc).isoformat()
    statement = audit_insert_statement(
        {
            "trace_id": "same",
            "started_at": now,
            "finished_at": now,
            "provider": "a",
            "final_status": 200,
            "latency_ms": 1.0,
            "attempts": [],
            "error_class": None,
        }
    )

    sql = str(statement.compile(dialect=postgresql.dialect()))

    assert "ON CONFLICT (trace_id) DO NOTHING" in sql
