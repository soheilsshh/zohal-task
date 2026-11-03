"""The audit message is plain JSON. No broker and no database."""

import json
from datetime import datetime, timezone

from app.audit import BODY_CAP_BYTES, audit_message


def _message(**overrides: object) -> dict:
    started = datetime(2026, 11, 3, 13, 35, tzinfo=timezone.utc)
    fields = {
        "trace_id": "phase8",
        "started_at": started,
        "finished_at": started,
        "provider": "a",
        "final_status": 200,
        "latency_ms": 1200.34,
        "attempts": [{"provider": "a", "status": 200, "latency_ms": 1199.6, "error": None}],
        "error_class": None,
        "request_body": {"payload": {"order_id": "1001"}},
        "response_body": {"id": "txn", "provider": "a"},
    }
    fields.update(overrides)
    return audit_message(**fields)


def test_audit_message_is_json_and_keeps_the_attempts() -> None:
    message = _message()

    encoded = json.dumps(message)

    assert json.loads(encoded)["trace_id"] == "phase8"
    assert json.loads(encoded)["attempts"][0]["provider"] == "a"
    assert message["latency_ms"] == 1200.3
    assert message["error_class"] is None
    assert message["request"]["dropped"] is False
    assert message["request"]["content"]["payload"]["order_id"] == "1001"
    assert message["response"]["content"]["provider"] == "a"


def test_oversized_body_keeps_the_size_and_drops_the_content() -> None:
    message = _message(request_body={"payload": {"blob": "x" * (BODY_CAP_BYTES + 1)}})

    assert message["request"]["dropped"] is True
    assert message["request"]["bytes"] > BODY_CAP_BYTES
    assert "content" not in message["request"]
    assert message["response"]["dropped"] is False
    json.dumps(message)
