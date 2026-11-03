"""The audit message is plain JSON. No broker and no database."""

import json
from datetime import datetime, timezone

from app.audit import audit_message


def test_audit_message_is_json_and_keeps_the_attempts() -> None:
    started = datetime(2026, 11, 3, 13, 35, tzinfo=timezone.utc)
    message = audit_message(
        trace_id="phase8",
        started_at=started,
        finished_at=started,
        provider="a",
        final_status=200,
        latency_ms=1200.34,
        attempts=[{"provider": "a", "status": 200, "latency_ms": 1199.6, "error": None}],
        error_class=None,
    )

    encoded = json.dumps(message)

    assert json.loads(encoded)["trace_id"] == "phase8"
    assert json.loads(encoded)["attempts"][0]["provider"] == "a"
    assert message["latency_ms"] == 1200.3
    assert message["error_class"] is None
