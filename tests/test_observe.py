"""JSON logs carry the trace id. The duration histogram reaches 10 seconds."""

import json
import logging

from app.log import configure_logging, logger, trace_id
from app.metrics import DURATION_BUCKETS, HTTP_REQUESTS, record_http, render_metrics
from app.providers import provider_event_is_warning


def test_json_log_line_has_the_trace_id(capsys) -> None:
    configure_logging()
    token = trace_id.set("phase10")
    logger.info("provider_call status=200")
    trace_id.reset(token)

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)

    assert payload["trace_id"] == "phase10"
    assert payload["level"] == "INFO"
    assert payload["message"] == "provider_call status=200"


def test_provider_500_is_a_warning_and_a_200_is_not() -> None:
    assert provider_event_is_warning("500", None) is True
    assert provider_event_is_warning("200", None) is False
    assert provider_event_is_warning("timeout", "ReadTimeout") is True
    assert logging.WARNING > logging.INFO


def test_duration_buckets_include_ten_seconds_and_metrics_move() -> None:
    assert 10 in DURATION_BUCKETS
    assert any(bucket > 3 for bucket in DURATION_BUCKETS)

    before = HTTP_REQUESTS.labels(status="200")._value.get()
    record_http(200, 0.2)
    text = render_metrics().decode()

    assert HTTP_REQUESTS.labels(status="200")._value.get() == before + 1
    assert "gateway_http_request_duration_seconds" in text
    assert "gateway_provider_outcomes_total" in text
    assert "gateway_audit_publish_failures_total" in text
