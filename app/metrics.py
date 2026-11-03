"""Process-local Prometheus metrics.

No label is a trace id or a client id. Those belong in the log line.
Gauges describe this process only. A second replica has its own series.
"""

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# Upper bounds in seconds. 5 and 10 sit past the 3 second deadline so a
# slow provider is a bucket, not only +Inf.
DURATION_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)

HTTP_REQUESTS = Counter(
    "gateway_http_requests_total",
    "HTTP responses from this process, excluding liveness, readiness, and metrics.",
    ["status"],
)
HTTP_DURATION = Histogram(
    "gateway_http_request_duration_seconds",
    "Time until the HTTP response is returned, including the provider call.",
    buckets=DURATION_BUCKETS,
)
PROVIDER_OUTCOMES = Counter(
    "gateway_provider_outcomes_total",
    "Provider answers and transport failures. A 500 is a status, not an exception.",
    ["provider", "status"],
)
INFLIGHT = Gauge(
    "gateway_inflight",
    "Calls this process has in flight for one provider.",
    ["provider"],
)
ADMISSION_INFLIGHT = Gauge(
    "gateway_admission_inflight",
    "Calls this process has accepted and not finished.",
)
CIRCUIT_STATE = Gauge(
    "gateway_circuit_state",
    "0 closed, 1 half_open, 2 open. One series per provider, per process.",
    ["provider"],
)
ADMISSION_REJECTED = Counter(
    "gateway_admission_rejected_total",
    "Calls rejected because this process was already full.",
)
PUBLISH_FAILURES = Counter(
    "gateway_audit_publish_failures_total",
    "Audit messages that did not reach RabbitMQ.",
)

_CIRCUIT = {"closed": 0, "half_open": 1, "open": 2}

METRICS_MEDIA_TYPE = CONTENT_TYPE_LATEST


def record_http(status: int, seconds: float) -> None:
    HTTP_REQUESTS.labels(status=str(status)).inc()
    HTTP_DURATION.observe(seconds)


def record_provider(provider: str, status: str) -> None:
    PROVIDER_OUTCOMES.labels(provider=provider, status=status).inc()


def set_inflight(provider: str, value: int) -> None:
    INFLIGHT.labels(provider=provider).set(value)


def set_admission(value: int) -> None:
    ADMISSION_INFLIGHT.set(value)


def set_circuit(provider: str, state: str) -> None:
    CIRCUIT_STATE.labels(provider=provider).set(_CIRCUIT[state])


def record_admission_rejected() -> None:
    ADMISSION_REJECTED.inc()


def record_publish_failure() -> None:
    PUBLISH_FAILURES.inc()


def render_metrics() -> bytes:
    return generate_latest()
