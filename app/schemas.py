"""Public request and error bodies, plus the provider result we document.

``ProcessRequest`` matches provider A's published schema. The success
body is not re-validated on the way out: A owns that JSON, and a new
field from A must not become a 500 in our process.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ProcessRequest(BaseModel):
    """JSON object forwarded to provider A.

    ``payload`` is optional. When it is present it must be an object.
    Other top-level keys are rejected here so a typo is not silently
    dropped the way A drops unknown keys.
    """

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"example": {"payload": {"order_id": "1001"}}},
    )

    payload: dict[str, Any] | None = Field(
        default=None,
        description="Object provider A copies into its echo field.",
        examples=[{"order_id": "1001"}],
    )


class ProviderResult(BaseModel):
    """Shape observed from provider A on HTTP 200. Extra keys are allowed."""

    model_config = ConfigDict(extra="allow")

    id: str
    status: str
    provider: str
    echo: dict[str, Any]
    latency_ms: float
    request_id: str


class ValidationDetail(BaseModel):
    loc: list[str]
    msg: str
    type: str


class ErrorResponse(BaseModel):
    trace_id: str
    error: str
    details: list[ValidationDetail] = Field(default_factory=list)
