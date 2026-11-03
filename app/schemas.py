"""Public request, response, and error bodies.

These models are the contract a client can depend on. Phase 3 replaces
the echoed object with the real provider fields. The error model stays.
"""

from typing import Any

from pydantic import BaseModel, Field, RootModel


class ProcessRequest(RootModel[dict[str, Any]]):
    """One JSON object.

    A root array or a JSON scalar is rejected. There is no field list
    yet because the provider contract has not been observed.
    """


class ProcessResponse(BaseModel):
    trace_id: str
    body: dict[str, Any]


class ValidationDetail(BaseModel):
    loc: list[str]
    msg: str
    type: str


class ErrorResponse(BaseModel):
    trace_id: str
    error: str
    details: list[ValidationDetail] = Field(default_factory=list)
