"""Outbound call to provider A.

One ``httpx.AsyncClient`` is created with the process and closed with it.
A route must not construct a client. Doing that would pay a new
connection on every request and defeat keepalive.
"""

import logging
import time
from typing import Any

import httpx
from fastapi.responses import JSONResponse

logger = logging.getLogger("gateway")


class ProviderCallError(Exception):
    """The provider was not reached, or its body was not JSON.

    An HTTP response from the provider, including 4xx and 5xx, is not
    this error. That response is returned to the caller as-is.
    """

    def __init__(self, error: str, status_code: int, retry_after: int | None = None) -> None:
        self.error = error
        self.status_code = status_code
        self.retry_after = retry_after


class ProviderClient:
    def __init__(self, client: httpx.AsyncClient, url: str, name: str = "a") -> None:
        self._client = client
        self._url = url
        self.name = name

    async def process(
        self,
        body: dict[str, Any],
        trace_id: str,
        timeout: httpx.Timeout | None = None,
    ) -> JSONResponse:
        started = time.perf_counter()
        try:
            response = await self._client.post(
                self._url,
                json=body,
                headers={"X-Request-ID": trace_id},
                timeout=timeout,
            )
        except httpx.TimeoutException as exc:
            self._log(trace_id, "timeout", started, error=type(exc).__name__)
            raise ProviderCallError("upstream_timeout", 504) from exc
        except httpx.HTTPError as exc:
            self._log(trace_id, "error", started, error=type(exc).__name__)
            raise ProviderCallError("upstream_unavailable", 502) from exc

        self._log(trace_id, str(response.status_code), started)
        return _json_response(response)

    def _log(self, trace_id: str, status: str, started: float, error: str | None = None) -> None:
        latency_ms = (time.perf_counter() - started) * 1000
        if error is None:
            logger.info(
                "provider_call trace_id=%s provider=%s status=%s latency_ms=%.1f",
                trace_id,
                self.name,
                status,
                latency_ms,
            )
            return
        logger.warning(
            "provider_call trace_id=%s provider=%s status=%s latency_ms=%.1f error=%s",
            trace_id,
            self.name,
            status,
            latency_ms,
            error,
        )


def build_client(timeout: httpx.Timeout, max_connections: int) -> httpx.AsyncClient:
    """The pool matches the in-flight cap so a full bulkhead does not queue inside httpx."""
    return httpx.AsyncClient(
        timeout=timeout,
        limits=httpx.Limits(
            max_connections=max_connections,
            max_keepalive_connections=min(20, max_connections),
            keepalive_expiry=30.0,
        ),
    )


def _json_response(response: httpx.Response) -> JSONResponse:
    try:
        content = response.json()
    except ValueError as exc:
        raise ProviderCallError("upstream_invalid_response", 502) from exc
    if not isinstance(content, (dict, list)):
        raise ProviderCallError("upstream_invalid_response", 502)

    headers: dict[str, str] = {}
    retry_after = response.headers.get("retry-after")
    if retry_after is not None:
        headers["retry-after"] = retry_after
    return JSONResponse(
        status_code=response.status_code,
        content=content,
        headers=headers,
    )
