"""Redis admission: a token bucket for A, and a fixed window per caller.

The bucket is one Lua script. Redis runs that script alone, so two
gateway processes cannot both read "one token left" and both take it.
The inbound counter is a separate script for the same reason: ``INCR``
and ``EXPIRE`` happen together. It is still a fixed window, so the
second just after a boundary can allow the limit again.

Redis errors before a provider call reject the call. An empty bucket
does not. An empty bucket means A has no budget, and the router may
try B or C once. A Redis error means we do not know, so calling anyone
would spend a provider we can no longer count.

The bucket numbers are guesses. The key expires after a quiet minute
and the next call starts from a full bucket, which is also where a
quiet minute of refill would have landed.
"""

import logging
import time
from typing import NoReturn
from urllib.parse import urlparse

from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.idempotency import Pending, ResultStore, Stored, dump_response, load_response
from app.providers import ProviderCallError
from app.settings import Settings

logger = logging.getLogger("gateway")

# KEYS[1] bucket hash. ARGV: capacity, refill per second, key ttl ms.
# TIME is Redis's clock, so two app hosts do not need to agree.
_BUCKET = """
local capacity = tonumber(ARGV[1])
local refill_per_second = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])
if refill_per_second <= 0 or capacity <= 0 then
  return {0, 1000}
end
local clock = redis.call('TIME')
local now_ms = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now_ms
end
local elapsed_ms = math.max(0, now_ms - ts)
tokens = math.min(capacity, tokens + (elapsed_ms * refill_per_second / 1000))
local allowed = 0
local retry_after_ms = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
else
  retry_after_ms = math.ceil((1 - tokens) * 1000 / refill_per_second)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now_ms)
redis.call('PEXPIRE', KEYS[1], ttl_ms)
return {allowed, retry_after_ms}
"""

# KEYS[1] is already the current window. ARGV: limit, ttl seconds.
_INBOUND = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[2])
end
if current > tonumber(ARGV[1]) then
  return 0
end
return 1
"""

_BUCKET_KEY = "bucket:a"
_BUCKET_TTL_MS = 60_000


def open_redis(url: str) -> Redis:
    """One client for this process. The caller closes it."""
    return Redis.from_url(
        url,
        socket_timeout=0.2,
        socket_connect_timeout=0.2,
        decode_responses=True,
    )


def redis_target(url: str) -> str:
    """Host and port only. The URL may carry a password."""
    parsed = urlparse(url)
    host = parsed.hostname or "unknown"
    port = parsed.port or 6379
    return f"{host}:{port}"


class ProviderABudget:
    """Adapter the router calls. Only A spends a token."""

    def __init__(self, gate: "RedisGate") -> None:
        self._gate = gate

    async def take(self, provider: str) -> str:
        if provider != "a":
            return "ok"
        return await self._gate.take_a()


class RedisGate(ResultStore):
    def __init__(self, client: Redis, settings: Settings) -> None:
        self._redis = client
        self._settings = settings

    async def ping(self) -> None:
        try:
            await self._redis.ping()
        except RedisError as exc:
            logger.error("redis_unavailable op=ping error=%s", type(exc).__name__)
            return
        logger.info("redis opened target=%s", redis_target(self._settings.redis_url))

    async def aclose(self) -> None:
        await self._redis.aclose()

    async def allow_inbound(self, client_key: str) -> bool:
        window = int(time.time()) // self._settings.inbound_window_seconds
        key = f"inbound:{client_key}:{window}"
        ttl = max(self._settings.inbound_window_seconds * 2, 1)
        try:
            allowed = await self._redis.eval(
                _INBOUND,
                1,
                key,
                self._settings.inbound_limit,
                ttl,
            )
        except RedisError as exc:
            self._closed("inbound", exc, "rate_limit_unavailable")
        return int(allowed) == 1

    async def take_a(self) -> str:
        settings = self._settings
        try:
            result = await self._redis.eval(
                _BUCKET,
                1,
                _BUCKET_KEY,
                settings.provider_a_bucket_capacity,
                settings.provider_a_refill_per_second,
                _BUCKET_TTL_MS,
            )
        except RedisError as exc:
            self._closed("bucket:a", exc, "rate_limit_unavailable")
        allowed, retry_after_ms = result
        if int(allowed) == 1:
            return "ok"
        logger.info("bucket_denied provider=a retry_after_ms=%s", int(retry_after_ms))
        return "empty"

    async def read(self, key: str) -> Stored | Pending | None:
        try:
            raw = await self._redis.get(self._idem_key(key))
        except RedisError as exc:
            self._closed("idempotency_read", exc, "idempotency_unavailable")
        if raw is None:
            return None
        try:
            return load_response(raw)
        except (ValueError, KeyError, TypeError) as exc:
            logger.error("idempotency_corrupt error=%s", type(exc).__name__)
            raise ProviderCallError("idempotency_unavailable", 503) from exc

    async def claim(self, key: str) -> bool:
        try:
            created = await self._redis.set(
                self._idem_key(key),
                '{"state":"pending"}',
                nx=True,
                ex=self._settings.idempotency_pending_seconds,
            )
        except RedisError as exc:
            self._closed("idempotency_claim", exc, "idempotency_unavailable")
        return bool(created)

    async def finish(self, key: str, response: JSONResponse) -> None:
        try:
            await self._redis.set(
                self._idem_key(key),
                dump_response(response),
                ex=self._settings.idempotency_ttl_seconds,
            )
        except RedisError as exc:
            raise ProviderCallError("idempotency_unavailable", 503) from exc

    async def release(self, key: str) -> None:
        try:
            await self._redis.delete(self._idem_key(key))
        except RedisError as exc:
            raise ProviderCallError("idempotency_unavailable", 503) from exc

    def _closed(self, op: str, exc: RedisError, error: str) -> NoReturn:
        logger.error("redis_unavailable op=%s error=%s", op, type(exc).__name__)
        raise ProviderCallError(error, 503) from exc

    def _idem_key(self, key: str) -> str:
        return f"idem:{key}"
