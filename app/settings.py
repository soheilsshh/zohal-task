"""Process settings.

Environment variables override the env file. The env file overrides
the defaults below. A call prefers provider A when the shared token
bucket has a token. One failure on the failover list may try B or C
inside the deadline. Nothing calls all three at once.
"""

from functools import lru_cache

from pydantic import AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    provider_a_url: AnyHttpUrl = "http://92.114.51.251:8001/v1/process"
    provider_b_url: AnyHttpUrl = "http://92.114.51.251:8002/v1/process"
    provider_c_url: AnyHttpUrl = "http://92.114.51.251:8003/v1/process"

    # A answers in about 300–1000 ms. The read timeout sits just above that.
    # Connect is 1 s because A is a remote host, not a container on localhost.
    provider_a_connect_timeout_seconds: float = 1.0
    provider_a_read_timeout_seconds: float = 1.5
    provider_a_write_timeout_seconds: float = 1.0
    provider_a_pool_timeout_seconds: float = 0.5

    # B and C may take up to 10 s. The read timeout is the per-call cap;
    # the deadline below still cancels the attempt earlier.
    provider_b_connect_timeout_seconds: float = 1.0
    provider_b_read_timeout_seconds: float = 3.0
    provider_b_write_timeout_seconds: float = 1.0
    provider_b_pool_timeout_seconds: float = 0.5
    provider_c_connect_timeout_seconds: float = 1.0
    provider_c_read_timeout_seconds: float = 3.0
    provider_c_write_timeout_seconds: float = 1.0
    provider_c_pool_timeout_seconds: float = 0.5

    deadline_seconds: float = 3.0
    min_attempt_seconds: float = 0.05

    # Per process, not per fleet. See app/routing.py.
    provider_a_max_inflight: int = 100
    provider_b_max_inflight: int = 20
    provider_c_max_inflight: int = 20

    # Per process, like the bulkheads. A second replica has its own door
    # and its own breakers. 120 in flight at about one second each is
    # about 120 RPS through this process: concurrency ≈ RPS × latency.
    admission_max_inflight: int = 120
    circuit_failure_threshold: int = 5
    circuit_rate_limit_threshold: int = 5
    circuit_open_seconds: float = 20.0

    # Inside Compose the hostname is the service name, not localhost.
    redis_url: str = "redis://redis:6379/0"
    # Guesses until a benchmark measures A's 429 line. Capacity is the
    # burst. Refill is the sustained rate. One quiet minute refills a
    # full bucket either way, because an expired key starts full.
    provider_a_bucket_capacity: float = 20
    provider_a_refill_per_second: float = 10
    # Fixed window, per caller. A window boundary can allow this twice.
    inbound_limit: int = 100
    inbound_window_seconds: int = 1
    idempotency_ttl_seconds: int = 86400
    idempotency_pending_seconds: int = 30

    # Inside Compose the hostname is the service name, not localhost.
    # The password is the local development password, the same one
    # postgres is started with. It is not a production secret.
    database_url: str = "postgresql+asyncpg://zohal:zohal@postgres:5432/zohal"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Build settings once per process.

    Uvicorn's reloader starts a new process, so a changed env file is
    picked up without clearing this cache by hand.
    """
    return Settings()
