"""Process settings.

Environment variables override the env file. The env file overrides
the defaults below. Provider A is called from this process.
B and C are loaded so the names exist, and nothing calls them yet.
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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Build settings once per process.

    Uvicorn's reloader starts a new process, so a changed env file is
    picked up without clearing this cache by hand.
    """
    return Settings()
