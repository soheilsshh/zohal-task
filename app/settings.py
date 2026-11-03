"""Process settings.

Environment variables override the env file. The env file overrides
the defaults below. Provider URLs are loaded now and called in a later
phase; importing them here keeps every URL out of the route code.
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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Build settings once per process.

    Uvicorn's reloader starts a new process, so a changed env file is
    picked up without clearing this cache by hand.
    """
    return Settings()
