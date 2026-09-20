from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://queue:queue@localhost:5432/queue"
    redis_url: str = "redis://localhost:6379/0"
    queue_name: str = "default"
    queue_depth_limit: int = Field(default=10_000, ge=1)
    visibility_timeout_seconds: int = Field(default=60, ge=5)
    worker_heartbeat_seconds: int = Field(default=5, ge=1)
    recovery_interval_seconds: int = Field(default=10, ge=1)
    default_job_timeout_seconds: int = Field(default=30, ge=1, le=3600)
    backoff_schedule_seconds: tuple[int, ...] = (5, 30, 300)
    worker_concurrency: int = Field(default=4, ge=1, le=64)

    @field_validator("backoff_schedule_seconds", mode="before")
    @classmethod
    def parse_backoff(cls, value: object) -> object:
        if isinstance(value, str):
            return tuple(int(item.strip()) for item in value.split(",") if item.strip())
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
