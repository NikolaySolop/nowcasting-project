from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "nowcast-ingestion"
    log_level: str = "INFO"

    request_timeout_seconds: float = 30.0
    request_user_agent: str = "nowcast-ingestion/0.1"
    retry_attempts: int = 3
    retry_backoff_seconds: float = 1.5

    source_config_path: Path | None = None
    scheduler_enabled: bool = False

    model_config = SettingsConfigDict(
        env_prefix="INGESTION_",
        env_file=".env",
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
