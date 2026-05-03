from functools import lru_cache
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "nowcast-ingestion"
    log_level: str = "INFO"

    request_timeout_seconds: float = 30.0
    request_user_agent: str = "nowcast-ingestion/0.1"
    retry_attempts: int = 3
    retry_backoff_seconds: float = 1.5
    eia_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("API_EIA_KEY", "EIA_API_KEY", "INGESTION_EIA_API_KEY"),
    )
    exchangerates_cookie: str | None = Field(
        default=None,
        validation_alias=AliasChoices("EXCHANGERATES_COOKIE", "INGESTION_EXCHANGERATES_COOKIE"),
    )
    exchangerates_ajax_nonce: str | None = Field(
        default=None,
        validation_alias=AliasChoices("EXCHANGERATES_AJAX_NONCE", "INGESTION_EXCHANGERATES_AJAX_NONCE"),
    )

    source_config_path: Path | None = None
    csv_export_dir: Path = Path("storage/exports")
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
