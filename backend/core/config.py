"""Application settings loaded from environment variables with fail-fast validation."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Strongly typed settings; required variables must be present or startup fails."""

    model_config = SettingsConfigDict(
        env_file=(".env", "../.env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        ...,
        description="Async PostgreSQL connection string (postgresql://...)",
    )
    ollama_host: str | None = Field(
        default=None,
        validation_alias="OLLAMA_HOST",
        description="Base URL for Ollama (e.g. http://localhost:11434). Optional for CI.",
    )
    fred_api_key: str | None = Field(
        default=None,
        validation_alias="FRED_API_KEY",
        description="Optional FRED API key for economics sources.",
    )
    api_host: str = Field(default="0.0.0.0", validation_alias="API_HOST")
    api_port: int = Field(default=8000, validation_alias="API_PORT")


@lru_cache
def get_settings() -> Settings:
    """Return a cached Settings instance (call once per process)."""
    return Settings()


def clear_settings_cache() -> None:
    """Clear the settings cache (useful in tests)."""
    get_settings.cache_clear()
