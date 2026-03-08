from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration loaded from environment and .env file."""

    database_url: str = Field(alias="DATABASE_URL")
    redis_url: str = Field(alias="REDIS_URL")

    provider_a_api_key: str = Field(alias="PROVIDER_A_API_KEY")
    provider_b_api_key: str = Field(alias="PROVIDER_B_API_KEY")

    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    env: str = Field(default="development", alias="ENV")

    cache_ttl_seconds: int = Field(default=60, alias="CACHE_TTL_SECONDS")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings instance."""

    return Settings()

