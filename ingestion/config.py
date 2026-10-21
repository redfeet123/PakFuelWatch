"""Configuration management for PakFuelWatch ingestion and pipeline layers."""

from functools import lru_cache
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables or .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # MinIO / S3 Configuration
    minio_endpoint: str = Field(
        default="http://localhost:9000",
        description="Endpoint URL for MinIO S3 object storage",
    )
    minio_access_key: str = Field(
        default="minioadmin",
        description="Access key for MinIO S3 storage",
    )
    minio_secret_key: str = Field(
        default="minioadmin",
        description="Secret key for MinIO S3 storage",
    )

    # External APIs
    eia_api_key: Optional[str] = Field(
        default=None,
        description="API key for US Energy Information Administration (EIA)",
    )
    fred_api_key: Optional[str] = Field(
        default=None,
        description="API key for Federal Reserve Economic Data (FRED)",
    )
    sbp_api_key: Optional[str] = Field(
        default=None,
        description="API key for State Bank of Pakistan (SBP) data portal",
    )

    # Telegram Bot Alerts
    telegram_bot_token: Optional[str] = Field(
        default=None,
        description="Telegram bot token for notification alerts",
    )
    telegram_chat_id: Optional[str] = Field(
        default=None,
        description="Telegram chat ID for notification destination",
    )

    # PostgreSQL Database (Dagster metadata storage)
    postgres_user: str = Field(
        default="postgres",
        description="PostgreSQL username",
    )
    postgres_password: str = Field(
        default="postgres",
        description="PostgreSQL password",
    )
    postgres_db: str = Field(
        default="pakfuelwatch",
        description="PostgreSQL database name",
    )
    postgres_host: str = Field(
        default="localhost",
        description="PostgreSQL host address",
    )
    postgres_port: int = Field(
        default=5432,
        description="PostgreSQL port number",
    )

    @property
    def postgres_connection_string(self) -> str:
        """Construct PostgreSQL SQLAlchemy connection URI."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton instance of Settings."""
    return Settings()
