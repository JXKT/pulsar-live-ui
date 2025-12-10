from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class AppSettings(BaseSettings):
    """
    Configuration loaded from environment variables and .env file.
    """
    broker_service_url: str = Field(
        default="pulsar://localhost:6650",
        alias="PULSAR_BROKER_URL"
    )
    admin_service_url: str = Field(
        default="http://localhost:8080",
        alias="PULSAR_ADMIN_URL"
    )
    token: str | None = Field(
        default=None,
        alias="PULSAR_TOKEN"
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )