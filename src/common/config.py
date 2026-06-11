from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AMS_", env_file=".env", extra="ignore")

    broker: str = "memory"
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    database_url: str = "postgresql+asyncpg://ams:ams@localhost:5432/ams"

    validate_duration: float = 0.05
    process_duration: float = 0.40
    finalize_duration: float = 0.05

    process_fail_rate: float = 0.0

    max_attempts: int = 5
    retry_base_delay: float = 0.1

    gateway_port: int = 8000
    query_port: int = 8001
    log_level: str = "INFO"


def load_settings() -> Settings:
    return Settings()
