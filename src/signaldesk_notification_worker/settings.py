from __future__ import annotations
import secrets
from pydantic import AnyHttpUrl, RedisDsn, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SIGNALDESK_NOTIFICATION_WORKER_", extra="forbid", strict=True)
    notification_api_url: AnyHttpUrl
    control_api_url: AnyHttpUrl
    notification_api_credential: SecretStr
    control_api_credential: SecretStr
    redis_url: RedisDsn
    poll_interval_seconds: float = 5.0
    reclaim_idle_seconds: float = 60.0
    request_connect_timeout_seconds: float = 2.0
    request_read_timeout_seconds: float = 5.0
    request_write_timeout_seconds: float = 5.0
    request_pool_timeout_seconds: float = 2.0

    @field_validator("notification_api_credential", "control_api_credential")
    @classmethod
    def credential(cls, value: SecretStr) -> SecretStr:
        raw = value.get_secret_value()
        if len(raw) < 32 or not raw.isascii() or any(char.isspace() for char in raw): raise ValueError("invalid service credential")
        return value

    @model_validator(mode="after")
    def bounded(self) -> "Settings":
        if secrets.compare_digest(self.notification_api_credential.get_secret_value(), self.control_api_credential.get_secret_value()): raise ValueError("service credentials must be distinct")
        values = (self.poll_interval_seconds, self.reclaim_idle_seconds, self.request_connect_timeout_seconds, self.request_read_timeout_seconds, self.request_write_timeout_seconds, self.request_pool_timeout_seconds)
        if any(value <= 0 or value > 300 for value in values): raise ValueError("settings must be positive and bounded")
        return self
