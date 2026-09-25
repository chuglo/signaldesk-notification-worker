from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field, field_validator


class NotificationEvent(BaseModel):
    """The safe identifier subset used by this worker's transport boundary."""
    model_config = ConfigDict(extra="forbid", strict=True)
    notification_id: UUID
    organization_id: UUID

    @classmethod
    def from_contract(cls, event: object) -> "NotificationEvent":
        return cls.model_validate({"notification_id": event.notification_id, "organization_id": event.organization_id})


class NotificationClaim(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: UUID = Field(strict=False)
    organization_id: UUID = Field(strict=False)
    correlation_id: UUID = Field(strict=False)
    state: Literal["claimed"]
    monitor_id: UUID = Field(strict=False)
    monitor_run_id: UUID = Field(strict=False)
    diagnostic_job_id: UUID = Field(strict=False)
    requested_by_user_id: UUID = Field(strict=False)
    template_name: Literal["diagnostic_alert"]
    template_data: "TemplateData"
    email_delivery_id: UUID | None = Field(default=None, strict=False)
    failure_code: str | None = None
    lease_generation: int = Field(ge=1)
    lease_expires_at: datetime = Field(strict=False)
    lease_token: str = Field(min_length=32)


class TemplateData(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    monitor_id: UUID = Field(strict=False)
    monitor_run_id: UUID = Field(strict=False)
    diagnostic_job_id: UUID = Field(strict=False)
    status: Literal["completed", "failed"]
    outcome: Literal["reachable", "error", "blocked"] | None = None
    error_code: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")


class Delivery(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    email_delivery_id: UUID
    notification_id: UUID
    diagnostic_job_id: UUID
    organization_id: UUID
    status: str

    @field_validator("email_delivery_id", "notification_id", "diagnostic_job_id", "organization_id", mode="before")
    @classmethod
    def canonical_uuid_wire_value(cls, value: object) -> object:
        """Permit only canonical UUID strings at the control API JSON boundary."""
        if isinstance(value, UUID):
            return value
        if not isinstance(value, str):
            raise ValueError("UUID wire value must be a canonical UUID string")
        try:
            parsed = UUID(value)
        except ValueError as error:
            raise ValueError("UUID wire value must be a canonical UUID string") from error
        if str(parsed) != value:
            raise ValueError("UUID wire value must be a canonical UUID string")
        return value
