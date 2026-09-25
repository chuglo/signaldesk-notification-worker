from __future__ import annotations

from typing import Any
from uuid import UUID
import httpx
from .events import Delivery, NotificationClaim

ACTOR = "notification-worker"


class ImpossibleStateError(ValueError):
    """An authority confirmed that an event can never be completed."""


class TerminalNotificationFailure(Exception):
    """A finite, authoritative reason to terminally fail a notification."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _headers(credential: str) -> dict[str, str]:
    return {"X-SignalDesk-Service-Actor": ACTOR, "X-SignalDesk-Service-Credential": credential}


class NotificationClient:
    def __init__(self, base_url: str, credential: str, *, client: httpx.Client | None = None, timeout: httpx.Timeout | None = None):
        self.client = client or httpx.Client(base_url=base_url, timeout=timeout or httpx.Timeout(5.0, connect=2.0))
        self.headers = _headers(credential)

    def claim(self, notification_id: UUID, lease_seconds: int = 60) -> NotificationClaim | None:
        response = self.client.post(f"/internal/notifications/{notification_id}/claim", headers=self.headers, json={"lease_seconds": lease_seconds})
        if response.status_code == 404:
            raise ImpossibleStateError("notification not found")
        if response.status_code == 204:
            return None
        response.raise_for_status()
        payload = response.json()
        if payload.get("state") in {"email_attached", "failed"}:
            return None
        result = NotificationClaim.model_validate(payload)
        if result.id != notification_id or result.state != "claimed":
            if result.state == "email_attached":
                return None
            raise ValueError("impossible notification claim state")
        return result

    claim_notification = claim

    def attach(self, claim: NotificationClaim, delivery_id: UUID) -> None:
        response = self.client.post(f"/internal/notifications/{claim.id}/email", headers=self.headers, json={"lease_token": claim.lease_token, "lease_generation": claim.lease_generation, "email_delivery_id": str(delivery_id)})
        response.raise_for_status()

    attach_email = attach

    def fail(self, claim: NotificationClaim, code: str) -> None:
        response = self.client.post(f"/internal/notifications/{claim.id}/failed", headers=self.headers, json={"lease_token": claim.lease_token, "lease_generation": claim.lease_generation, "failure_code": code})
        response.raise_for_status()

    mark_failed = fail


class ControlClient:
    def __init__(self, base_url: str, credential: str, *, client: httpx.Client | None = None, timeout: httpx.Timeout | None = None):
        self.client = client or httpx.Client(base_url=base_url, timeout=timeout or httpx.Timeout(5.0, connect=2.0))
        self.headers = _headers(credential)

    def create_notification_email(self, claim: NotificationClaim) -> Delivery:
        data = claim.template_data
        body = {"notification_id": str(claim.id), "diagnostic_job_id": str(claim.diagnostic_job_id), "monitor_id": str(claim.monitor_id), "monitor_run_id": str(claim.monitor_run_id), "status": data.status, "outcome": data.outcome, "error_code": data.error_code}
        response = self.client.post("/internal/notification-worker/notification-emails", headers={**self.headers, "Idempotency-Key": f"notification:{claim.id}"}, json=body)
        # Only finite outcomes specified by the authoritative control route
        # become terminal. All other errors remain safely replayable.
        if response.status_code == 409:
            try:
                detail = response.json().get("detail")
            except (TypeError, ValueError):
                detail = None
            if detail == "Authoritative recipient unavailable":
                raise TerminalNotificationFailure("member_removed")
            if detail in {"Diagnostic terminal data mismatch", "Reachable diagnostic is not an alert", "Notification idempotency conflict"}:
                raise ImpossibleStateError("control notification scope conflict")
        if response.status_code == 404:
            raise ImpossibleStateError("authoritative diagnostic not found")
        response.raise_for_status()
        result = Delivery.model_validate_json(response.content)
        if result.notification_id != claim.id or result.organization_id != claim.organization_id or result.diagnostic_job_id != claim.diagnostic_job_id:
            raise ValueError("notification delivery scope conflict")
        return result

    create_email_delivery = create_notification_email
