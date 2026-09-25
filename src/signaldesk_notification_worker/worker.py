from __future__ import annotations

import logging
import time
from collections.abc import Callable
from uuid import UUID
import httpx
from pydantic import ValidationError
from signaldesk_contracts import NotificationRequestedV1
from signaldesk_streams_kit import ack_if_owned, dead_letter_if_owned, DeadLetterReason, parse_stream_event
from .clients import ControlClient, ImpossibleStateError, NotificationClient, TerminalNotificationFailure
from .events import NotificationEvent

log = logging.getLogger("signaldesk_notification_worker")


class NotificationWorker:
    def __init__(self, notifications: NotificationClient, control: ControlClient, *, redis: object | None = None, stream: str = "signaldesk:notifications", group: str = "notification-workers", dlq: str = "signaldesk:notifications:dlq", sleep: Callable[[float], None] = time.sleep, retry_backoff_seconds: float = 0.25):
        self.notifications, self.control, self.redis = notifications, control, redis
        self.stream, self.group, self.dlq = stream, group, dlq
        self.sleep, self.retry_backoff_seconds = sleep, retry_backoff_seconds

    def process(self, event: NotificationEvent | NotificationRequestedV1, message_id: str, consumer: str, *, raw: dict[str, str] | None = None) -> bool:
        safe = event if isinstance(event, NotificationEvent) else NotificationEvent.from_contract(event)
        claim = None
        try:
            claim = self.notifications.claim(safe.notification_id)
            if claim is None:
                return self._ack(message_id, consumer)
            if claim.organization_id != safe.organization_id:
                return self._dlq(message_id, consumer, DeadLetterReason.TENANT_MISMATCH, raw or {})
            delivery = None
            for attempt in range(2):
                try:
                    delivery = self.control.create_notification_email(claim)
                    break
                except httpx.HTTPError:
                    if attempt == 0:
                        self.sleep(self.retry_backoff_seconds)
                    else:
                        raise
            if delivery is None:
                return False
            for attempt in range(2):
                try:
                    self.notifications.attach(claim, delivery.email_delivery_id)
                    break
                except httpx.TransportError:
                    if attempt == 0:
                        self.sleep(self.retry_backoff_seconds)
                    else:
                        raise
            return self._ack(message_id, consumer)
        except TerminalNotificationFailure as error:
            if claim is None:
                return self._dlq(message_id, consumer, DeadLetterReason.IMPOSSIBLE_STATE, raw or {})
            try:
                self.notifications.fail(claim, error.code)
                return self._ack(message_id, consumer)
            except httpx.HTTPError:
                return False
        except ImpossibleStateError:
            return self._dlq(message_id, consumer, DeadLetterReason.IMPOSSIBLE_STATE, raw or {})
        except httpx.HTTPStatusError as error:
            # A claim returning 404 is impossible. Other HTTP errors can be
            # lease/retry races and must remain pending for fenced replay.
            if claim is None and error.response.status_code == 404:
                return self._dlq(message_id, consumer, DeadLetterReason.IMPOSSIBLE_STATE, raw or {})
            # The fixed control request is composed solely from an already
            # validated claim. A 422 therefore identifies an unrecoverable
            # template-contract failure, never a delivery retry.
            if claim is not None and error.response.status_code == 422:
                try:
                    self.notifications.fail(claim, "invalid_template")
                    return self._ack(message_id, consumer)
                except httpx.HTTPError:
                    return False
            log.warning("notification_id=%s outcome=transient", safe.notification_id)
            return False
        except (httpx.HTTPError, TimeoutError):
            log.warning("notification_id=%s outcome=transient", safe.notification_id)
            return False
        except (ValueError, KeyError, TypeError):
            return self._dlq(message_id, consumer, DeadLetterReason.IMPOSSIBLE_STATE, raw or {})

    def _ack(self, message_id: str, consumer: str) -> bool:
        if self.redis is None: return True
        return ack_if_owned(self.redis, self.stream, self.group, message_id, consumer)

    def _dlq(self, message_id: str, consumer: str, reason: DeadLetterReason, raw: dict[str, str]) -> bool:
        if self.redis is None: return True
        return dead_letter_if_owned(self.redis, self.stream, self.group, message_id, consumer, self.dlq, reason, raw)

    def run_once(self, consumer: str, *, count: int = 1, block_ms: int = 1000) -> int:
        """Process at most ``count`` new entries; transport errors remain pending."""
        if self.redis is None:
            raise RuntimeError("redis is required for polling")
        records = self.redis.xreadgroup(self.group, consumer, {self.stream: ">"}, count=count, block=block_ms)
        processed = 0
        for _, entries in records:
            for message_id, fields in entries:
                processed += 1
                try:
                    event = parse_stream_event(fields)
                except (ValidationError, UnicodeDecodeError, ValueError, TypeError):
                    self._dlq(message_id, consumer, DeadLetterReason.MALFORMED_EVENT, fields)
                    continue
                if event.event_type != "notification.requested.v1":
                    self._dlq(message_id, consumer, DeadLetterReason.UNSUPPORTED_EVENT, fields)
                    continue
                try:
                    self.process(event, message_id, consumer, raw=fields)
                except Exception:
                    # A valid pending event must not become malformed because
                    # of a local defect; retain it for fenced replay/reclaim.
                    log.exception("notification_id=%s outcome=processing_error", event.notification_id)
        return processed
