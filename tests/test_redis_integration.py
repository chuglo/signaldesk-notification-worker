"""Real Redis 7 coverage using direct, bounded Docker subprocesses only."""

from __future__ import annotations

import subprocess
import time
from datetime import datetime, timezone
from uuid import uuid4

import pytest
import redis
from signaldesk_contracts import NotificationRequestedV1
from signaldesk_streams_kit import (
    ack_if_owned,
    canonical_event_json,
    ensure_consumer_group,
    reclaim_stale_pending,
)

from signaldesk_notification_worker.events import Delivery, NotificationClaim
from signaldesk_notification_worker.worker import NotificationWorker


# The digest is inspected before use: Docker must not contact a registry.
REDIS_IMAGE = "redis@sha256:6ab0b6e7381779332f97b8ca76193e45b0756f38d4c0dcda72dbb3c32061ab99"


@pytest.fixture
def real_redis() -> redis.Redis:
    subprocess.run(["docker", "image", "inspect", REDIS_IMAGE], check=True, capture_output=True, text=True, timeout=15)
    name = f"signaldesk-notification-worker-redis-{uuid4().hex}"
    container = subprocess.run(
        ["docker", "run", "--pull=never", "--rm", "-d", "--name", name, "-p", "127.0.0.1::6379", REDIS_IMAGE],
        check=True, capture_output=True, text=True, timeout=30,
    ).stdout.strip()
    client: redis.Redis | None = None
    try:
        port = int(subprocess.run(["docker", "port", container, "6379/tcp"], check=True, capture_output=True, text=True, timeout=15).stdout.strip().rsplit(":", 1)[1])
        client = redis.Redis(host="127.0.0.1", port=port, decode_responses=True, socket_connect_timeout=1, socket_timeout=1)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                if client.ping():
                    break
            except redis.RedisError:
                time.sleep(0.1)
        else:
            pytest.fail("Redis 7 did not become ready within 10 seconds")
        yield client
    finally:
        if client is not None:
            client.close()
        subprocess.run(["docker", "rm", "-f", container], check=False, capture_output=True, text=True, timeout=20)
        assert not subprocess.run(["docker", "ps", "-aq", "--filter", f"name=^{name}$"], check=True, capture_output=True, text=True, timeout=15).stdout.strip()


def _event(notification_id, organization_id) -> NotificationRequestedV1:
    return NotificationRequestedV1(schema_version=1, event_type="notification.requested.v1", event_id=uuid4(), occurred_at=datetime.now(timezone.utc), correlation_id=uuid4(), organization_id=organization_id, notification_id=notification_id)


def _claim(notification_id, organization_id) -> NotificationClaim:
    monitor_id, run_id, diagnostic_id = uuid4(), uuid4(), uuid4()
    return NotificationClaim.model_validate({
        "id": str(notification_id), "organization_id": str(organization_id), "correlation_id": str(uuid4()), "state": "claimed",
        "monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "requested_by_user_id": str(uuid4()),
        "template_name": "diagnostic_alert", "template_data": {"monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "status": "failed", "outcome": None, "error_code": None},
        "email_delivery_id": None, "failure_code": None, "lease_generation": 1, "lease_expires_at": datetime.now(timezone.utc).isoformat(), "lease_token": "l" * 32,
    })


def test_real_redis_group_consume_reclaim_fenced_ack_dlq_and_replay(real_redis: redis.Redis) -> None:
    stream, group, dlq = "signaldesk:notifications", "notification-workers", "signaldesk:notifications:dlq"
    assert ensure_consumer_group(real_redis, stream, group)
    assert not ensure_consumer_group(real_redis, stream, group)
    notification_id, organization_id = uuid4(), uuid4()
    contract = _event(notification_id, organization_id)
    fields = {"event": canonical_event_json(contract), "event_id": str(contract.event_id)}
    real_redis.xadd(stream, fields)

    class Notifications:
        attached = False
        def claim(self, _notification_id): return None if self.attached else _claim(notification_id, organization_id)
        def attach(self, _claim, _delivery_id): self.attached = True

    class Control:
        calls = 0
        def create_notification_email(self, claim):
            self.calls += 1
            return Delivery(email_delivery_id=uuid4(), notification_id=claim.id, diagnostic_job_id=claim.diagnostic_job_id, organization_id=claim.organization_id, status="pending")

    notifications, control = Notifications(), Control()
    worker = NotificationWorker(notifications, control, redis=real_redis)
    assert worker.run_once("owner") == 1
    assert real_redis.xpending_range(stream, group, "-", "+", 10) == []
    # Response-loss replay is idempotent: the authority reports terminal and
    # the duplicate stream entry is ACKed without another email creation.
    real_redis.xadd(stream, fields)
    worker.run_once("owner")
    assert control.calls == 1

    stale = real_redis.xadd(stream, fields)
    real_redis.xreadgroup(group, "old-owner", {stream: ">"}, count=1)
    time.sleep(0.01)
    reclaimed = reclaim_stale_pending(real_redis, stream, group, "new-owner", min_idle_ms=1)
    assert any(entry.id == stale for entry in reclaimed.entries)
    assert not ack_if_owned(real_redis, stream, group, stale, "old-owner")
    assert ack_if_owned(real_redis, stream, group, stale, "new-owner")

    malformed = real_redis.xadd(stream, {"event": "{not-json", "event_id": "not-a-uuid", "secret": "excluded"})
    worker.run_once("owner")
    assert real_redis.xpending_range(stream, group, "-", "+", 10) == []
    dlq_fields = real_redis.xrange(dlq)[0][1]
    assert dlq_fields["original_message_id"] == malformed and dlq_fields["reason_code"] == "malformed_event"
    assert "event" not in dlq_fields and "secret" not in dlq_fields
