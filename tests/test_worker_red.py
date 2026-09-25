import json
from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest
import redis
from pydantic import RedisDsn, ValidationError
from types import SimpleNamespace

from signaldesk_notification_worker.clients import ControlClient, NotificationClient
from signaldesk_notification_worker import cli
from signaldesk_notification_worker.events import NotificationEvent
from signaldesk_notification_worker.events import NotificationClaim
from signaldesk_notification_worker.worker import NotificationWorker
from signaldesk_notification_worker import health


def test_cli_constructs_bounded_redis_from_redis_dsn_and_closes_owned_resources(monkeypatch) -> None:
    settings = type("Settings", (), {
        "notification_api_url": "https://notifications.test",
        "control_api_url": "https://control.test",
        "redis_url": RedisDsn("redis://redis.test/0"),
        "request_connect_timeout_seconds": 1.0,
        "request_read_timeout_seconds": 1.0,
        "request_write_timeout_seconds": 1.0,
        "request_pool_timeout_seconds": 1.0,
        "notification_api_credential": type("Secret", (), {"get_secret_value": lambda self: "n" * 32})(),
        "control_api_credential": type("Secret", (), {"get_secret_value": lambda self: "c" * 32})(),
        "reclaim_idle_seconds": 1.0,
    })()
    seen: list[tuple[str, dict[str, object]]] = []
    closed: list[str] = []

    class Redis:
        def xreadgroup(self, *_args, **_kwargs): return []
        def close(self): closed.append("redis")

    class Http:
        def close(self): closed.append("http")

    class Service:
        def __init__(self, *_args, **_kwargs): self.client = Http()

    def from_url(url, **kwargs):
        seen.append((url, kwargs))
        return Redis()

    monkeypatch.setattr(cli, "Settings", lambda: settings)
    monkeypatch.setattr(cli.redis.Redis, "from_url", from_url)
    monkeypatch.setattr(cli, "ensure_consumer_group", lambda *_args: None)
    monkeypatch.setattr(cli, "reclaim_stale_pending", lambda *_args, **_kwargs: SimpleNamespace(entries=[]))
    monkeypatch.setattr(cli, "NotificationClient", Service)
    monkeypatch.setattr(cli, "ControlClient", Service)

    assert cli.main(["--once"]) == 0
    assert seen == [("redis://redis.test:6379/0", {"decode_responses": True, "socket_connect_timeout": 1.0, "socket_timeout": 1.0})]
    assert closed == ["http", "http", "redis"]


def test_cli_closes_owned_resources_when_redis_processing_fails(monkeypatch) -> None:
    settings = type("Settings", (), {
        "notification_api_url": "https://notifications.test", "control_api_url": "https://control.test",
        "redis_url": RedisDsn("redis://redis.test/0"), "request_connect_timeout_seconds": 1.0,
        "request_read_timeout_seconds": 1.0, "request_write_timeout_seconds": 1.0,
        "request_pool_timeout_seconds": 1.0, "notification_api_credential": type("Secret", (), {"get_secret_value": lambda self: "n" * 32})(),
        "control_api_credential": type("Secret", (), {"get_secret_value": lambda self: "c" * 32})(),
        "reclaim_idle_seconds": 1.0,
    })()
    closed: list[str] = []

    class Redis:
        def xreadgroup(self, *_args, **_kwargs): raise redis.ConnectionError("blackholed")
        def close(self): closed.append("redis")

    class Http:
        def close(self): closed.append("http")

    class Service:
        def __init__(self, *_args, **_kwargs): self.client = Http()

    monkeypatch.setattr(cli, "Settings", lambda: settings)
    monkeypatch.setattr(cli.redis.Redis, "from_url", lambda *_args, **_kwargs: Redis())
    monkeypatch.setattr(cli, "ensure_consumer_group", lambda *_args: None)
    monkeypatch.setattr(cli, "reclaim_stale_pending", lambda *_args, **_kwargs: SimpleNamespace(entries=[]))
    monkeypatch.setattr(cli, "NotificationClient", Service)
    monkeypatch.setattr(cli, "ControlClient", Service)

    with pytest.raises(redis.ConnectionError):
        cli.main(["--once"])
    assert closed == ["http", "http", "redis"]


def test_success_claims_creates_authoritative_delivery_attaches_and_acknowledges():
    notification_id, organization_id = uuid4(), uuid4()
    monitor_id, run_id, diagnostic_id = uuid4(), uuid4(), uuid4()
    seen: list[httpx.Request] = []
    delivery_id = uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "notifications.test":
            if request.url.path.endswith("/claim"):
                return httpx.Response(200, json={
                    "id": str(notification_id), "organization_id": str(organization_id),
                    "correlation_id": str(uuid4()), "state": "claimed",
                    "monitor_id": str(monitor_id), "monitor_run_id": str(run_id),
                    "diagnostic_job_id": str(diagnostic_id), "requested_by_user_id": str(uuid4()),
                    "template_name": "diagnostic_alert", "template_data": {
                        "monitor_id": str(monitor_id), "monitor_run_id": str(run_id),
                        "diagnostic_job_id": str(diagnostic_id), "status": "failed", "outcome": None,
                        "error_code": "timeout"}, "email_delivery_id": None, "failure_code": None,
                    "lease_generation": 1, "lease_expires_at": datetime.now(timezone.utc).isoformat(),
                    "lease_token": "l" * 32})
            return httpx.Response(200, json={})
        return httpx.Response(201, json={"email_delivery_id": str(delivery_id),
                "notification_id": str(notification_id), "diagnostic_job_id": str(diagnostic_id),
            "organization_id": str(organization_id), "status": "pending"})

    transport = httpx.MockTransport(handler)
    notifications = httpx.Client(transport=transport, base_url="https://notifications.test")
    control = httpx.Client(transport=transport, base_url="https://control.test")
    redis = _Redis()
    event = NotificationEvent(notification_id=notification_id, organization_id=organization_id)
    worker = NotificationWorker(NotificationClient("https://notifications.test", "n" * 32, client=notifications), ControlClient("https://control.test", "c" * 32, client=control), redis=redis)
    assert worker.process(event, "1-0", "consumer") is True
    assert any(r.url.host == "control.test" and r.headers.get("Idempotency-Key") == f"notification:{notification_id}" for r in seen), [(str(r.url), dict(r.headers)) for r in seen]
    assert any(r.url.host == "notifications.test" and r.url.path.endswith("/email") for r in seen)


def test_control_client_accepts_canonical_uuid_strings_from_created_delivery_response() -> None:
    """The control API's 201 JSON response is a string-UUID wire contract."""
    notification_id, organization_id = uuid4(), uuid4()
    monitor_id, run_id, diagnostic_id, delivery_id = uuid4(), uuid4(), uuid4(), uuid4()
    claim = NotificationClaim.model_validate({
        "id": str(notification_id), "organization_id": str(organization_id), "correlation_id": str(uuid4()), "state": "claimed",
        "monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "requested_by_user_id": str(uuid4()),
        "template_name": "diagnostic_alert", "template_data": {"monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "status": "failed", "outcome": None, "error_code": "timeout"},
        "email_delivery_id": None, "failure_code": None, "lease_generation": 1, "lease_expires_at": datetime.now(timezone.utc).isoformat(), "lease_token": "l" * 32,
    })

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        assert request.url.path == "/internal/notification-worker/notification-emails"
        body = json.loads(request.content)
        assert set(body) == {"notification_id", "diagnostic_job_id", "monitor_id", "monitor_run_id", "status", "outcome", "error_code"}
        assert "recipient" not in body
        return httpx.Response(201, json={
            "email_delivery_id": str(delivery_id), "notification_id": str(notification_id),
            "diagnostic_job_id": str(diagnostic_id), "organization_id": str(organization_id), "status": "pending",
        })

    client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://control.test")
    delivery = ControlClient("https://control.test", "c" * 32, client=client).create_notification_email(claim)
    assert delivery.email_delivery_id == delivery_id
    assert delivery.notification_id == notification_id
    assert delivery.diagnostic_job_id == diagnostic_id
    assert delivery.organization_id == organization_id


@pytest.mark.parametrize("field,value", [
    ("email_delivery_id", "123456781234123412341234567890ab"),
    ("notification_id", "not-a-uuid"),
    ("unexpected", "value"),
])
def test_control_client_rejects_noncanonical_or_extra_delivery_response_fields(field: str, value: str) -> None:
    """Control response parsing stays closed and canonical even when JSON-aware."""
    notification_id, organization_id = uuid4(), uuid4()
    monitor_id, run_id, diagnostic_id, delivery_id = uuid4(), uuid4(), uuid4(), uuid4()
    claim = NotificationClaim.model_validate({
        "id": str(notification_id), "organization_id": str(organization_id), "correlation_id": str(uuid4()), "state": "claimed",
        "monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "requested_by_user_id": str(uuid4()),
        "template_name": "diagnostic_alert", "template_data": {"monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "status": "failed", "outcome": None, "error_code": "timeout"},
        "email_delivery_id": None, "failure_code": None, "lease_generation": 1, "lease_expires_at": datetime.now(timezone.utc).isoformat(), "lease_token": "l" * 32,
    })
    response = {
        "email_delivery_id": str(delivery_id), "notification_id": str(notification_id),
        "diagnostic_job_id": str(diagnostic_id), "organization_id": str(organization_id), "status": "pending",
    }
    response[field] = value

    client = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(201, json=response)), base_url="https://control.test")
    with pytest.raises(ValidationError):
        ControlClient("https://control.test", "c" * 32, client=client).create_notification_email(claim)


def test_invalid_control_delivery_is_dlqd_without_raw_payload_or_attachment() -> None:
    notification_id, organization_id = uuid4(), uuid4()
    monitor_id, run_id, diagnostic_id = uuid4(), uuid4(), uuid4()
    claim = NotificationClaim.model_validate({
        "id": str(notification_id), "organization_id": str(organization_id), "correlation_id": str(uuid4()), "state": "claimed",
        "monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "requested_by_user_id": str(uuid4()),
        "template_name": "diagnostic_alert", "template_data": {"monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "status": "failed", "outcome": None, "error_code": "timeout"},
        "email_delivery_id": None, "failure_code": None, "lease_generation": 1, "lease_expires_at": datetime.now(timezone.utc).isoformat(), "lease_token": "l" * 32,
    })

    class Notifications:
        attached = False

        def claim(self, _notification_id): return claim
        def attach(self, _claim, _delivery_id): self.attached = True

    class Redis:
        arguments: tuple[object, ...]

        def eval(self, _script, *_args):
            self.arguments = _args
            return [1, "dead_lettered"]

    control_http = httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(201, json={
        "email_delivery_id": str(uuid4()), "notification_id": str(notification_id),
        "diagnostic_job_id": str(diagnostic_id), "organization_id": str(organization_id),
        "status": "pending", "recipient": "must-not-leak@example.test",
    })), base_url="https://control.test")
    notifications, redis = Notifications(), Redis()
    worker = NotificationWorker(notifications, ControlClient("https://control.test", "c" * 32, client=control_http), redis=redis)

    assert worker.process(NotificationEvent(notification_id=notification_id, organization_id=organization_id), "1-0", "consumer", raw={"secret": "must-not-leak@example.test"})
    assert not notifications.attached
    assert "impossible_state" in redis.arguments
    assert "must-not-leak@example.test" not in redis.arguments


class _Redis:
    def eval(self, *_args): return [1, "acknowledged"]


def test_missing_notification_is_an_impossible_event_and_is_dlqd() -> None:
    notification_id, organization_id = uuid4(), uuid4()

    class Notifications:
        def claim(self, _notification_id):
            request = httpx.Request("POST", "https://notifications.test/claim")
            response = httpx.Response(404, request=request)
            raise httpx.HTTPStatusError("not found", request=request, response=response)

    class Control:
        def create_notification_email(self, _claim):
            raise AssertionError("control must not be called")

    worker = NotificationWorker(Notifications(), Control(), redis=_DlqRedis())
    assert worker.process(NotificationEvent(notification_id=notification_id, organization_id=organization_id), "1-0", "consumer", raw={})
    assert worker.redis.reason == "impossible_state"


def test_redis_ack_error_never_turns_a_completed_event_into_a_malformed_dlq() -> None:
    notification_id, organization_id = uuid4(), uuid4()

    class Notifications:
        def claim(self, _notification_id):
            return None

    class Control:
        pass

    class RedisFailure:
        calls = 0

        def xreadgroup(self, *_args, **_kwargs):
            return [("signaldesk:notifications", [("1-0", {"event": "bad", "event_id": "also-bad"})])]

        def eval(self, *_args):
            self.calls += 1
            raise __import__("redis").ConnectionError("unavailable")

    # A malformed payload does request the DLQ, but a failure of that fenced
    # operation must propagate as Redis availability failure rather than retry
    # a second, misclassified DLQ operation.
    worker = NotificationWorker(Notifications(), Control(), redis=RedisFailure())
    with pytest.raises(__import__("redis").ConnectionError):
        worker.run_once("consumer")
    assert worker.redis.calls == 1


def test_unexpected_processing_error_is_not_misclassified_as_malformed_event() -> None:
    event = NotificationEvent(notification_id=uuid4(), organization_id=uuid4())

    class Notifications:
        def claim(self, _notification_id):
            raise RuntimeError("local programming fault")

    class Control:
        pass

    class Redis:
        dlq_calls = 0

        def xreadgroup(self, *_args, **_kwargs):
            from signaldesk_contracts import NotificationRequestedV1
            from signaldesk_streams_kit import canonical_event_json
            from datetime import datetime, timezone

            contract = NotificationRequestedV1(schema_version=1, event_type="notification.requested.v1", event_id=uuid4(), occurred_at=datetime.now(timezone.utc), correlation_id=uuid4(), organization_id=event.organization_id, notification_id=event.notification_id)
            return [("signaldesk:notifications", [("1-0", {"event": canonical_event_json(contract), "event_id": str(contract.event_id)})])]

        def eval(self, script, *_args):
            if "XADD" in script:
                self.dlq_calls += 1
            return [1, "acknowledged"]

    worker = NotificationWorker(Notifications(), Control(), redis=Redis())
    worker.run_once("consumer")
    assert worker.redis.dlq_calls == 0


class _DlqRedis:
    reason: str | None = None

    def eval(self, script, _keys, *args):
        if "XADD" in script:
            self.reason = next(value for index, value in enumerate(args) if args[index - 1] == "reason_code")
        return [1, "dead_lettered"]


def test_readiness_requires_redis_as_well_as_both_authorities(monkeypatch) -> None:
    class Http:
        def __enter__(self): return self
        def __exit__(self, *_args): pass
        def close(self): pass
        def get(self, _url): return httpx.Response(200)

    class Redis:
        def ping(self): raise __import__("redis").ConnectionError("down")
        def close(self): pass

    monkeypatch.setattr(health.httpx, "Client", lambda **_kwargs: Http())
    monkeypatch.setattr(health.redis.Redis, "from_url", lambda *_args, **_kwargs: Redis())
    assert not health.readiness("https://notifications.test", "https://control.test", redis_url="redis://redis.test/0", timeout=httpx.Timeout(1))


def test_readiness_constructs_bounded_redis_with_string_dsn_and_closes_resources(monkeypatch) -> None:
    closed: list[str] = []
    seen: list[tuple[str, dict[str, object]]] = []

    class Http:
        def __enter__(self): return self
        def __exit__(self, *_args): closed.append("http")
        def close(self): closed.append("http")
        def get(self, _url): return httpx.Response(200)

    class Redis:
        def ping(self): return True
        def close(self): closed.append("redis")

    def from_url(url, **kwargs):
        seen.append((url, kwargs))
        return Redis()

    monkeypatch.setattr(health.httpx, "Client", lambda **_kwargs: Http())
    monkeypatch.setattr(health.redis.Redis, "from_url", from_url)

    assert health.readiness("https://notifications.test", "https://control.test", redis_url=RedisDsn("redis://redis.test/0"), timeout=httpx.Timeout(connect=2.0, read=3.0, write=4.0, pool=5.0))
    assert seen == [("redis://redis.test:6379/0", {"socket_connect_timeout": 2.0, "socket_timeout": 3.0})]
    assert closed == ["redis", "http"]


def test_readiness_closes_resources_when_redis_ping_fails(monkeypatch) -> None:
    closed: list[str] = []

    class Http:
        def __enter__(self): return self
        def __exit__(self, *_args): closed.append("http")
        def close(self): closed.append("http")
        def get(self, _url): return httpx.Response(200)

    class Redis:
        def ping(self): raise redis.ConnectionError("blackholed")
        def close(self): closed.append("redis")

    monkeypatch.setattr(health.httpx, "Client", lambda **_kwargs: Http())
    monkeypatch.setattr(health.redis.Redis, "from_url", lambda *_args, **_kwargs: Redis())

    assert not health.readiness("https://notifications.test", "https://control.test", redis_url="redis://redis.test/0", timeout=httpx.Timeout(1))
    assert closed == ["redis", "http"]


def test_authoritative_invalid_template_marks_the_claim_failed_before_ack() -> None:
    notification_id, organization_id = uuid4(), uuid4()
    monitor_id, run_id, diagnostic_id = uuid4(), uuid4(), uuid4()
    claim = NotificationClaim.model_validate({
        "id": str(notification_id), "organization_id": str(organization_id), "correlation_id": str(uuid4()), "state": "claimed",
        "monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "requested_by_user_id": str(uuid4()),
        "template_name": "diagnostic_alert", "template_data": {"monitor_id": str(monitor_id), "monitor_run_id": str(run_id), "diagnostic_job_id": str(diagnostic_id), "status": "failed", "outcome": None, "error_code": None},
        "email_delivery_id": None, "failure_code": None, "lease_generation": 1, "lease_expires_at": datetime.now(timezone.utc).isoformat(), "lease_token": "l" * 32,
    })

    class Notifications:
        failure_code = None
        def claim(self, _notification_id): return claim
        def fail(self, _claim, code): self.failure_code = code

    class Control:
        def create_notification_email(self, _claim):
            request = httpx.Request("POST", "https://control.test/notification-emails")
            response = httpx.Response(422, request=request)
            raise httpx.HTTPStatusError("unprocessable", request=request, response=response)

    notifications = Notifications()
    worker = NotificationWorker(notifications, Control(), redis=_Redis())
    assert worker.process(NotificationEvent(notification_id=notification_id, organization_id=organization_id), "1-0", "consumer")
    assert notifications.failure_code == "invalid_template"
