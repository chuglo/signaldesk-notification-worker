from __future__ import annotations
import argparse
import logging
import signal
from uuid import UUID
import httpx
import redis
from pydantic import ValidationError
from signaldesk_service_kit import bounded_timeout
from signaldesk_streams_kit import ensure_consumer_group, parse_stream_event, process_consumer_name, reclaim_stale_pending
from .clients import ControlClient, NotificationClient
from .health import readiness
from .settings import Settings
from .worker import NotificationWorker

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--ready", action="store_true")
    args = parser.parse_args(argv)
    try: settings = Settings()
    except Exception: return 2
    timeout = bounded_timeout(connect=settings.request_connect_timeout_seconds, read=settings.request_read_timeout_seconds, write=settings.request_write_timeout_seconds, pool=settings.request_pool_timeout_seconds)
    if args.ready: return 0 if readiness(str(settings.notification_api_url), str(settings.control_api_url), redis_url=str(settings.redis_url), timeout=timeout) else 1
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    client = redis.Redis.from_url(
        str(settings.redis_url),
        decode_responses=True,
        socket_connect_timeout=settings.request_connect_timeout_seconds,
        socket_timeout=settings.request_read_timeout_seconds,
    )
    consumer = process_consumer_name("notification-worker")
    stream, group, dlq = "signaldesk:notifications", "notification-workers", "signaldesk:notifications:dlq"
    notifications = None
    control = None
    try:
        notifications = NotificationClient(str(settings.notification_api_url), settings.notification_api_credential.get_secret_value(), timeout=timeout)
        control = ControlClient(str(settings.control_api_url), settings.control_api_credential.get_secret_value(), timeout=timeout)
        ensure_consumer_group(client, stream, group)
        worker = NotificationWorker(notifications, control, redis=client)
        stopping = False
        def stop(_signum, _frame):
            nonlocal stopping; stopping = True
        signal.signal(signal.SIGTERM, stop); signal.signal(signal.SIGINT, stop)
        def once() -> None:
            reclaimed = reclaim_stale_pending(client, stream, group, consumer, min_idle_ms=int(settings.reclaim_idle_seconds * 1000), count=50)
            records = [(entry.id, entry.fields) for entry in reclaimed.entries]
            if not records:
                records = client.xreadgroup(group, consumer, {stream: ">"}, count=1, block=1_000)
                records = [(mid, fields) for _, items in records for mid, fields in items]
            for message_id, fields in records:
                try:
                    event = parse_stream_event(fields)
                except (ValidationError, UnicodeDecodeError, ValueError, TypeError):
                    worker._dlq(message_id, consumer, __import__("signaldesk_streams_kit").DeadLetterReason.MALFORMED_EVENT, fields)
                    continue
                if event.event_type != "notification.requested.v1":
                    worker._dlq(message_id, consumer, __import__("signaldesk_streams_kit").DeadLetterReason.UNSUPPORTED_EVENT, fields)
                    continue
                try:
                    worker.process(event, message_id, consumer, raw=fields)
                except Exception:
                    logging.exception("notification_id=%s outcome=processing_error", event.notification_id)
        if args.once:
            once(); return 0
        while not stopping:
            try: once()
            except redis.RedisError: logging.warning("outcome=redis_unavailable")
        return 0
    finally:
        try:
            if control is not None:
                control.client.close()
        finally:
            try:
                if notifications is not None:
                    notifications.client.close()
            finally:
                client.close()
