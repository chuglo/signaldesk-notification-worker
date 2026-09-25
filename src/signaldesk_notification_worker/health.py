from __future__ import annotations
import httpx
import redis

def liveness() -> dict[str, str]:
    return {"status": "ok"}

def readiness(notification_url: str, control_url: str, *, redis_url: str, timeout: httpx.Timeout) -> bool:
    redis_client = None
    http_client = None
    try:
        http_client = httpx.Client(timeout=timeout)
        if not (http_client.get(notification_url.rstrip("/") + "/healthz").is_success and http_client.get(control_url.rstrip("/") + "/healthz").is_success):
            return False
        redis_client = redis.Redis.from_url(
            str(redis_url),
            socket_connect_timeout=timeout.connect,
            socket_timeout=timeout.read,
        )
        return bool(redis_client.ping())
    except (httpx.HTTPError, redis.RedisError):
        return False
    finally:
        try:
            if redis_client is not None:
                redis_client.close()
        finally:
            if http_client is not None:
                http_client.close()
