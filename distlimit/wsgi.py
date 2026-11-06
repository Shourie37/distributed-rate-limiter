"""WSGI middleware that answers 429 with Retry-After when a limit is hit."""

from __future__ import annotations

import json
import math
import time
from typing import Any, Callable, Iterable

from .tenants import TenantRateLimiter

Environ = dict[str, Any]
StartResponse = Callable[..., Any]
WSGIApp = Callable[[Environ, StartResponse], Iterable[bytes]]


def default_identify(environ: Environ) -> tuple[str, str]:
    tenant = environ.get("HTTP_X_TENANT_ID") or "anonymous"
    key = environ.get("HTTP_X_API_KEY") or environ.get("REMOTE_ADDR") or "unknown"
    return tenant, key


def retry_after_seconds(retry_after: float) -> int:
    # Retry-After takes whole seconds. Round up so a client never retries too early.
    return max(1, math.ceil(retry_after))


class RateLimitMiddleware:
    def __init__(
        self,
        app: WSGIApp,
        limiter: TenantRateLimiter,
        *,
        identify: Callable[[Environ], tuple[str, str]] = default_identify,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.app = app
        self.limiter = limiter
        self.identify = identify
        self.sleep = sleep

    def __call__(self, environ: Environ, start_response: StartResponse) -> Iterable[bytes]:
        tenant, key = self.identify(environ)
        d = self.limiter.check(tenant, key)
        headers = [("RateLimit-Limit", str(d.limit)), ("RateLimit-Remaining", str(d.remaining))]
        if not d.allowed:
            retry = retry_after_seconds(d.retry_after)
            body = json.dumps({"error": "rate_limited", "reason": d.reason, "retry_after": retry}).encode()
            start_response(
                "429 Too Many Requests",
                [
                    ("Content-Type", "application/json"),
                    ("Content-Length", str(len(body))),
                    ("Retry-After", str(retry)),
                    *headers,
                ],
            )
            return [body]
        if d.delay > 0:
            self.sleep(d.delay)

        def wrapped(status: str, response_headers: list[tuple[str, str]], exc_info: Any = None) -> Any:
            return start_response(status, list(response_headers) + headers, exc_info)

        return self.app(environ, wrapped)
