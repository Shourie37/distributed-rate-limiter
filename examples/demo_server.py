"""Demo WSGI service with per-tenant and per-key limits.

Run:  python examples/demo_server.py
Then: curl -i -H 'X-Tenant-Id: free' -H 'X-Api-Key: k1' http://127.0.0.1:8080/
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from wsgiref.simple_server import make_server

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from distlimit import (  # noqa: E402
    Clock,
    FailMode,
    FailurePolicy,
    FixedWindow,
    InMemoryStore,
    RateLimitMiddleware,
    SlidingWindowLog,
    TenantPolicy,
    TenantRateLimiter,
    TokenBucket,
)


def build_app(clock: Clock | None = None):
    limiter = TenantRateLimiter(
        InMemoryStore(clock),
        default=TenantPolicy(
            tenant_limit=TokenBucket(rate=20, burst=40),
            key_limit=SlidingWindowLog(limit=5, window=10),
        ),
        clock=clock,
        policy=FailurePolicy(mode=FailMode.OPEN, timeout=0.05),
    )
    limiter.set_tenant(
        "pro",
        TenantPolicy(
            tenant_limit=TokenBucket(rate=200, burst=400),
            key_limit=FixedWindow(limit=50, window=1),
            key_overrides={"batch-job": TokenBucket(rate=100, burst=500)},
        ),
    )

    def hello(environ, start_response):
        start_response("200 OK", [("Content-Type", "text/plain")])
        return [b"hello\n"]

    limited = RateLimitMiddleware(hello, limiter)

    def app(environ, start_response):
        if environ.get("PATH_INFO") == "/metrics":
            body = json.dumps(limiter.metrics.snapshot(), indent=2).encode()
            start_response("200 OK", [("Content-Type", "application/json"), ("Content-Length", str(len(body)))])
            return [body]
        return limited(environ, start_response)

    return app


if __name__ == "__main__":
    with make_server("127.0.0.1", 8080, build_app()) as server:
        print("Listening on http://127.0.0.1:8080 (GET / and GET /metrics)")
        server.serve_forever()
