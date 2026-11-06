from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from wsgiref.util import setup_testing_defaults
from wsgiref.validate import validator

import pytest

from distlimit import (
    FailMode,
    FailurePolicy,
    FaultyStore,
    FixedWindow,
    InMemoryStore,
    LeakyBucket,
    RateLimitMiddleware,
    TenantPolicy,
    TenantRateLimiter,
)
from distlimit.wsgi import retry_after_seconds


def hello(environ, start_response):
    start_response("200 OK", [("Content-Type", "text/plain")])
    return [b"hello"]


def call(app, path="/", **headers):
    environ = {f"HTTP_{k.upper()}": v for k, v in headers.items()}
    environ.update(PATH_INFO=path, SCRIPT_NAME="", QUERY_STRING="")
    setup_testing_defaults(environ)
    captured = {}

    def start_response(status, response_headers, exc_info=None):
        captured["status"] = status
        captured["headers"] = dict(response_headers)
        return lambda data: None

    result = app(environ, start_response)
    try:
        body = b"".join(result)
    finally:
        result.close()
    return captured["status"], captured["headers"], body


def make_app(clock, policy=None, store=None, sleep=None):
    rl = TenantRateLimiter(
        store or InMemoryStore(clock),
        TenantPolicy(key_limit=FixedWindow(2, 10)),
        clock=clock,
        policy=policy,
    )
    kwargs = {"sleep": sleep} if sleep else {}
    return validator(RateLimitMiddleware(hello, rl, **kwargs))


def test_allowed_request_gets_rate_headers(clock):
    status, headers, body = call(make_app(clock), x_api_key="k")
    assert status == "200 OK" and body == b"hello"
    assert headers["RateLimit-Limit"] == "2"
    assert headers["RateLimit-Remaining"] == "1"


def test_limited_request_gets_429_and_retry_after(clock):
    app = make_app(clock)
    clock.advance(6.5)
    for _ in range(2):
        call(app, x_api_key="k")
    status, headers, body = call(app, x_api_key="k")
    assert status == "429 Too Many Requests"
    assert headers["Retry-After"] == "4"  # 3.5 s left in the window, rounded up
    assert json.loads(body)["reason"] == "limited"
    assert call(app, x_api_key="other")[0] == "200 OK"


def test_fail_closed_returns_429(clock):
    store = FaultyStore(InMemoryStore(clock), down=True)
    app = make_app(clock, policy=FailurePolicy(FailMode.CLOSED, retry_after=2.0), store=store)
    status, headers, body = call(app, x_api_key="k")
    assert status.startswith("429") and headers["Retry-After"] == "2"
    assert json.loads(body)["reason"] == "fail_closed"


def test_fail_open_passes_through(clock):
    store = FaultyStore(InMemoryStore(clock), down=True)
    app = make_app(clock, policy=FailurePolicy(FailMode.OPEN), store=store)
    assert call(app, x_api_key="k")[0] == "200 OK"


def test_leaky_bucket_delay_is_applied(clock):
    slept = []
    rl = TenantRateLimiter(InMemoryStore(clock), TenantPolicy(key_limit=LeakyBucket(rate=4, capacity=3)), clock=clock)
    app = validator(RateLimitMiddleware(hello, rl, sleep=slept.append))
    for _ in range(3):
        call(app, x_api_key="k")
    assert slept == pytest.approx([0.25, 0.5])


@pytest.mark.parametrize("seconds,header", [(0.0, 1), (0.2, 1), (1.0, 1), (1.01, 2), (59.5, 60)])
def test_retry_after_rounds_up(seconds, header):
    assert retry_after_seconds(seconds) == header


def test_demo_app_serves_and_limits(clock):
    path = Path(__file__).parent.parent / "examples" / "demo_server.py"
    spec = importlib.util.spec_from_file_location("demo_server", path)
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)
    app = validator(demo.build_app(clock))
    statuses = [call(app, x_tenant_id="free", x_api_key="k")[0] for _ in range(6)]
    assert statuses.count("200 OK") == 5 and statuses[-1].startswith("429")
    status, _, body = call(app, path="/metrics")
    assert status == "200 OK" and json.loads(body)["decisions"] == 6
