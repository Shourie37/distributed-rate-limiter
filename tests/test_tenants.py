from __future__ import annotations

from distlimit import FixedWindow, InMemoryStore, TenantPolicy, TenantRateLimiter, TokenBucket


def make(clock, default=None):
    default = default or TenantPolicy(tenant_limit=FixedWindow(10, 60), key_limit=FixedWindow(3, 60))
    return TenantRateLimiter(InMemoryStore(clock), default, clock=clock)


def test_key_limit_is_per_key(clock):
    rl = make(clock)
    assert sum(rl.check("t1", "a").allowed for _ in range(5)) == 3
    assert rl.check("t1", "b").allowed


def test_tenant_limit_caps_all_keys(clock):
    rl = make(clock)
    admitted = sum(rl.check("t1", f"key-{i % 5}").allowed for i in range(50))
    assert admitted == 10


def test_tenants_are_isolated(clock):
    rl = make(clock)
    for i in range(20):
        rl.check("t1", f"k{i}")
    assert rl.check("t2", "k0").allowed


def test_override_and_custom_tenant(clock):
    rl = make(clock)
    rl.set_tenant(
        "paid",
        TenantPolicy(
            tenant_limit=TokenBucket(rate=100, burst=100),
            key_limit=FixedWindow(3, 60),
            key_overrides={"vip": FixedWindow(50, 60)},
        ),
    )
    assert sum(rl.check("paid", "vip").allowed for _ in range(60)) == 50
    assert sum(rl.check("paid", "basic").allowed for _ in range(10)) == 3


def test_denial_reports_tightest_limit(clock):
    rl = make(clock)
    d = rl.check("t1", "a")
    assert d.limit == 3 and d.remaining == 2
    for _ in range(2):
        rl.check("t1", "a")
    denied = rl.check("t1", "a")
    assert not denied.allowed and denied.limit == 3


def test_metrics_by_tenant(clock):
    rl = make(clock)
    for _ in range(5):
        rl.check("t1", "a")
    snap = rl.metrics.snapshot()
    assert snap["tenants"]["t1"] == {"allowed": 3, "denied": 2}
    assert snap["decisions"] == 5
