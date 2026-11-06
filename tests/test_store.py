from __future__ import annotations

import time

import pytest

from distlimit import FaultyStore, InMemoryStore, Limiter, StoreTimeout, StoreUnavailable, TokenBucket


def test_cas_put_if_absent_and_version_check(clock):
    s = InMemoryStore(clock)
    assert s.compare_and_set("k", None, "a", ttl=None)
    assert not s.compare_and_set("k", None, "b", ttl=None)
    e = s.get("k")
    assert e.value == "a"
    assert not s.compare_and_set("k", e.version + 1, "b", ttl=None)
    assert s.compare_and_set("k", e.version, "b", ttl=None)
    assert s.get("k").value == "b"


def test_get_expires_at_ttl(clock):
    s = InMemoryStore(clock)
    s.compare_and_set("k", None, 1, ttl=5)
    clock.advance(4.999)
    assert s.get("k") is not None
    clock.advance(0.001)
    assert s.get("k") is None


def test_incr_keeps_first_ttl_and_restarts_after_expiry(clock):
    s = InMemoryStore(clock)
    assert s.incr("c", 2, ttl=10) == 2
    clock.advance(6)
    assert s.incr("c", 3, ttl=10) == 5  # TTL is not extended
    clock.advance(4)
    assert s.incr("c", 1, ttl=10) == 1


def test_expired_key_cannot_pass_old_cas(clock):
    s = InMemoryStore(clock)
    s.compare_and_set("k", None, "old", ttl=1)
    old_version = s.get("k").version
    clock.advance(2)
    s.compare_and_set("k", None, "new", ttl=None)
    assert not s.compare_and_set("k", old_version, "stale", ttl=None)


def test_purge_removes_only_expired(clock):
    s = InMemoryStore(clock)
    s.compare_and_set("short", None, 1, ttl=1)
    s.compare_and_set("forever", None, 1, ttl=None)
    clock.advance(1)
    assert s.purge_expired() == 1
    assert len(s) == 1


def test_limiter_state_expires_and_comes_back_full(clock):
    store = InMemoryStore(clock)
    lim = Limiter(TokenBucket(rate=2, burst=4), store, clock=clock)
    for _ in range(4):
        lim.acquire("k")
    clock.advance(4 / 2 + 1)  # time to refill plus the TTL margin
    assert store.purge_expired() == 1
    assert sum(lim.acquire("k").allowed for _ in range(5)) == 4


def test_calls_are_counted(clock):
    s = InMemoryStore(clock)
    s.get("a")
    s.incr("b", 1, ttl=1)
    s.compare_and_set("c", None, 1, ttl=None)
    assert s.calls == {"get": 1, "incr": 1, "cas": 1}
    assert s.total_calls == 3


def test_faulty_store_down_and_slow(clock):
    f = FaultyStore(InMemoryStore(clock), down=True)
    with pytest.raises(StoreUnavailable):
        f.get("k")
    f.down, f.latency = False, 0.2
    start = time.monotonic()
    with pytest.raises(StoreTimeout):
        f.get("k", timeout=0.01)
    assert time.monotonic() - start < 0.1
