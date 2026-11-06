from __future__ import annotations

import pytest

from distlimit import InMemoryStore, LeakyBucket, Limiter, SkewedClock, SlidingWindowLog, TokenBucket


def nodes(algo_factory, clock, skew, use_store_time):
    store = InMemoryStore(clock)  # the store keeps true time
    fast = Limiter(algo_factory(), store, clock=SkewedClock(clock, skew), use_store_time=use_store_time)
    normal = Limiter(algo_factory(), store, clock=clock, use_store_time=use_store_time)
    return store, fast, normal


def test_fast_node_over_admit_is_bounded_by_rate_times_skew(clock):
    rate, burst, skew = 1.0, 5, 3.0
    _, fast, normal = nodes(lambda: TokenBucket(rate=rate, burst=burst), clock, skew, False)
    assert sum(normal.acquire("k").allowed for _ in range(10)) == burst
    extra = sum(fast.acquire("k").allowed for _ in range(10))
    assert extra == rate * skew


def test_store_time_removes_skew(clock):
    _, fast, normal = nodes(lambda: TokenBucket(rate=1, burst=5), clock, 3.0, True)
    assert sum(normal.acquire("k").allowed for _ in range(10)) == 5
    assert sum(fast.acquire("k").allowed for _ in range(10)) == 0


def test_slow_node_cannot_rewind_stored_time(clock):
    store = InMemoryStore(clock)
    fast = Limiter(TokenBucket(rate=1, burst=5), store, clock=SkewedClock(clock, 3.0))
    slow = Limiter(TokenBucket(rate=1, burst=5), store, clock=clock)
    for _ in range(5):
        fast.acquire("k")
    _, last = store.get("token_bucket:k").value
    # The slow node sees time 3 s behind the stored time: no refill, and the stamp does not move back.
    denied = slow.acquire("k")
    assert not denied.allowed
    assert denied.retry_after == pytest.approx(1.0 + 3.0)
    clock.advance(3.5)
    assert not slow.acquire("k").allowed
    clock.advance(0.5)
    assert slow.acquire("k").allowed
    assert store.get("token_bucket:k").value[1] >= last


def test_leaky_bucket_delay_includes_skew(clock):
    store = InMemoryStore(clock)
    fast = Limiter(LeakyBucket(rate=1, capacity=5), store, clock=SkewedClock(clock, 2.0))
    slow = Limiter(LeakyBucket(rate=1, capacity=5), store, clock=clock)
    fast.acquire("k")
    assert slow.acquire("k").delay == pytest.approx(1.0 + 2.0)


def test_log_stays_sorted_with_mixed_clocks(clock):
    store = InMemoryStore(clock)
    fast = Limiter(SlidingWindowLog(5, 10), store, clock=SkewedClock(clock, 2.0))
    slow = Limiter(SlidingWindowLog(5, 10), store, clock=clock)
    fast.acquire("k")
    slow.acquire("k")
    log = store.get("sliding_window_log:k").value
    assert list(log) == sorted(log)
