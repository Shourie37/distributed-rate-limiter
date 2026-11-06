from __future__ import annotations

import pytest

from distlimit import (
    FixedWindow,
    InMemoryStore,
    LeakyBucket,
    Limiter,
    ManualClock,
    SlidingWindowCounter,
    SlidingWindowLog,
    TokenBucket,
)
from distlimit.algorithms import Algorithm

FACTORIES = {
    "token_bucket": lambda: TokenBucket(rate=5, per=1.0),
    "leaky_bucket": lambda: LeakyBucket(rate=5, per=1.0),
    "fixed_window": lambda: FixedWindow(limit=5, window=1.0),
    "sliding_log": lambda: SlidingWindowLog(limit=5, window=1.0),
    "sliding_counter": lambda: SlidingWindowCounter(limit=5, window=1.0),
}


def make(algo: Algorithm, clock: ManualClock) -> Limiter:
    return Limiter(algo, InMemoryStore(clock), clock=clock)


@pytest.mark.parametrize("factory", FACTORIES.values(), ids=FACTORIES.keys())
def test_admits_exactly_limit_at_frozen_time(factory, clock):
    lim = make(factory(), clock)
    results = [lim.acquire("k") for _ in range(8)]
    assert [d.allowed for d in results] == [True] * 5 + [False] * 3
    assert [d.remaining for d in results[:5]] == [4, 3, 2, 1, 0]
    assert all(d.retry_after > 0 and d.reason == "limited" for d in results[5:])


@pytest.mark.parametrize("factory", FACTORIES.values(), ids=FACTORIES.keys())
def test_keys_are_isolated(factory, clock):
    lim = make(factory(), clock)
    for _ in range(5):
        lim.acquire("a")
    assert not lim.acquire("a").allowed
    assert lim.acquire("b").allowed


@pytest.mark.parametrize("factory", FACTORIES.values(), ids=FACTORIES.keys())
@pytest.mark.parametrize("cost", [0, 6])
def test_cost_outside_limit_is_rejected(factory, clock, cost):
    with pytest.raises(ValueError):
        make(factory(), clock).acquire("k", cost=cost)


class TestTokenBucket:
    def test_refill_and_retry_after(self, clock):
        lim = make(TokenBucket(rate=2, burst=4), clock)
        assert all(lim.acquire("k").allowed for _ in range(4))
        denied = lim.acquire("k")
        assert denied.retry_after == pytest.approx(0.5)
        clock.advance(0.49)
        assert not lim.acquire("k").allowed
        clock.advance(0.01)
        assert lim.acquire("k").allowed
        assert not lim.acquire("k").allowed

    def test_idle_time_never_exceeds_burst(self, clock):
        lim = make(TokenBucket(rate=2, burst=4), clock)
        clock.advance(3600)
        assert sum(lim.acquire("k").allowed for _ in range(10)) == 4

    def test_weighted_cost(self, clock):
        lim = make(TokenBucket(rate=10, burst=10), clock)
        assert lim.acquire("k", cost=7).allowed
        denied = lim.acquire("k", cost=4)
        assert not denied.allowed
        assert denied.retry_after == pytest.approx(0.1)

    def test_rate_per_minute(self, clock):
        lim = make(TokenBucket(rate=60, per=60, burst=1), clock)
        assert lim.acquire("k").allowed
        assert lim.acquire("k").retry_after == pytest.approx(1.0)


class TestLeakyBucket:
    def test_delays_space_requests_evenly(self, clock):
        lim = make(LeakyBucket(rate=2, capacity=3), clock)
        assert [lim.acquire("k").delay for _ in range(3)] == pytest.approx([0.0, 0.5, 1.0])
        denied = lim.acquire("k")
        assert not denied.allowed
        assert denied.retry_after == pytest.approx(0.5)

    def test_drains_over_time(self, clock):
        lim = make(LeakyBucket(rate=2, capacity=3), clock)
        for _ in range(3):
            lim.acquire("k")
        clock.advance(0.5)
        d = lim.acquire("k")
        assert d.allowed
        assert d.delay == pytest.approx(1.0)


class TestFixedWindow:
    def test_retry_after_is_time_to_window_end(self, clock):
        lim = make(FixedWindow(limit=3, window=10), clock)
        for _ in range(3):
            lim.acquire("k")
        clock.advance(4)
        assert lim.acquire("k").retry_after == pytest.approx(6)
        clock.set(1009.999)
        assert not lim.acquire("k").allowed
        clock.set(1010.0)
        assert lim.acquire("k").allowed

    def test_boundary_allows_double_burst(self, clock):
        lim = make(FixedWindow(limit=3, window=10), clock)
        clock.set(1009.9)
        first = sum(lim.acquire("k").allowed for _ in range(5))
        clock.set(1010.0)
        second = sum(lim.acquire("k").allowed for _ in range(5))
        assert first + second == 6  # 2x the limit inside 0.1 s


class TestSlidingWindowLog:
    def test_rolling_window_is_exact(self, clock):
        lim = make(SlidingWindowLog(limit=3, window=10), clock)
        for t in (1000, 1004, 1008):
            clock.set(t)
            assert lim.acquire("k").allowed
        clock.set(1009)
        assert lim.acquire("k").retry_after == pytest.approx(1)
        clock.set(1010)
        assert lim.acquire("k").allowed  # 1000 left the window
        denied = lim.acquire("k")
        assert not denied.allowed
        assert denied.retry_after == pytest.approx(4)

    def test_no_boundary_burst(self, clock):
        lim = make(SlidingWindowLog(limit=3, window=10), clock)
        clock.set(1009.9)
        assert sum(lim.acquire("k").allowed for _ in range(5)) == 3
        clock.set(1010.0)
        assert not lim.acquire("k").allowed


class TestSlidingWindowCounter:
    def test_previous_window_is_weighted(self, clock):
        store = InMemoryStore(clock)
        lim = Limiter(SlidingWindowCounter(limit=10, window=10), store, clock=clock)
        clock.set(1005)
        assert sum(lim.acquire("k").allowed for _ in range(10)) == 10
        clock.set(1015)  # half of the previous window still overlaps: 10 * 0.5 = 5
        assert sum(lim.acquire("k").allowed for _ in range(5)) == 5
        denied = lim.acquire("k")
        assert not denied.allowed
        assert denied.retry_after == pytest.approx(1.0)
        # The denied request was rolled back.
        assert store.get("sliding_window_counter:k:101").value == 5
        clock.advance(1.0)
        assert lim.acquire("k").allowed

    def test_boundary_burst_is_damped(self, clock):
        lim = make(SlidingWindowCounter(limit=10, window=10), clock)
        clock.set(1009.9)
        assert sum(lim.acquire("k").allowed for _ in range(10)) == 10
        clock.set(1010.0)
        assert not lim.acquire("k").allowed
