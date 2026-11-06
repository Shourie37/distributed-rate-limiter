from __future__ import annotations

import time

from distlimit import (
    FailMode,
    FailurePolicy,
    FaultyStore,
    InMemoryStore,
    Limiter,
    Metrics,
    SlidingWindowCounter,
    TokenBucket,
)


def make(store, clock, mode, timeout=0.05, metrics=None, algo=None):
    return Limiter(
        algo or TokenBucket(rate=5),
        store,
        clock=clock,
        policy=FailurePolicy(mode=mode, timeout=timeout),
        metrics=metrics,
    )


def test_store_down_fail_open_allows(clock):
    metrics = Metrics()
    lim = make(FaultyStore(InMemoryStore(clock), down=True), clock, FailMode.OPEN, metrics=metrics)
    d = lim.acquire("k")
    assert d.allowed and d.reason == "fail_open"
    assert metrics["reason.fail_open"] == 1
    assert metrics["error.StoreUnavailable"] == 1


def test_store_down_fail_closed_denies_with_retry_after(clock):
    metrics = Metrics()
    lim = make(FaultyStore(InMemoryStore(clock), down=True), clock, FailMode.CLOSED, metrics=metrics)
    d = lim.acquire("k")
    assert not d.allowed and d.reason == "fail_closed"
    assert d.retry_after == 1.0
    assert metrics["denied"] == 1 and metrics["store_errors"] == 1


def test_slow_store_is_cut_off_by_timeout(clock):
    lim = make(FaultyStore(InMemoryStore(clock), latency=0.5), clock, FailMode.CLOSED, timeout=0.02)
    start = time.monotonic()
    d = lim.acquire("k")
    assert time.monotonic() - start < 0.2
    assert d.reason == "fail_closed"


def test_budget_is_shared_across_calls(clock):
    # The sliding counter makes two calls. Each one fits the budget, but both do not.
    store = FaultyStore(InMemoryStore(clock), latency=0.015)
    tight = make(store, clock, FailMode.CLOSED, timeout=0.025, algo=SlidingWindowCounter(5, 1))
    assert tight.acquire("k").reason == "fail_closed"
    loose = make(store, clock, FailMode.CLOSED, timeout=0.5, algo=SlidingWindowCounter(5, 1))
    assert loose.acquire("k").reason == "ok"


def test_recovers_when_store_comes_back(clock):
    store = FaultyStore(InMemoryStore(clock), down=True)
    lim = make(store, clock, FailMode.CLOSED)
    assert not lim.acquire("k").allowed
    store.down = False
    assert lim.acquire("k").reason == "ok"


def test_partial_errors_fail_open_never_deny_below_limit(clock):
    metrics = Metrics()
    store = FaultyStore(InMemoryStore(clock), error_rate=0.5, seed=7)
    lim = make(store, clock, FailMode.OPEN, metrics=metrics, algo=TokenBucket(rate=1000))
    assert all(lim.acquire("k").allowed for _ in range(200))
    assert 0 < metrics["reason.fail_open"] < 200


class AlwaysConflict(InMemoryStore):
    def compare_and_set(self, *args, **kwargs) -> bool:
        super().compare_and_set(*args, **kwargs)
        return False


def test_cas_contention_triggers_policy(clock):
    metrics = Metrics()
    lim = Limiter(
        TokenBucket(rate=5),
        AlwaysConflict(clock),
        clock=clock,
        policy=FailurePolicy(FailMode.CLOSED),
        metrics=metrics,
        max_cas_retries=3,
    )
    assert lim.acquire("k").reason == "fail_closed"
    assert metrics["cas_retries"] == 4
    assert metrics["error.StoreContention"] == 1
