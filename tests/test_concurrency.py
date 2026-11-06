"""Many threads on many nodes share one store. No algorithm may over-admit."""

from __future__ import annotations

import pytest

from conftest import hammer
from distlimit import (
    FailMode,
    FailurePolicy,
    FixedWindow,
    InMemoryStore,
    LeakyBucket,
    Limiter,
    LocalCacheLimiter,
    Metrics,
    SlidingWindowCounter,
    SlidingWindowLog,
    TokenBucket,
)

LIMIT = 200
NODES = 4
THREADS = 4
ATTEMPTS = 50  # 800 attempts against a limit of 200

FACTORIES = {
    "token_bucket": lambda: TokenBucket(rate=1, burst=LIMIT),
    "leaky_bucket": lambda: LeakyBucket(rate=1, capacity=LIMIT),
    "fixed_window": lambda: FixedWindow(limit=LIMIT, window=60),
    "sliding_log": lambda: SlidingWindowLog(limit=LIMIT, window=60),
    "sliding_counter": lambda: SlidingWindowCounter(limit=LIMIT, window=60),
}

# A generous budget, so a slow thread never triggers the failure policy.
POLICY = FailurePolicy(mode=FailMode.CLOSED, timeout=10.0)


@pytest.mark.parametrize("factory", FACTORIES.values(), ids=FACTORIES.keys())
def test_shared_store_admits_exactly_the_limit(factory, clock, fast_switch):
    store = InMemoryStore(clock)
    metrics = Metrics()
    nodes = [
        Limiter(factory(), store, clock=clock, policy=POLICY, metrics=metrics, max_cas_retries=100_000)
        for _ in range(NODES)
    ]
    admitted = hammer([n.acquire for n in nodes], THREADS, ATTEMPTS)
    assert admitted == LIMIT
    assert metrics["store_errors"] == 0


@pytest.mark.parametrize("batch_size", [1, 10, 25])
def test_local_cache_over_admit_stays_in_bound(batch_size, clock, fast_switch):
    store = InMemoryStore(clock)
    nodes = [
        LocalCacheLimiter(LIMIT, 60, store, batch_size=batch_size, clock=clock, policy=POLICY)
        for _ in range(NODES)
    ]
    admitted = hammer([n.acquire for n in nodes], THREADS, ATTEMPTS)
    assert LIMIT <= admitted <= LIMIT + nodes[0].over_admit_bound(NODES)
