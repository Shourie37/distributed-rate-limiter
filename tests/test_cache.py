from __future__ import annotations

from distlimit import FailMode, FailurePolicy, FaultyStore, InMemoryStore, LocalCacheLimiter


def test_batching_cuts_store_calls(clock):
    store = InMemoryStore(clock)
    lim = LocalCacheLimiter(1000, 60, store, batch_size=10, clock=clock)
    for _ in range(500):
        assert lim.acquire("k").allowed
    assert store.total_calls == 50


def test_worst_case_interleaving_stays_in_bound(clock):
    limit, batch, n = 10, 5, 3
    store = InMemoryStore(clock)
    nodes = [LocalCacheLimiter(limit, 60, store, batch_size=batch, clock=clock) for _ in range(n)]
    admitted = 0
    for _ in range(20):  # round robin, so every node builds a batch on a stale count
        for node in nodes:
            admitted += node.acquire("k").allowed
    assert admitted == limit + (n - 1) * batch
    assert admitted <= limit + nodes[0].over_admit_bound(n)


def test_denies_locally_without_store_call(clock):
    store = InMemoryStore(clock)
    lim = LocalCacheLimiter(5, 60, store, batch_size=5, clock=clock)
    for _ in range(5):
        lim.acquire("k")
    calls = store.total_calls
    assert not lim.acquire("k").allowed
    assert store.total_calls == calls


def test_new_window_resets_and_flushes_old_pending(clock):
    store = InMemoryStore(clock)
    lim = LocalCacheLimiter(5, 10, store, batch_size=10, clock=clock)
    for _ in range(3):
        lim.acquire("k")
    clock.advance(10)
    d = lim.acquire("k")
    assert d.allowed and d.remaining == 4
    assert store.get("cached:k:100").value == 3


def test_sync_interval_forces_sync(clock):
    store = InMemoryStore(clock)
    lim = LocalCacheLimiter(100, 60, store, batch_size=50, sync_interval=1.0, clock=clock)
    lim.acquire("k")
    assert store.total_calls == 0
    clock.advance(1.0)
    lim.acquire("k")
    assert store.get("cached:k:16").value == 2


def test_flush_pushes_pending(clock):
    store = InMemoryStore(clock)
    lim = LocalCacheLimiter(100, 60, store, batch_size=50, clock=clock)
    for _ in range(7):
        lim.acquire("k")
    lim.flush()
    assert store.get("cached:k:16").value == 7


def test_sync_failure_follows_policy(clock):
    store = FaultyStore(InMemoryStore(clock), down=True)
    closed = LocalCacheLimiter(10, 60, store, batch_size=1, clock=clock, policy=FailurePolicy(FailMode.CLOSED))
    d = closed.acquire("k")
    assert not d.allowed and d.reason == "fail_closed"
    opened = LocalCacheLimiter(10, 60, store, batch_size=1, clock=clock, policy=FailurePolicy(FailMode.OPEN))
    d = opened.acquire("k")
    assert d.allowed and d.reason == "fail_open"
