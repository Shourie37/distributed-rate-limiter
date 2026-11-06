"""Benchmarks: throughput, accuracy under concurrency, and store calls saved by caching.

Run: python bench/benchmark.py
"""

from __future__ import annotations

import platform
import sys
import threading
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from distlimit import (  # noqa: E402
    Decision,
    FailMode,
    FailurePolicy,
    FaultyStore,
    FixedWindow,
    InMemoryStore,
    LeakyBucket,
    Limiter,
    LocalCacheLimiter,
    ManualClock,
    Metrics,
    SlidingWindowCounter,
    SlidingWindowLog,
    SystemClock,
    TokenBucket,
)

ALGORITHMS: dict[str, Callable[[int], object]] = {
    "token_bucket": lambda n: TokenBucket(rate=1, burst=n),
    "leaky_bucket": lambda n: LeakyBucket(rate=1, capacity=n),
    "fixed_window": lambda n: FixedWindow(limit=n, window=60),
    "sliding_window_log": lambda n: SlidingWindowLog(limit=n, window=60),
    "sliding_window_counter": lambda n: SlidingWindowCounter(limit=n, window=60),
}

NO_TIMEOUT = FailurePolicy(mode=FailMode.CLOSED, timeout=60.0)


def table(headers: list[str], rows: list[list[object]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(lines)


def run_threads(fns: list[Callable[[], int]]) -> int:
    total = 0
    lock = threading.Lock()
    barrier = threading.Barrier(len(fns))

    def worker(fn: Callable[[], int]) -> None:
        nonlocal total
        barrier.wait()
        n = fn()
        with lock:
            total += n

    threads = [threading.Thread(target=worker, args=(f,)) for f in fns]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return total


def throughput(decisions: int = 100_000, keys: int = 1_000, threads: int = 1) -> str:
    def build(name: str, store: InMemoryStore, clock: SystemClock):
        if name == "cached_fixed_window (batch 10)":
            return LocalCacheLimiter(100, 1, store, batch_size=10, clock=clock, policy=NO_TIMEOUT)
        return Limiter(ALGORITHMS[name](100), store, clock=clock, policy=NO_TIMEOUT)

    rows = []
    for name in [*ALGORITHMS, "cached_fixed_window (batch 10)"]:
        clock = SystemClock()
        store = InMemoryStore(clock)
        lim = build(name, store, clock)
        per_thread = decisions // threads

        def work(offset: int) -> Callable[[], int]:
            def fn() -> int:
                acquire = lim.acquire
                return sum(acquire(f"k{(offset + i) % keys}").allowed for i in range(per_thread))

            return fn

        start = time.perf_counter()
        allowed = run_threads([work(t * 7919) for t in range(threads)])
        elapsed = time.perf_counter() - start
        n = per_thread * threads
        rows.append([name, f"{n / elapsed:,.0f}", f"{store.total_calls / n:.2f}", f"{allowed / n:.0%}"])
    return table(["algorithm", "decisions/sec", "store calls/decision", "allowed"], rows)


def accuracy(limit: int = 1_000, nodes: int = 4, threads: int = 4, attempts: int = 500) -> str:
    sys.setswitchinterval(1e-6)
    rows = []
    baseline_calls = None
    configs: list[tuple[str, Callable[[InMemoryStore, ManualClock, Metrics], Callable[[str], Decision]]]] = [
        (name, lambda s, c, m, f=f: Limiter(f(limit), s, clock=c, policy=NO_TIMEOUT, metrics=m, max_cas_retries=10**6).acquire)
        for name, f in ALGORITHMS.items()
    ]
    for b in (1, 10, 50):
        configs.append(
            (
                f"cached_fixed_window (batch {b})",
                lambda s, c, m, b=b: LocalCacheLimiter(limit, 60, s, batch_size=b, clock=c, policy=NO_TIMEOUT, metrics=m).acquire,
            )
        )
    for name, make in configs:
        clock = ManualClock(1_000_020.0)  # frozen, so the exact answer is the limit
        store = InMemoryStore(clock)
        metrics = Metrics()
        acquires = [make(store, clock, metrics) for _ in range(nodes)]

        def fn(acquire: Callable[[str], Decision]) -> Callable[[], int]:
            return lambda: sum(acquire("hot").allowed for _ in range(attempts))

        admitted = run_threads([fn(a) for a in acquires for _ in range(threads)])
        calls = store.total_calls
        if name == "fixed_window":
            baseline_calls = calls
        if name.startswith("cached"):
            b = int(name.split()[-1].rstrip(")"))
            bound = f"<= {nodes * b - 1}"
            saved = f"{1 - calls / baseline_calls:.1%}"
        else:
            bound, saved = "0", "-"
        rows.append([name, admitted, limit, admitted - limit, bound, calls, metrics["cas_retries"], saved])
    sys.setswitchinterval(0.005)
    headers = ["algorithm", "admitted", "exact limit", "over-admit", "documented bound", "store calls", "CAS retries", "calls saved vs fixed_window"]
    return table(headers, rows)


def failure_modes(decisions: int = 40) -> str:
    rows = []
    for state, kwargs in [("healthy", {}), ("slow (200 ms)", {"latency": 0.2}), ("down", {"down": True})]:
        for mode in (FailMode.OPEN, FailMode.CLOSED):
            clock = SystemClock()
            store = FaultyStore(InMemoryStore(clock), **kwargs)
            lim = Limiter(TokenBucket(rate=1_000), store, clock=clock, policy=FailurePolicy(mode=mode, timeout=0.05), metrics=Metrics())
            start = time.perf_counter()
            allowed = sum(lim.acquire(f"k{i}").allowed for i in range(decisions))
            avg_ms = (time.perf_counter() - start) / decisions * 1000
            rows.append([state, mode.value, f"{allowed / decisions:.0%}", f"{avg_ms:.2f}", lim.metrics["store_errors"]])
    return table(["store", "policy", "allowed", "avg decision ms", "store errors"], rows)


def main() -> None:
    print(f"Python {platform.python_version()} on {platform.system()} {platform.machine()}\n")
    print("## Throughput, 1 thread, 1,000 keys, limit 100 per key\n")
    print(throughput(threads=1))
    print("\n## Throughput, 8 threads (one process, so the GIL applies)\n")
    print(throughput(threads=8))
    print("\n## Accuracy: 4 nodes x 4 threads x 500 attempts on one key, frozen clock\n")
    print(accuracy())
    print("\n## Failure policy, timeout budget 50 ms\n")
    print(failure_modes())


if __name__ == "__main__":
    main()
