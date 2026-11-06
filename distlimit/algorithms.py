"""Rate limit algorithms.

Each algorithm keeps its state in the shared store. State values are
immutable tuples. Read-modify-write algorithms use a compare-and-set loop.
Counter algorithms use atomic increments.

Clock skew: elapsed time is never negative, and a stored timestamp never
moves backward. A node whose clock runs behind cannot erase time that a
faster node already counted.
"""

from __future__ import annotations

import math
from abc import ABC, abstractmethod
from typing import Any, Callable

from .decision import LIMITED, Decision
from .session import StoreSession

EPS = 1e-9

# step(state) -> (new_state or None to skip the write, ttl, decision)
Step = Callable[[Any], tuple[Any, float | None, Decision]]


def _cas_update(s: StoreSession, key: str, step: Step) -> Decision:
    while True:
        entry = s.get(key)
        new_state, ttl, decision = step(None if entry is None else entry.value)
        if new_state is None:
            return decision
        if s.cas(key, None if entry is None else entry.version, new_state, ttl):
            return decision
        s.conflict()


class Algorithm(ABC):
    name: str
    limit: int

    @abstractmethod
    def acquire(self, s: StoreSession, key: str, now: float, cost: int) -> Decision: ...

    def _check_cost(self, cost: int) -> None:
        if not 1 <= cost <= self.limit:
            raise ValueError(f"cost must be between 1 and {self.limit}, got {cost}")


class TokenBucket(Algorithm):
    """Refills at rate/per tokens per second, up to burst tokens."""

    name = "token_bucket"

    def __init__(self, rate: float, per: float = 1.0, burst: int | None = None) -> None:
        self.rate = rate / per
        self.limit = burst if burst is not None else max(1, int(rate))

    def acquire(self, s: StoreSession, key: str, now: float, cost: int = 1) -> Decision:
        self._check_cost(cost)
        cap, rate = self.limit, self.rate

        def step(state: Any) -> tuple[Any, float | None, Decision]:
            tokens, last = (float(cap), now) if state is None else state
            tokens = min(cap, tokens + max(0.0, now - last) * rate)
            ahead = max(0.0, last - now)
            last = max(last, now)
            if tokens + EPS >= cost:
                tokens = max(0.0, tokens - cost)
                ttl = (cap - tokens) / rate + ahead + 1.0
                return (tokens, last), ttl, Decision(True, cap, int(tokens + EPS))
            retry = (cost - tokens) / rate + ahead
            return None, None, Decision(False, cap, int(tokens + EPS), retry_after=retry, reason=LIMITED)

        return _cas_update(s, key, step)


class LeakyBucket(Algorithm):
    """A queue of size capacity that drains at rate/per requests per second.

    An admitted request gets a delay. If the caller waits that long, the
    output is a smooth stream with no bursts.
    """

    name = "leaky_bucket"

    def __init__(self, rate: float, per: float = 1.0, capacity: int | None = None) -> None:
        self.rate = rate / per
        self.limit = capacity if capacity is not None else max(1, int(rate))

    def acquire(self, s: StoreSession, key: str, now: float, cost: int = 1) -> Decision:
        self._check_cost(cost)
        cap, rate = self.limit, self.rate

        def step(state: Any) -> tuple[Any, float | None, Decision]:
            level, last = (0.0, now) if state is None else state
            level = max(0.0, level - max(0.0, now - last) * rate)
            ahead = max(0.0, last - now)
            last = max(last, now)
            if level + cost <= cap + EPS:
                delay = level / rate + ahead
                level += cost
                ttl = level / rate + ahead + 1.0
                return (level, last), ttl, Decision(True, cap, int(cap - level + EPS), delay=delay)
            retry = (level + cost - cap) / rate + ahead
            return None, None, Decision(False, cap, int(cap - level + EPS), retry_after=retry, reason=LIMITED)

        return _cas_update(s, key, step)


class FixedWindow(Algorithm):
    """At most limit requests per aligned window. One atomic increment per call."""

    name = "fixed_window"

    def __init__(self, limit: int, window: float) -> None:
        self.limit = limit
        self.window = window

    def acquire(self, s: StoreSession, key: str, now: float, cost: int = 1) -> Decision:
        self._check_cost(cost)
        idx = math.floor(now / self.window)
        count = s.incr(f"{key}:{idx}", cost, ttl=2 * self.window)
        if count <= self.limit:
            return Decision(True, self.limit, self.limit - count)
        reset = (idx + 1) * self.window - now
        return Decision(False, self.limit, 0, retry_after=reset, reason=LIMITED)


class SlidingWindowLog(Algorithm):
    """Stores each admitted timestamp. Exact, but memory is O(limit) per key."""

    name = "sliding_window_log"

    def __init__(self, limit: int, window: float) -> None:
        self.limit = limit
        self.window = window

    def acquire(self, s: StoreSession, key: str, now: float, cost: int = 1) -> Decision:
        self._check_cost(cost)
        limit, window = self.limit, self.window

        def step(state: Any) -> tuple[Any, float | None, Decision]:
            cutoff = now - window
            log = tuple(t for t in (state or ()) if t > cutoff)
            if len(log) + cost <= limit:
                new = log + (now,) * cost
                if log and log[-1] > now:
                    new = tuple(sorted(new))
                ttl = new[-1] - now + window
                return new, ttl, Decision(True, limit, limit - len(new))
            k = len(log) + cost - limit
            retry = log[k - 1] + window - now
            return None, None, Decision(False, limit, limit - len(log), retry_after=retry, reason=LIMITED)

        return _cas_update(s, key, step)


class SlidingWindowCounter(Algorithm):
    """Weights the previous window count by how much of it still overlaps.

    Memory is two counters per key. The estimate assumes requests in the
    previous window were spread evenly.
    """

    name = "sliding_window_counter"

    def __init__(self, limit: int, window: float) -> None:
        self.limit = limit
        self.window = window

    def acquire(self, s: StoreSession, key: str, now: float, cost: int = 1) -> Decision:
        self._check_cost(cost)
        limit, window = self.limit, self.window
        idx = math.floor(now / window)
        weight = 1.0 - (now - idx * window) / window
        prev_entry = s.get(f"{key}:{idx - 1}")
        prev = 0 if prev_entry is None else prev_entry.value
        curr_key = f"{key}:{idx}"
        curr = s.incr(curr_key, cost, ttl=2 * window)
        estimate = prev * weight + curr
        if estimate <= limit + EPS:
            return Decision(True, limit, int(limit - estimate + EPS))
        # Undo so the denied request does not count against the next window.
        s.incr(curr_key, -cost, ttl=2 * window)
        if curr <= limit and prev > 0:
            retry = (weight - (limit - curr) / prev) * window
        else:
            retry = weight * window
        remaining = max(0, int(limit - (estimate - cost) + EPS))
        return Decision(False, limit, remaining, retry_after=max(retry, 0.0), reason=LIMITED)
