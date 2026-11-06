"""Fixed-window limiter that caches counts on the node and syncs in batches.

Each node admits against (last known global count + its own unsynced count).
It pushes its unsynced count to the store when that count reaches
batch_size, or when sync_interval has passed. This cuts store calls by
about batch_size times.

Over-admit bound, per key and window, for N nodes:

    admitted <= limit + N * batch_size - 1

Reason: before the store count reaches the limit, each sync adds at most
batch_size, so the sync that crosses the limit leaves at most
limit - 1 + batch_size. After that, every other node can add at most one
more batch that it built on a stale count, and then it sees the limit.

Fail-open sync failures void the bound, because unsynced counts can grow.
"""

from __future__ import annotations

import math
import threading
import time
from dataclasses import dataclass

from .clock import Clock, SystemClock
from .decision import FAIL_OPEN, LIMITED, Decision
from .limiter import FailMode, FailurePolicy, Outcome
from .metrics import Metrics
from .session import StoreSession
from .store import Store, StoreError


@dataclass(slots=True)
class _KeyState:
    window: int
    known: int = 0
    pending: int = 0
    last_sync: float = 0.0


class LocalCacheLimiter:
    name = "cached_fixed_window"

    def __init__(
        self,
        limit: int,
        window: float,
        store: Store,
        *,
        batch_size: int = 10,
        sync_interval: float = 0.25,
        clock: Clock | None = None,
        policy: FailurePolicy | None = None,
        metrics: Metrics | None = None,
        prefix: str = "cached",
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self.limit = limit
        self.window = window
        self.store = store
        self.batch_size = batch_size
        self.sync_interval = sync_interval
        self.clock = clock or SystemClock()
        self.policy = policy or FailurePolicy()
        self.metrics = metrics
        self.prefix = prefix
        self._state: dict[str, _KeyState] = {}
        # Store calls happen under this lock. It keeps each node's batch accounting exact.
        self._lock = threading.Lock()

    def over_admit_bound(self, nodes: int) -> int:
        return nodes * self.batch_size - 1

    def _store_key(self, key: str, window: int) -> str:
        return f"{self.prefix}:{key}:{window}"

    def _sync(self, s: StoreSession, key: str, st: _KeyState, now: float) -> None:
        st.known = s.incr(self._store_key(key, st.window), st.pending, ttl=2 * self.window)
        st.pending = 0
        st.last_sync = now

    def evaluate(self, key: str, cost: int = 1) -> Outcome:
        if not 1 <= cost <= self.limit:
            raise ValueError(f"cost must be between 1 and {self.limit}, got {cost}")
        start = time.monotonic()
        s = StoreSession(self.store, self.policy.timeout)
        now = self.clock.now()
        idx = math.floor(now / self.window)
        error = None
        with self._lock:
            st = self._state.get(key)
            if st is None or st.window != idx:
                if st is not None and st.pending:
                    try:
                        self._sync(s, key, st, now)
                    except StoreError:
                        pass  # The old window is over. Losing its tail count is safe.
                st = _KeyState(idx, last_sync=now)
                self._state[key] = st

            if st.known + st.pending + cost > self.limit:
                reset = (idx + 1) * self.window - now
                decision = Decision(False, self.limit, 0, retry_after=reset, reason=LIMITED)
            else:
                st.pending += cost
                decision = Decision(True, self.limit, self.limit - st.known - st.pending)
                if st.pending >= self.batch_size or now - st.last_sync >= self.sync_interval:
                    try:
                        self._sync(s, key, st, now)
                    except StoreError as e:
                        error = type(e).__name__
                        if self.policy.mode is FailMode.CLOSED:
                            st.pending -= cost
                            decision = self.policy.fallback(self.limit)
                        else:
                            decision = Decision(True, self.limit, 0, reason=FAIL_OPEN)
        return Outcome(decision, s.calls, s.retries, time.monotonic() - start, error)

    def acquire(self, key: str, cost: int = 1) -> Decision:
        out = self.evaluate(key, cost)
        if self.metrics is not None:
            self.metrics.record(
                out.decision, store_calls=out.store_calls, latency=out.latency, error=out.error
            )
        return out.decision

    def flush(self) -> None:
        """Push all unsynced counts. Call on shutdown."""
        now = self.clock.now()
        with self._lock:
            for key, st in self._state.items():
                if st.pending:
                    self._sync(StoreSession(self.store, self.policy.timeout), key, st, now)
