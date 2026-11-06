from __future__ import annotations

import time
from dataclasses import dataclass
from enum import StrEnum

from .algorithms import Algorithm
from .clock import Clock, SystemClock
from .decision import FAIL_CLOSED, FAIL_OPEN, Decision
from .metrics import Metrics
from .session import StoreSession
from .store import InMemoryStore, Store, StoreError


class FailMode(StrEnum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class FailurePolicy:
    """What to do when the store fails or the timeout budget runs out."""

    mode: FailMode = FailMode.OPEN
    # Total store time for one decision, across all calls and CAS retries.
    timeout: float = 0.05
    # Retry-After for fail-closed denials.
    retry_after: float = 1.0

    def fallback(self, limit: int) -> Decision:
        if self.mode is FailMode.OPEN:
            return Decision(True, limit, 0, reason=FAIL_OPEN)
        return Decision(False, limit, 0, retry_after=self.retry_after, reason=FAIL_CLOSED)


@dataclass(frozen=True, slots=True)
class Outcome:
    decision: Decision
    store_calls: int
    cas_retries: int
    latency: float
    error: str | None


class Limiter:
    """One algorithm bound to a store. Many Limiter nodes can share one store."""

    def __init__(
        self,
        algorithm: Algorithm,
        store: Store | None = None,
        *,
        clock: Clock | None = None,
        policy: FailurePolicy | None = None,
        metrics: Metrics | None = None,
        use_store_time: bool = False,
        max_cas_retries: int = 64,
        prefix: str | None = None,
    ) -> None:
        self.algorithm = algorithm
        self.clock = clock or SystemClock()
        self.store = store if store is not None else InMemoryStore(self.clock)
        self.policy = policy or FailurePolicy()
        self.metrics = metrics
        # Store time removes clock skew between nodes for one extra store call.
        self.use_store_time = use_store_time
        self.max_cas_retries = max_cas_retries
        self.prefix = prefix or algorithm.name

    def evaluate(self, key: str, cost: int = 1) -> Outcome:
        start = time.monotonic()
        s = StoreSession(self.store, self.policy.timeout, self.max_cas_retries)
        error = None
        try:
            now = s.time() if self.use_store_time else self.clock.now()
            decision = self.algorithm.acquire(s, f"{self.prefix}:{key}", now, cost)
        except StoreError as e:
            error = type(e).__name__
            decision = self.policy.fallback(self.algorithm.limit)
        return Outcome(decision, s.calls, s.retries, time.monotonic() - start, error)

    def acquire(self, key: str, cost: int = 1) -> Decision:
        out = self.evaluate(key, cost)
        if self.metrics is not None:
            self.metrics.record(
                out.decision,
                store_calls=out.store_calls,
                cas_retries=out.cas_retries,
                latency=out.latency,
                error=out.error,
            )
        return out.decision
