from __future__ import annotations

import threading
from collections import Counter, defaultdict
from typing import Any

from .decision import Decision


class Metrics:
    """Thread-safe decision counters, in total and per tenant."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Counter[str] = Counter()
        self._tenants: defaultdict[str, Counter[str]] = defaultdict(Counter)
        self._latency_sum = 0.0
        self._latency_max = 0.0

    def record(
        self,
        decision: Decision,
        *,
        store_calls: int = 0,
        cas_retries: int = 0,
        latency: float = 0.0,
        error: str | None = None,
        tenant: str | None = None,
    ) -> None:
        outcome = "allowed" if decision.allowed else "denied"
        with self._lock:
            c = self._counters
            c["decisions"] += 1
            c[outcome] += 1
            c[f"reason.{decision.reason}"] += 1
            c["store_calls"] += store_calls
            c["cas_retries"] += cas_retries
            if error:
                c["store_errors"] += 1
                c[f"error.{error}"] += 1
            if tenant is not None:
                self._tenants[tenant][outcome] += 1
            self._latency_sum += latency
            self._latency_max = max(self._latency_max, latency)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            n = self._counters["decisions"]
            return {
                **self._counters,
                "latency_avg_ms": (self._latency_sum / n * 1000) if n else 0.0,
                "latency_max_ms": self._latency_max * 1000,
                "tenants": {t: dict(c) for t, c in self._tenants.items()},
            }

    def __getitem__(self, name: str) -> int:
        with self._lock:
            return self._counters[name]
