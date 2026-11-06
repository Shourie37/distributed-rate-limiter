"""Store access for one decision, with a shared timeout budget."""

from __future__ import annotations

import time
from typing import Any

from .store import Entry, Store, StoreContention, StoreTimeout


class StoreSession:
    __slots__ = ("_store", "_deadline", "_max_retries", "calls", "retries")

    def __init__(self, store: Store, timeout: float, max_retries: int = 64) -> None:
        self._store = store
        self._deadline = time.monotonic() + timeout
        self._max_retries = max_retries
        self.calls = 0
        self.retries = 0

    def _remaining(self) -> float:
        remaining = self._deadline - time.monotonic()
        if remaining <= 0:
            raise StoreTimeout("timeout budget spent")
        return remaining

    def get(self, key: str) -> Entry | None:
        self.calls += 1
        return self._store.get(key, timeout=self._remaining())

    def cas(self, key: str, expected_version: int | None, value: Any, ttl: float | None) -> bool:
        self.calls += 1
        return self._store.compare_and_set(key, expected_version, value, ttl, timeout=self._remaining())

    def incr(self, key: str, amount: int, ttl: float) -> int:
        self.calls += 1
        return self._store.incr(key, amount, ttl, timeout=self._remaining())

    def time(self) -> float:
        self.calls += 1
        return self._store.time(timeout=self._remaining())

    def conflict(self) -> None:
        self.retries += 1
        if self.retries > self._max_retries:
            raise StoreContention(f"gave up after {self._max_retries} CAS retries")
