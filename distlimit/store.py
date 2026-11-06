"""Shared state store.

The interface is small on purpose. It maps to Redis (WATCH/MULTI or a Lua
script, INCRBY + EXPIRE NX) and to DynamoDB (conditional PutItem on a
version attribute, UpdateItem ADD with a TTL attribute).
"""

from __future__ import annotations

import random
import threading
import time
from abc import ABC, abstractmethod
from collections import Counter
from dataclasses import dataclass
from typing import Any

from .clock import Clock, SystemClock


class StoreError(Exception):
    """The store failed or did not answer in time."""


class StoreUnavailable(StoreError):
    pass


class StoreTimeout(StoreError):
    pass


class StoreContention(StoreError):
    """Too many compare-and-set conflicts on one key."""


@dataclass(frozen=True, slots=True)
class Entry:
    value: Any
    version: int


class Store(ABC):
    @abstractmethod
    def get(self, key: str, timeout: float | None = None) -> Entry | None:
        """Return the live entry for key, or None if it is missing or expired."""

    @abstractmethod
    def compare_and_set(
        self,
        key: str,
        expected_version: int | None,
        value: Any,
        ttl: float | None,
        timeout: float | None = None,
    ) -> bool:
        """Write value only if the current version matches.

        expected_version=None means "write only if the key does not exist".
        """

    @abstractmethod
    def incr(self, key: str, amount: int, ttl: float, timeout: float | None = None) -> int:
        """Add amount to an integer key and return the new value.

        A missing or expired key starts at 0 and gets the TTL. An existing key keeps its TTL.
        """

    @abstractmethod
    def time(self, timeout: float | None = None) -> float:
        """Return the store's own clock, like Redis TIME."""


class InMemoryStore(Store):
    """Thread-safe store with versions and lazy TTL expiry."""

    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or SystemClock()
        self._data: dict[str, tuple[Any, int, float | None]] = {}
        self._lock = threading.Lock()
        self._version = 0
        self.calls: Counter[str] = Counter()

    @property
    def total_calls(self) -> int:
        with self._lock:
            return sum(self.calls.values())

    def reset_calls(self) -> None:
        with self._lock:
            self.calls.clear()

    def _live(self, key: str, now: float) -> tuple[Any, int, float | None] | None:
        record = self._data.get(key)
        if record is None:
            return None
        expires_at = record[2]
        if expires_at is not None and now >= expires_at:
            del self._data[key]
            return None
        return record

    def _next_version(self) -> int:
        # Versions never repeat, so a key that expires and comes back cannot pass an old CAS.
        self._version += 1
        return self._version

    def get(self, key: str, timeout: float | None = None) -> Entry | None:
        now = self._clock.now()
        with self._lock:
            self.calls["get"] += 1
            record = self._live(key, now)
            return None if record is None else Entry(record[0], record[1])

    def compare_and_set(
        self,
        key: str,
        expected_version: int | None,
        value: Any,
        ttl: float | None,
        timeout: float | None = None,
    ) -> bool:
        now = self._clock.now()
        with self._lock:
            self.calls["cas"] += 1
            record = self._live(key, now)
            current = None if record is None else record[1]
            if current != expected_version:
                return False
            expires_at = None if ttl is None else now + ttl
            self._data[key] = (value, self._next_version(), expires_at)
            return True

    def incr(self, key: str, amount: int, ttl: float, timeout: float | None = None) -> int:
        now = self._clock.now()
        with self._lock:
            self.calls["incr"] += 1
            record = self._live(key, now)
            if record is None:
                value, expires_at = amount, now + ttl
            else:
                value, expires_at = record[0] + amount, record[2]
            self._data[key] = (value, self._next_version(), expires_at)
            return value

    def time(self, timeout: float | None = None) -> float:
        with self._lock:
            self.calls["time"] += 1
        return self._clock.now()

    def purge_expired(self) -> int:
        now = self._clock.now()
        with self._lock:
            expired = [k for k, r in self._data.items() if r[2] is not None and now >= r[2]]
            for k in expired:
                del self._data[k]
            return len(expired)

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


class FaultyStore(Store):
    """Wraps a store and injects latency, errors, or a full outage."""

    def __init__(
        self,
        inner: Store,
        *,
        latency: float = 0.0,
        error_rate: float = 0.0,
        down: bool = False,
        seed: int = 0,
    ) -> None:
        self.inner = inner
        self.latency = latency
        self.error_rate = error_rate
        self.down = down
        self._rng = random.Random(seed)
        self._lock = threading.Lock()

    def _before(self, timeout: float | None) -> None:
        if self.down:
            raise StoreUnavailable("store is down")
        if self.error_rate:
            with self._lock:
                failed = self._rng.random() < self.error_rate
            if failed:
                raise StoreUnavailable("injected error")
        if self.latency > 0:
            if timeout is not None and self.latency > timeout:
                time.sleep(max(timeout, 0.0))
                raise StoreTimeout(f"no answer within {timeout:.3f}s")
            time.sleep(self.latency)

    def get(self, key: str, timeout: float | None = None) -> Entry | None:
        self._before(timeout)
        return self.inner.get(key)

    def compare_and_set(
        self,
        key: str,
        expected_version: int | None,
        value: Any,
        ttl: float | None,
        timeout: float | None = None,
    ) -> bool:
        self._before(timeout)
        return self.inner.compare_and_set(key, expected_version, value, ttl)

    def incr(self, key: str, amount: int, ttl: float, timeout: float | None = None) -> int:
        self._before(timeout)
        return self.inner.incr(key, amount, ttl)

    def time(self, timeout: float | None = None) -> float:
        self._before(timeout)
        return self.inner.time()
