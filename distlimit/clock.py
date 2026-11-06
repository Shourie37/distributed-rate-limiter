"""Time sources. Tests and benchmarks use ManualClock to control time."""

from __future__ import annotations

import threading
import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float: ...


class SystemClock:
    def now(self) -> float:
        return time.time()


class ManualClock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self._t = start
        self._lock = threading.Lock()

    def now(self) -> float:
        with self._lock:
            return self._t

    def advance(self, seconds: float) -> None:
        with self._lock:
            self._t += seconds

    def set(self, t: float) -> None:
        with self._lock:
            self._t = t


class SkewedClock:
    """A node clock that runs ahead of (or behind) a base clock by a fixed offset."""

    def __init__(self, base: Clock, offset: float) -> None:
        self._base = base
        self.offset = offset

    def now(self) -> float:
        return self._base.now() + self.offset
