from __future__ import annotations

import sys
import threading
from typing import Callable

import pytest

from distlimit import Decision, ManualClock


@pytest.fixture
def clock() -> ManualClock:
    # Aligned to every window size used in the tests.
    return ManualClock(1000.0)


@pytest.fixture
def fast_switch():
    # Make threads switch often so races between read and write show up.
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    yield
    sys.setswitchinterval(old)


def hammer(nodes: list[Callable[[str], Decision]], threads_per_node: int, attempts: int, key: str = "hot") -> int:
    admitted = 0
    lock = threading.Lock()
    barrier = threading.Barrier(len(nodes) * threads_per_node)

    def worker(acquire: Callable[[str], Decision]) -> None:
        nonlocal admitted
        barrier.wait()
        n = sum(acquire(key).allowed for _ in range(attempts))
        with lock:
            admitted += n

    threads = [threading.Thread(target=worker, args=(n,)) for n in nodes for _ in range(threads_per_node)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return admitted
