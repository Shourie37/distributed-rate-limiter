from __future__ import annotations

from dataclasses import dataclass

OK = "ok"
LIMITED = "limited"
FAIL_OPEN = "fail_open"
FAIL_CLOSED = "fail_closed"


@dataclass(frozen=True, slots=True)
class Decision:
    allowed: bool
    limit: int
    remaining: int
    # Seconds until a retry can pass. 0 when allowed.
    retry_after: float = 0.0
    # Leaky bucket only: wait this long before the request is processed.
    delay: float = 0.0
    reason: str = OK
