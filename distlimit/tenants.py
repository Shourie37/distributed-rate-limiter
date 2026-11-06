"""Multi-tenant limits: one limit per tenant and one limit per key in a tenant."""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Mapping

from .algorithms import Algorithm
from .clock import Clock
from .decision import Decision
from .limiter import FailurePolicy, Limiter, Outcome
from .metrics import Metrics
from .store import Store


@dataclass(frozen=True)
class TenantPolicy:
    # Shared by all keys of the tenant.
    tenant_limit: Algorithm | None = None
    # Applied to each key on its own.
    key_limit: Algorithm | None = None
    # Per-key replacements for key_limit, for example a paid API key.
    key_overrides: Mapping[str, Algorithm] = field(default_factory=dict)


@dataclass
class _TenantLimiters:
    tenant: Limiter | None
    key: Limiter | None
    overrides: dict[str, Limiter]


class TenantRateLimiter:
    def __init__(
        self,
        store: Store,
        default: TenantPolicy,
        *,
        clock: Clock | None = None,
        policy: FailurePolicy | None = None,
        metrics: Metrics | None = None,
        use_store_time: bool = False,
    ) -> None:
        self.store = store
        self.clock = clock
        self.policy = policy
        self.metrics = metrics or Metrics()
        self.use_store_time = use_store_time
        self._default = default
        self._tenants: dict[str, _TenantLimiters] = {}
        self._lock = threading.Lock()

    def _limiter(self, algorithm: Algorithm | None, prefix: str) -> Limiter | None:
        if algorithm is None:
            return None
        return Limiter(
            algorithm,
            self.store,
            clock=self.clock,
            policy=self.policy,
            use_store_time=self.use_store_time,
            prefix=f"{prefix}/{algorithm.name}",
        )

    def _build(self, tenant: str, p: TenantPolicy) -> _TenantLimiters:
        return _TenantLimiters(
            tenant=self._limiter(p.tenant_limit, f"t/{tenant}/all"),
            key=self._limiter(p.key_limit, f"t/{tenant}/key"),
            overrides={
                k: lim
                for k, a in p.key_overrides.items()
                if (lim := self._limiter(a, f"t/{tenant}/override")) is not None
            },
        )

    def set_tenant(self, tenant: str, policy: TenantPolicy) -> None:
        built = self._build(tenant, policy)
        with self._lock:
            self._tenants[tenant] = built

    def _get(self, tenant: str) -> _TenantLimiters:
        with self._lock:
            found = self._tenants.get(tenant)
            if found is None:
                found = self._tenants[tenant] = self._build(tenant, self._default)
            return found

    def check(self, tenant: str, key: str, cost: int = 1) -> Decision:
        """Check the key limit first, then the tenant limit.

        A key that passes but then hits the tenant limit still spends its
        key token. That keeps each check to one atomic step per limit.
        """
        lims = self._get(tenant)
        outcomes: list[Outcome] = []
        key_lim = lims.overrides.get(key, lims.key)
        for lim, store_key in ((key_lim, key), (lims.tenant, "*")):
            if lim is None:
                continue
            outcomes.append(lim.evaluate(store_key, cost))
            if not outcomes[-1].decision.allowed:
                break
        decision = _combine([o.decision for o in outcomes])
        self.metrics.record(
            decision,
            store_calls=sum(o.store_calls for o in outcomes),
            cas_retries=sum(o.cas_retries for o in outcomes),
            latency=sum(o.latency for o in outcomes),
            error=next((o.error for o in outcomes if o.error), None),
            tenant=tenant,
        )
        return decision


def _combine(decisions: list[Decision]) -> Decision:
    if not decisions:
        return Decision(True, 0, 0)
    last = decisions[-1]
    if not last.allowed:
        return last
    tightest = min(decisions, key=lambda d: d.remaining)
    return Decision(
        True,
        tightest.limit,
        tightest.remaining,
        delay=max(d.delay for d in decisions),
        reason=next((d.reason for d in decisions if d.reason != "ok"), "ok"),
    )
