"""Policies: which requests are limited, by what key, at which rates.

Key functions take an ASGI ``scope`` and return a string, or ``None`` to say
"this policy does not apply to this request" (for example, no API key sent).
"""

from __future__ import annotations

from collections.abc import Collection, Sequence
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from throttlekit.backends.base import Backend
from throttlekit.rate import Rate, Result

Scope = dict[str, Any]
KeyFunc = Callable[[Scope], Optional[str]]


def _header(scope: Scope, name: str) -> str | None:
    target = name.lower().encode("latin-1")
    for key, value in scope.get("headers") or ():
        if key.lower() == target:
            return value.decode("latin-1")
    return None


def client_ip(*, trust_forwarded: bool = False) -> KeyFunc:
    """Key by client address.

    Only set ``trust_forwarded`` behind a proxy that overwrites
    ``X-Forwarded-For``; otherwise clients can pick their own key.
    """

    def key(scope: Scope) -> str | None:
        if trust_forwarded:
            forwarded = _header(scope, "x-forwarded-for")
            if forwarded:
                return "ip:" + forwarded.split(",")[0].strip()
        client = scope.get("client")
        return f"ip:{client[0]}" if client else None

    return key


def api_key(header: str = "x-api-key") -> KeyFunc:
    """Key by an API key header. Requests without the header are not limited by this policy."""

    def key(scope: Scope) -> str | None:
        value = _header(scope, header)
        return f"key:{value}" if value else None

    return key


def route() -> KeyFunc:
    """One shared bucket per method and path (protects an endpoint as a whole)."""

    def key(scope: Scope) -> str | None:
        return f"route:{scope.get('method', '')}:{scope.get('path', '')}"

    return key


def compose(*funcs: KeyFunc) -> KeyFunc:
    """Combine keys, e.g. ``compose(route(), client_ip())`` for per-IP-per-route."""

    def key(scope: Scope) -> str | None:
        parts = [f(scope) for f in funcs]
        if any(p is None for p in parts):
            return None
        return "|".join(p for p in parts if p is not None)

    return key


@dataclass(frozen=True)
class Policy:
    """A named set of rates applied to requests that match ``path``/``methods``.

    ``path`` is a prefix; ``None`` matches every path. Every rate in a policy is
    checked atomically: a request either counts against all of them or none.
    """

    name: str
    rates: Sequence[Rate | str]
    key: KeyFunc = field(default_factory=client_ip)
    path: str | None = None
    methods: Collection[str] | None = None
    cost: int = 1

    def __post_init__(self) -> None:
        object.__setattr__(self, "rates", tuple(Rate.coerce(r) for r in self.rates))
        if self.methods is not None:
            object.__setattr__(self, "methods", frozenset(m.upper() for m in self.methods))

    def matches(self, scope: Scope) -> bool:
        if self.path is not None and not scope.get("path", "").startswith(self.path):
            return False
        return self.methods is None or scope.get("method", "").upper() in self.methods


class RateLimiter:
    """Evaluates policies against a backend."""

    def __init__(self, backend: Backend, policies: Sequence[Policy] = ()) -> None:
        self.backend = backend
        self.policies = tuple(policies)

    async def hit(self, key: str, rates: Sequence[Rate | str], cost: int = 1) -> Result:
        """Direct use outside HTTP, e.g. in a Celery task or a CLI."""
        return await self.backend.hit(key, rates, cost)

    async def check(self, scope: Scope) -> Result | None:
        """Apply every matching policy in order; stop at the first denial.

        Returns ``None`` when no policy applies. Policies are evaluated one
        after another, so a request allowed by an earlier policy has already
        been counted there when a later policy denies it.
        """
        results: list[Result] = []
        for policy in self.policies:
            if not policy.matches(scope):
                continue
            ident = policy.key(scope)
            if ident is None:
                continue
            result = await self.backend.hit(f"{policy.name}:{ident}", policy.rates, policy.cost)
            results.append(result)
            if not result.allowed:
                break
        return Result.merge(results) if results else None
