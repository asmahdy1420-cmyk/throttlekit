"""Rates, decisions and results."""

from __future__ import annotations

import functools
import math
import re
from dataclasses import dataclass, field

_UNITS = {
    "ms": 0.001,
    "s": 1.0,
    "sec": 1.0,
    "second": 1.0,
    "m": 60.0,
    "min": 60.0,
    "minute": 60.0,
    "h": 3600.0,
    "hour": 3600.0,
    "d": 86400.0,
    "day": 86400.0,
}

_RATE_RE = re.compile(
    r"^\s*(?P<limit>\d+)\s*(?:/|per)\s*(?P<count>\d+(?:\.\d+)?)?\s*(?P<unit>[a-z]+)\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Rate:
    """``limit`` requests per ``period`` seconds."""

    limit: int
    period: float
    period_ms: int = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be at least 1")
        if self.period <= 0:
            raise ValueError("period must be positive")
        object.__setattr__(self, "period_ms", round(self.period * 1000))
        if self.period_ms < 1:
            raise ValueError("period must be at least one millisecond")

    @classmethod
    def parse(cls, text: str) -> Rate:
        """Parse ``"10/s"``, ``"1000/h"``, ``"100/5m"`` or ``"5 per minute"``."""
        return _parse(text)

    @classmethod
    def _parse_uncached(cls, text: str) -> Rate:
        match = _RATE_RE.match(text)
        if match is None:
            raise ValueError(f"invalid rate: {text!r}")
        unit = match.group("unit").lower()
        if unit not in _UNITS and unit.endswith("s"):
            unit = unit[:-1]
        if unit not in _UNITS:
            raise ValueError(f"unknown time unit in rate: {text!r}")
        count = float(match.group("count") or 1)
        return cls(limit=int(match.group("limit")), period=count * _UNITS[unit])

    @classmethod
    def coerce(cls, value: Rate | str) -> Rate:
        return value if isinstance(value, Rate) else cls.parse(value)

    def __str__(self) -> str:
        period = f"{self.period:g}s"
        return f"{self.limit}/{period}"


@dataclass(frozen=True)
class Decision:
    """The outcome for one rate on one key."""

    rate: Rate
    allowed: bool
    remaining: int
    reset_after: float
    retry_after: float

    @property
    def limit(self) -> int:
        return self.rate.limit


@functools.lru_cache(maxsize=256)
def _parse(text: str) -> Rate:
    # Policies and call sites pass the same few strings on every request.
    return Rate._parse_uncached(text)


@dataclass(frozen=True)
class Result:
    """The outcome of one hit across one or more rates.

    ``allowed`` is true only when every rate allowed the hit. Hits are
    all-or-nothing: if one rate denies, no rate is consumed.
    """

    allowed: bool
    decisions: tuple[Decision, ...] = field(default_factory=tuple)

    @property
    def binding(self) -> Decision:
        """The decision that should be reported to the client.

        When denied, the denying rate with the longest wait. When allowed,
        the rate with the fewest requests left.
        """
        if not self.decisions:
            raise ValueError("empty result")
        if not self.allowed:
            denied = [d for d in self.decisions if not d.allowed]
            return max(denied, key=lambda d: d.retry_after)
        return min(self.decisions, key=lambda d: (d.remaining, -d.reset_after))

    @property
    def retry_after(self) -> float:
        return 0.0 if self.allowed else self.binding.retry_after

    def headers(self) -> dict[str, str]:
        """``RateLimit-*`` headers (IETF draft) plus ``Retry-After`` on denial."""
        d = self.binding
        headers = {
            "RateLimit-Limit": str(d.limit),
            "RateLimit-Remaining": str(max(d.remaining, 0)),
            "RateLimit-Reset": str(math.ceil(d.reset_after)),
            "RateLimit-Policy": ", ".join(
                f"{x.limit};w={math.ceil(x.rate.period)}" for x in self.decisions
            ),
        }
        if not self.allowed:
            headers["Retry-After"] = str(max(1, math.ceil(d.retry_after)))
        return headers

    @classmethod
    def merge(cls, results: list[Result]) -> Result:
        decisions: list[Decision] = []
        for r in results:
            decisions.extend(r.decisions)
        return cls(allowed=all(r.allowed for r in results), decisions=tuple(decisions))
