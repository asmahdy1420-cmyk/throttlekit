"""Backend interface shared by the in-memory and Redis implementations."""

from __future__ import annotations

import abc
import enum
from collections.abc import Sequence
from typing import Callable

from throttlekit.rate import Rate, Result


class Algorithm(str, enum.Enum):
    FIXED_WINDOW = "fixed_window"
    SLIDING_LOG = "sliding_log"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"
    GCRA = "gcra"


class BackendError(Exception):
    """The backing store could not answer. The middleware maps this to fail-open/closed."""


# Floating-point tolerance in milliseconds / tokens. Shared by Python and Lua code
# so both backends make identical decisions on the same inputs.
EPSILON = 1e-6


def now_ms(clock: Callable[[], float]) -> int:
    """Read a seconds clock as integer milliseconds."""
    return round(clock() * 1000)


class Backend(abc.ABC):
    """Applies one algorithm to keys. All rates in one call are all-or-nothing."""

    algorithm: Algorithm

    async def hit(self, key: str, rates: Sequence[Rate | str], cost: int = 1) -> Result:
        """Consume ``cost`` units from every rate on ``key`` if all of them allow it."""
        parsed = tuple(Rate.coerce(r) for r in rates)
        if not parsed:
            raise ValueError("at least one rate is required")
        if cost < 1:
            raise ValueError("cost must be at least 1")
        for rate in parsed:
            if cost > rate.limit:
                raise ValueError(f"cost {cost} can never fit in rate {rate}")
        return await self._hit(key, parsed, cost)

    @abc.abstractmethod
    async def _hit(self, key: str, rates: tuple[Rate, ...], cost: int) -> Result: ...

    async def close(self) -> None:  # noqa: B027 - optional hook
        """Release resources. Optional."""
