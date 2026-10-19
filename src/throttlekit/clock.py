"""Clocks.

Every backend reads time through a clock object so tests can drive time
explicitly instead of sleeping. A clock is any zero-argument callable that
returns the current time in seconds as a float.
"""

from __future__ import annotations

import time


class SystemClock:
    """Wall-clock time (``time.time``)."""

    def __call__(self) -> float:
        return time.time()


class ManualClock:
    """A clock that only moves when told to. Intended for tests."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._now = float(start)

    def __call__(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        if seconds < 0:
            raise ValueError("a clock cannot go backwards")
        self._now += seconds

    def set(self, now: float) -> None:
        self._now = float(now)
