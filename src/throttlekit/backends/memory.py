"""In-process backend.

Each algorithm is written as an *evaluate* step that inspects state without
consuming anything, and a *commit* step that writes the new state. The backend
evaluates every rate first and commits only if all of them allow the hit, which
is the same two-phase shape the Redis Lua scripts use. All time arithmetic is
in milliseconds so the two backends agree on every decision.

State lives in a dict guarded by a ``threading.Lock``; expired keys are swept
periodically so memory stays bounded by the number of active keys.
"""

from __future__ import annotations

import math
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

from throttlekit.backends.base import EPSILON, Algorithm, Backend, now_ms
from throttlekit.clock import SystemClock
from throttlekit.rate import Decision, Rate, Result


@dataclass
class _Eval:
    ok: bool
    remaining: float  # before this hit is consumed
    retry_ms: float
    reset_ms: float
    # returns (new_state, ttl_ms, reset_ms_after_commit)
    commit: Callable[[], tuple[Any, float, float]]


# --- algorithms ---------------------------------------------------------------


def _fixed_window(state: Any, now: int, limit: int, period: int, cost: int) -> _Eval:
    window = now // period
    count = state[1] if state is not None and state[0] == window else 0
    ok = count + cost <= limit
    reset = (window + 1) * period - now

    def commit() -> tuple[Any, float, float]:
        return (window, count + cost), reset, reset

    return _Eval(ok, limit - count, 0 if ok else reset, reset, commit)


def _sliding_log(state: Any, now: int, limit: int, period: int, cost: int) -> _Eval:
    log: deque[int] = state if state is not None else deque()
    while log and log[0] <= now - period:
        log.popleft()  # pruning expired entries is safe even when not committing
    count = len(log)
    ok = count + cost <= limit
    retry = 0.0
    if not ok:
        retry = log[count + cost - limit - 1] + period - now
    reset = (log[-1] + period - now) if log else 0

    def commit() -> tuple[Any, float, float]:
        log.extend([now] * cost)
        return log, period, period

    return _Eval(ok, limit - count, retry, reset, commit)


def _sliding_window(state: Any, now: int, limit: int, period: int, cost: int) -> _Eval:
    window = now // period
    curr = prev = 0
    if state is not None:
        if state[0] == window:
            curr, prev = state[1], state[2]
        elif state[0] == window - 1:
            prev = state[1]
    elapsed = now - window * period
    estimate = prev * (1 - elapsed / period) + curr
    ok = estimate + cost <= limit + EPSILON
    reset = period - elapsed
    retry = 0.0
    if not ok:
        if curr + cost <= limit and prev > 0:
            # wait until the previous window's weight has decayed enough
            retry = period * (1 - (limit - curr - cost) / prev) - elapsed
        else:
            # wait for the next window, where today's count becomes "previous"
            need = period * (1 - (limit - cost) / curr) if curr > 0 else 0
            retry = (period - elapsed) + max(need, 0)

    def commit() -> tuple[Any, float, float]:
        return (window, curr + cost, prev), 2 * period, reset

    return _Eval(ok, limit - estimate, retry, reset, commit)


def _token_bucket(state: Any, now: int, limit: int, period: int, cost: int) -> _Eval:
    if state is None:
        tokens, ts = float(limit), now
    else:
        tokens, ts = state
        if now > ts:
            tokens = min(float(limit), tokens + (now - ts) * limit / period)
            ts = now
    ok = tokens + EPSILON >= cost
    retry = 0.0 if ok else (cost - tokens) * period / limit
    reset = (limit - tokens) * period / limit

    def commit() -> tuple[Any, float, float]:
        left = tokens - cost
        full_in = (limit - left) * period / limit
        return (left, ts), full_in, full_in

    return _Eval(ok, tokens, retry, reset, commit)


def _gcra(state: Any, now: int, limit: int, period: int, cost: int) -> _Eval:
    interval = period / limit  # emission interval
    tat = max(state if state is not None else now, now)  # theoretical arrival time
    new_tat = tat + interval * cost
    allow_at = new_tat - period
    ok = allow_at - now <= EPSILON
    remaining = (now + period - tat) / interval + EPSILON

    def commit() -> tuple[Any, float, float]:
        return new_tat, new_tat - now, new_tat - now

    return _Eval(ok, remaining, 0.0 if ok else allow_at - now, tat - now, commit)


_ALGORITHMS: dict[Algorithm, Callable[[Any, int, int, int, int], _Eval]] = {
    Algorithm.FIXED_WINDOW: _fixed_window,
    Algorithm.SLIDING_LOG: _sliding_log,
    Algorithm.SLIDING_WINDOW: _sliding_window,
    Algorithm.TOKEN_BUCKET: _token_bucket,
    Algorithm.GCRA: _gcra,
}


# --- backend ------------------------------------------------------------------


class MemoryBackend(Backend):
    """Single-process backend. Fast, exact, and not shared between workers."""

    def __init__(
        self,
        algorithm: Algorithm | str = Algorithm.GCRA,
        *,
        clock: Callable[[], float] | None = None,
        sweep_every: int = 1000,
    ) -> None:
        self.algorithm = Algorithm(algorithm)
        self._evaluate = _ALGORITHMS[self.algorithm]
        self._clock = clock or SystemClock()
        self._store: dict[str, tuple[Any, float]] = {}  # key -> (state, expires_at_ms)
        self._lock = threading.Lock()
        self._sweep_every = sweep_every
        self._ops = 0

    def __len__(self) -> int:
        return len(self._store)

    def _get(self, key: str, now: int) -> Any:
        entry = self._store.get(key)
        if entry is None or entry[1] <= now:
            return None
        return entry[0]

    def _sweep(self, now: int) -> None:
        expired = [k for k, (_, exp) in self._store.items() if exp <= now]
        for k in expired:
            del self._store[k]

    async def _hit(self, key: str, rates: tuple[Rate, ...], cost: int) -> Result:
        now = now_ms(self._clock)
        keys = [f"{key}:{r.limit}/{r.period_ms}" for r in rates]
        with self._lock:
            evals = [
                self._evaluate(self._get(k, now), now, r.limit, r.period_ms, cost)
                for k, r in zip(keys, rates)
            ]
            allowed = all(e.ok for e in evals)
            decisions = []
            for k, r, e in zip(keys, rates, evals):
                remaining, reset = e.remaining, e.reset_ms
                if allowed:
                    state, ttl, reset = e.commit()
                    self._store[k] = (state, now + max(ttl, 1))
                    remaining -= cost
                decisions.append(
                    Decision(
                        rate=r,
                        allowed=e.ok,
                        remaining=max(math.floor(remaining), 0),
                        reset_after=math.ceil(max(reset, 0)) / 1000,
                        retry_after=math.ceil(max(e.retry_ms, 0)) / 1000,
                    )
                )
            self._ops += 1
            if self._ops % self._sweep_every == 0:
                self._sweep(now)
        return Result(allowed=allowed, decisions=tuple(decisions))
