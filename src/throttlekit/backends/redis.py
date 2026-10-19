"""Redis backend: one atomic Lua script per algorithm.

Keys for the rates of one call share a hash tag (``{...}``), so the backend
also works on Redis Cluster: a multi-key script only needs its keys in one slot.
"""

from __future__ import annotations

import math
import uuid
from typing import Any, Callable

from redis.exceptions import RedisError

from throttlekit.backends.base import Algorithm, Backend, BackendError, now_ms
from throttlekit.backends.lua import SCRIPTS
from throttlekit.rate import Decision, Rate, Result


class RedisBackend(Backend):
    """Shared backend for many processes.

    ``client`` is a ``redis.asyncio.Redis`` (or a compatible fake). With no
    ``clock``, the scripts read the Redis server clock, so app servers with
    skewed clocks still agree. Pass a clock to drive time from the client side,
    which is what the tests do.
    """

    def __init__(
        self,
        client: Any,
        algorithm: Algorithm | str = Algorithm.GCRA,
        *,
        prefix: str = "throttlekit:",
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.algorithm = Algorithm(algorithm)
        self._client = client
        self._prefix = prefix
        self._clock = clock
        self._script = client.register_script(SCRIPTS[self.algorithm.value])

    def key_for(self, key: str, rate: Rate) -> str:
        return f"{self._prefix}{self.algorithm.value}:{{{key}}}:{rate.limit}/{rate.period_ms}"

    async def _hit(self, key: str, rates: tuple[Rate, ...], cost: int) -> Result:
        keys = [self.key_for(key, r) for r in rates]
        args: list[Any] = [
            "" if self._clock is None else now_ms(self._clock),
            cost,
            uuid.uuid4().hex,
        ]
        for r in rates:
            args.extend((r.limit, r.period_ms))
        try:
            raw = await self._script(keys=keys, args=args)
        except (RedisError, OSError) as exc:
            raise BackendError(f"redis unavailable: {exc}") from exc

        values = [int(v) for v in raw]
        decisions = []
        for i, rate in enumerate(rates):
            ok, remaining, retry_ms, reset_ms = values[1 + 4 * i : 5 + 4 * i]
            decisions.append(
                Decision(
                    rate=rate,
                    allowed=bool(ok),
                    remaining=remaining,
                    reset_after=math.ceil(reset_ms) / 1000,
                    retry_after=math.ceil(retry_ms) / 1000,
                )
            )
        return Result(allowed=bool(values[0]), decisions=tuple(decisions))
