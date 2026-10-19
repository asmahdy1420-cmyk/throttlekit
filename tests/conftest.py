from __future__ import annotations

import os
import uuid
from typing import Callable

import fakeredis
import pytest
import redis.asyncio as aioredis

from throttlekit import Algorithm, Backend, ManualClock, MemoryBackend
from throttlekit.backends.redis import RedisBackend

BACKENDS = ["memory", "fakeredis"]
if os.environ.get("REDIS_URL"):
    BACKENDS.append("redis")

BackendFactory = Callable[[Algorithm], Backend]


@pytest.fixture
def clock() -> ManualClock:
    # Start on an exact second so window boundaries are predictable.
    return ManualClock(start=1_700_000_000.0)


@pytest.fixture(params=BACKENDS)
async def make_backend(request: pytest.FixtureRequest, clock: ManualClock):
    clients = []

    def factory(algorithm: Algorithm) -> Backend:
        if request.param == "memory":
            return MemoryBackend(algorithm, clock=clock)
        if request.param == "fakeredis":
            client = fakeredis.FakeAsyncRedis()
        else:
            client = aioredis.from_url(os.environ["REDIS_URL"])
        clients.append(client)
        # A unique prefix isolates tests that share a real Redis.
        return RedisBackend(client, algorithm, clock=clock, prefix=f"test:{uuid.uuid4().hex}:")

    yield factory
    for client in clients:
        await client.aclose()
