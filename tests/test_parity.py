"""The Lua scripts and the Python implementations must make identical decisions."""

from __future__ import annotations

import random

import fakeredis
import pytest

from throttlekit import Algorithm, ManualClock, MemoryBackend
from throttlekit.backends.redis import RedisBackend


@pytest.mark.parametrize("algorithm", list(Algorithm))
async def test_memory_and_redis_agree(algorithm):
    rng = random.Random(42)
    clock = ManualClock(start=1_700_000_000.0)
    memory = MemoryBackend(algorithm, clock=clock)
    client = fakeredis.FakeAsyncRedis()
    redis_backend = RedisBackend(client, algorithm, clock=clock)
    rates = ["7/s", "40/10s"]

    for _ in range(400):
        clock.advance(rng.choice([0, 0, 0.013, 0.05, 0.137, 0.4, 1.3]))
        key = rng.choice(["a", "b"])
        cost = rng.choice([1, 1, 1, 2, 3])
        expected = await memory.hit(key, rates, cost)
        actual = await redis_backend.hit(key, rates, cost)
        assert actual == expected

    await client.aclose()


async def test_redis_server_clock_mode():
    """Without a client clock the scripts read Redis TIME instead."""
    client = fakeredis.FakeAsyncRedis()
    backend = RedisBackend(client, Algorithm.TOKEN_BUCKET)
    results = [await backend.hit("k", ["3/h"]) for _ in range(4)]
    assert [r.allowed for r in results] == [True, True, True, False]
    assert backend.key_for("k", results[0].binding.rate) == "throttlekit:token_bucket:{k}:3/3600000"
    await client.aclose()


async def test_memory_backend_sweeps_expired_keys():
    clock = ManualClock()
    backend = MemoryBackend(Algorithm.FIXED_WINDOW, clock=clock, sweep_every=10)
    for i in range(9):
        await backend.hit(f"user-{i}", ["1/s"])
    assert len(backend) == 9
    clock.advance(2)
    await backend.hit("last", ["1/s"])  # tenth operation triggers the sweep
    assert len(backend) == 1
