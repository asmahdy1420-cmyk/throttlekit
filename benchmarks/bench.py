"""Throughput of each algorithm on each backend.

    python benchmarks/bench.py                 # memory + fakeredis
    REDIS_URL=redis://localhost:6379/0 python benchmarks/bench.py   # also real Redis

Numbers are single-coroutine, sequential calls: they measure per-call cost,
not the throughput of a Redis server under concurrent load.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from throttlekit import Algorithm, MemoryBackend
from throttlekit.backends.base import Backend

RATES = ["100/s", "10000/h"]


async def measure(backend: Backend, n: int, keys: int) -> float:
    start = time.perf_counter()
    for i in range(n):
        await backend.hit(f"user-{i % keys}", RATES)
    return n / (time.perf_counter() - start)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-n", type=int, default=20_000, help="hits per run")
    parser.add_argument("--keys", type=int, default=1_000, help="distinct keys")
    args = parser.parse_args()

    targets: list[tuple[str, object]] = [("memory", None)]
    try:
        import fakeredis

        targets.append(("fakeredis", fakeredis.FakeAsyncRedis))
    except ImportError:
        pass
    if os.environ.get("REDIS_URL"):
        import redis.asyncio as aioredis

        targets.append(("redis", lambda: aioredis.from_url(os.environ["REDIS_URL"])))

    print(f"{'algorithm':<16}" + "".join(f"{name:>14}" for name, _ in targets) + "   (hits/s)")
    for algorithm in Algorithm:
        row = f"{algorithm.value:<16}"
        for name, factory in targets:
            client = None
            if factory is None:
                backend: Backend = MemoryBackend(algorithm)
                n = args.n
            else:
                from throttlekit.backends.redis import RedisBackend

                client = factory()
                backend = RedisBackend(client, algorithm, prefix=f"bench:{time.time_ns()}:")
                n = args.n // 10 if name == "fakeredis" else args.n
            row += f"{await measure(backend, n, args.keys):>14,.0f}"
            if client is not None:
                await client.aclose()
        print(row)


if __name__ == "__main__":
    asyncio.run(main())
