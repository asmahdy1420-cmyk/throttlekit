from __future__ import annotations

import fakeredis
import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from throttlekit import (
    Algorithm,
    ManualClock,
    MemoryBackend,
    Policy,
    RateLimiter,
    RateLimitMiddleware,
    api_key,
    client_ip,
)
from throttlekit.backends.redis import RedisBackend


async def ok(request):
    return JSONResponse({"ok": True})


def build_app(limiter: RateLimiter, **options) -> Starlette:
    app = Starlette(routes=[Route("/items", ok), Route("/login", ok, methods=["POST"])])
    app.add_middleware(RateLimitMiddleware, limiter=limiter, **options)
    return app


def client_for(app: Starlette) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, client=("203.0.113.7", 4000))
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


async def test_returns_429_with_headers(clock):
    limiter = RateLimiter(
        MemoryBackend(Algorithm.SLIDING_LOG, clock=clock),
        [Policy("global", ["2/s", "100/h"], key=client_ip())],
    )
    async with client_for(build_app(limiter)) as http:
        first = await http.get("/items")
        assert first.status_code == 200
        assert first.headers["RateLimit-Limit"] == "2"
        assert first.headers["RateLimit-Remaining"] == "1"
        assert first.headers["RateLimit-Policy"] == "2;w=1, 100;w=3600"
        await http.get("/items")

        blocked = await http.get("/items")
        assert blocked.status_code == 429
        assert blocked.json() == {"detail": "Too Many Requests"}
        assert blocked.headers["Retry-After"] == "1"
        assert blocked.headers["RateLimit-Remaining"] == "0"

        clock.advance(1)
        assert (await http.get("/items")).status_code == 200


async def test_per_route_and_per_api_key_policies(clock):
    limiter = RateLimiter(
        MemoryBackend(Algorithm.TOKEN_BUCKET, clock=clock),
        [
            Policy("login", ["1/m"], key=client_ip(), path="/login", methods=["POST"]),
            Policy("api", ["3/s"], key=api_key()),
        ],
    )
    async with client_for(build_app(limiter)) as http:
        assert (await http.post("/login")).status_code == 200
        assert (await http.post("/login")).status_code == 429
        # /items is not covered by the login policy, and has no API key: unlimited.
        for _ in range(5):
            response = await http.get("/items")
            assert response.status_code == 200
            assert "RateLimit-Limit" not in response.headers

        statuses = [
            (await http.get("/items", headers={"X-API-Key": "abc"})).status_code for _ in range(4)
        ]
        assert statuses == [200, 200, 200, 429]
        # A different key has its own budget.
        assert (await http.get("/items", headers={"X-API-Key": "xyz"})).status_code == 200


@pytest.mark.parametrize(("fail_open", "status"), [(True, 200), (False, 503)])
async def test_redis_outage_fails_open_or_closed(clock, fail_open, status):
    server = fakeredis.FakeServer()
    redis_client = fakeredis.FakeAsyncRedis(server=server)
    limiter = RateLimiter(
        RedisBackend(redis_client, Algorithm.GCRA, clock=clock),
        [Policy("global", ["10/s"])],
    )
    async with client_for(build_app(limiter, fail_open=fail_open)) as http:
        assert (await http.get("/items")).status_code == 200
        server.connected = False
        response = await http.get("/items")
        assert response.status_code == status
        if not fail_open:
            assert response.headers["Retry-After"] == "1"
    await redis_client.aclose()
