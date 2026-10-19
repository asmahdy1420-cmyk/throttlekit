"""throttlekit: rate limiting with in-memory and Redis backends and ASGI middleware."""

from __future__ import annotations

from throttlekit.backends import Algorithm, Backend, BackendError, MemoryBackend
from throttlekit.clock import ManualClock, SystemClock
from throttlekit.middleware import RateLimitMiddleware
from throttlekit.policy import Policy, RateLimiter, api_key, client_ip, compose, route
from throttlekit.rate import Decision, Rate, Result

__version__ = "0.1.0"

__all__ = [
    "Algorithm",
    "Backend",
    "BackendError",
    "Decision",
    "ManualClock",
    "MemoryBackend",
    "Policy",
    "Rate",
    "RateLimitMiddleware",
    "RateLimiter",
    "RedisBackend",
    "Result",
    "SystemClock",
    "api_key",
    "client_ip",
    "compose",
    "route",
]


def __getattr__(name: str) -> object:
    if name == "RedisBackend":
        from throttlekit.backends.redis import RedisBackend

        return RedisBackend
    raise AttributeError(name)
