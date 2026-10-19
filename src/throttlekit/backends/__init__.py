from __future__ import annotations

from throttlekit.backends.base import Algorithm, Backend, BackendError
from throttlekit.backends.memory import MemoryBackend

__all__ = ["Algorithm", "Backend", "BackendError", "MemoryBackend", "RedisBackend"]


def __getattr__(name: str) -> object:
    # Imported lazily so the core package works without the redis client installed.
    if name == "RedisBackend":
        from throttlekit.backends.redis import RedisBackend

        return RedisBackend
    raise AttributeError(name)
