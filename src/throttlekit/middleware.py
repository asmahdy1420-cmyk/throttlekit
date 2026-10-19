"""Pure ASGI middleware. Works with Starlette, FastAPI or any ASGI app."""

from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable

from throttlekit.backends.base import BackendError
from throttlekit.policy import RateLimiter
from throttlekit.rate import Result

logger = logging.getLogger("throttlekit")

Message = dict[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]
ASGIApp = Callable[[dict[str, Any], Receive, Send], Awaitable[None]]


def _encode(headers: dict[str, str]) -> list[tuple[bytes, bytes]]:
    return [(k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()]


class RateLimitMiddleware:
    """Rejects over-limit requests with ``429`` and annotates allowed responses.

    When the backend fails (for example Redis is down):

    * ``fail_open=True`` (default) lets the request through without headers
      and logs a warning. Availability wins over strictness.
    * ``fail_open=False`` answers ``503`` with ``Retry-After``. Use this for
      endpoints where an unthrottled burst is worse than an outage, such as
      login or payment attempts.
    """

    def __init__(
        self,
        app: ASGIApp,
        limiter: RateLimiter,
        *,
        fail_open: bool = True,
        unavailable_retry_after: int = 1,
    ) -> None:
        self.app = app
        self.limiter = limiter
        self.fail_open = fail_open
        self.unavailable_retry_after = unavailable_retry_after

    async def __call__(self, scope: dict[str, Any], receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        try:
            result = await self.limiter.check(scope)
        except BackendError:
            logger.warning(
                "rate limit backend unavailable, failing %s",
                "open" if self.fail_open else "closed",
                exc_info=True,
            )
            if self.fail_open:
                await self.app(scope, receive, send)
            else:
                await self._respond(
                    send,
                    503,
                    {"Retry-After": str(self.unavailable_retry_after)},
                    "Rate limiting unavailable",
                )
            return

        if result is None:
            await self.app(scope, receive, send)
            return
        if not result.allowed:
            await self._respond(send, 429, result.headers(), "Too Many Requests")
            return

        await self.app(scope, receive, self._with_headers(send, result))

    @staticmethod
    def _with_headers(send: Send, result: Result) -> Send:
        extra = _encode(result.headers())

        async def wrapped(message: Message) -> None:
            if message["type"] == "http.response.start":
                message = {**message, "headers": [*message.get("headers", []), *extra]}
            await send(message)

        return wrapped

    @staticmethod
    async def _respond(send: Send, status: int, headers: dict[str, str], detail: str) -> None:
        body = json.dumps({"detail": detail}).encode()
        raw = [
            *_encode(headers),
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
        ]
        await send({"type": "http.response.start", "status": status, "headers": raw})
        await send({"type": "http.response.body", "body": body})
