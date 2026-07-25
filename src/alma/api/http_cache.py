"""HTTP transport policy for large semantic-map responses.

The application cache (materialized views) avoids recomputing layouts; this
middleware avoids retransmitting an unchanged JSON representation. It is
deliberately scoped to GET /api/v1/graphs/* so rapidly-changing API resources
retain their existing semantics.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable
from typing import Any

ASGIMessage = dict[str, Any]
ASGIScope = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]


def _header_value(headers: list[tuple[bytes, bytes]], name: bytes) -> bytes | None:
    name = name.lower()
    for key, value in headers:
        if key.lower() == name:
            return value
    return None


def _set_header(
    headers: list[tuple[bytes, bytes]],
    name: bytes,
    value: bytes,
) -> list[tuple[bytes, bytes]]:
    lowered = name.lower()
    kept = [(key, val) for key, val in headers if key.lower() != lowered]
    kept.append((name, value))
    return kept


def _drop_headers(
    headers: list[tuple[bytes, bytes]],
    *names: bytes,
) -> list[tuple[bytes, bytes]]:
    lowered = {name.lower() for name in names}
    return [(key, value) for key, value in headers if key.lower() not in lowered]


class GraphHttpCacheMiddleware:
    """Add weak ETags and truthful cache policy to graph JSON GETs.

    Weak validators are intentional: gzip may change the representation bytes
    after this middleware while the JSON meaning remains identical. A 304
    still requires the route's pure-read query, but saves the large response
    transfer and browser JSON parse/repair pass.
    """

    def __init__(self, app: Callable[..., Awaitable[None]]) -> None:
        self.app = app

    async def __call__(
        self,
        scope: ASGIScope,
        receive: ASGIReceive,
        send: ASGISend,
    ) -> None:
        if (
            scope.get("type") != "http"
            or scope.get("method") != "GET"
            or not str(scope.get("path") or "").startswith("/api/v1/graphs/")
        ):
            await self.app(scope, receive, send)
            return

        start: ASGIMessage | None = None
        body_parts: list[bytes] = []

        async def capture(message: ASGIMessage) -> None:
            nonlocal start
            if message["type"] == "http.response.start":
                start = message
                return
            if message["type"] == "http.response.body":
                body_parts.append(message.get("body", b""))
                if message.get("more_body", False):
                    return
                await self._send_cached_response(
                    scope,
                    send,
                    start,
                    b"".join(body_parts),
                )
                return
            await send(message)

        await self.app(scope, receive, capture)

    @staticmethod
    async def _send_cached_response(
        scope: ASGIScope,
        send: ASGISend,
        start: ASGIMessage | None,
        body: bytes,
    ) -> None:
        if start is None:
            return

        status = int(start.get("status") or 200)
        headers = list(start.get("headers") or [])
        content_type = (_header_value(headers, b"content-type") or b"").lower()

        if status == 202:
            # Never cache a transient "building" envelope.
            headers = _set_header(headers, b"cache-control", b"no-store")
        elif status == 200 and b"application/json" in content_type:
            digest = hashlib.sha256(body).hexdigest().encode("ascii")
            etag = b'W/"' + digest + b'"'
            headers = _set_header(headers, b"etag", etag)
            # Store privately, but always validate before reuse. This lets the
            # browser answer a 304 with its body while React Query remains the
            # authority over when a request occurs.
            headers = _set_header(
                headers,
                b"cache-control",
                b"private, no-cache",
            )
            request_headers = list(scope.get("headers") or [])
            if_none_match = _header_value(request_headers, b"if-none-match")
            validators = {
                item.strip()
                for item in (if_none_match or b"").split(b",")
                if item.strip()
            }
            if etag in validators or b"*" in validators:
                headers = _drop_headers(
                    headers,
                    b"content-length",
                    b"content-encoding",
                    b"content-type",
                )
                await send(
                    {
                        "type": "http.response.start",
                        "status": 304,
                        "headers": headers,
                    }
                )
                await send({"type": "http.response.body", "body": b""})
                return

        headers = _set_header(
            headers,
            b"content-length",
            str(len(body)).encode("ascii"),
        )
        await send({**start, "headers": headers})
        await send({"type": "http.response.body", "body": body})
