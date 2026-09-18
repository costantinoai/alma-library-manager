"""Response compression that leaves already-compressed media and ranges alone.

Starlette's ``GZipMiddleware`` compresses every response above its size floor
except ``text/event-stream``. For served files that is wrong twice over:

* PDFs, images and archives are already compressed, so gzip spends CPU on
  every byte for no gain;
* a ``206 Partial Content`` answer to a ``Range`` request must carry the
  exact byte span it names — gzip rewrites the body and drops
  ``Content-Length``, which breaks resumable downloads and the progressive
  loading a phone's PDF viewer relies on.

This module keeps Starlette's responder and only widens the exclusion, so
JSON (the graph payloads the middleware exists for) is compressed exactly as
before.
"""

from __future__ import annotations

from starlette.datastructures import Headers
from starlette.middleware.gzip import (
    DEFAULT_EXCLUDED_CONTENT_TYPES,
    GZipMiddleware,
    GZipResponder,
)
from starlette.types import Message, Receive, Scope, Send

# Content-type prefixes that are already compressed (or must stream as-is).
PASSTHROUGH_CONTENT_TYPES: tuple[str, ...] = (
    *DEFAULT_EXCLUDED_CONTENT_TYPES,
    "application/pdf",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
    "application/octet-stream",
    "image/",
    "audio/",
    "video/",
    "font/woff",
)


def is_passthrough(status: int, headers: Headers) -> bool:
    """True when a response must go out byte-for-byte, uncompressed."""
    if status == 206 or "content-range" in headers:
        return True
    return headers.get("content-type", "").startswith(PASSTHROUGH_CONTENT_TYPES)


class _MediaAwareGZipResponder(GZipResponder):
    async def send_with_compression(self, message: Message) -> None:
        await super().send_with_compression(message)
        if message["type"] == "http.response.start":
            # Starlette decided its own exclusion from the headers; widen it.
            headers = Headers(raw=message["headers"])
            if is_passthrough(int(message.get("status", 200)), headers):
                self.content_type_is_excluded = True


class MediaAwareGZipMiddleware(GZipMiddleware):
    """``GZipMiddleware`` with :data:`PASSTHROUGH_CONTENT_TYPES` and 206 excluded."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and "gzip" in Headers(scope=scope).get("Accept-Encoding", ""):
            responder = _MediaAwareGZipResponder(
                self.app, self.minimum_size, compresslevel=self.compresslevel
            )
            await responder(scope, receive, send)
            return
        await super().__call__(scope, receive, send)
