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

from starlette.middleware.gzip import DEFAULT_EXCLUDED_CONTENT_TYPES, GZipMiddleware
from starlette.types import ASGIApp

# Media types that are already compressed, or must stream as they are. Starlette
# passes a 206 through by itself.
PASSTHROUGH_CONTENT_TYPES: tuple[str, ...] = (
    *DEFAULT_EXCLUDED_CONTENT_TYPES,
    "application/pdf",
    "application/octet-stream",
    "image/*",
    "font/*",
)


class MediaAwareGZipMiddleware(GZipMiddleware):
    """``GZipMiddleware`` that leaves :data:`PASSTHROUGH_CONTENT_TYPES` alone."""

    def __init__(self, app: ASGIApp, minimum_size: int = 500, compresslevel: int = 9) -> None:
        super().__init__(
            app,
            minimum_size,
            compresslevel,
            exclude_content_types=PASSTHROUGH_CONTENT_TYPES,
        )
