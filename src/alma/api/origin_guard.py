"""Refuse state-changing requests sent by another website (CSRF).

ALMa has no login, so a request carries no proof of who sent it. CORS does not
help: it stops a page reading a response, not sending the request. A form on any
website can POST to ``http://127.0.0.1:8000`` and the write happens. This guard
is the one place that decides, for every method that can change state, whether
the sender is ALMa's own page, a configured origin, or a non-browser client.

The decision, in order:

1. ``GET``, ``HEAD`` and ``OPTIONS`` pass. They must not write (project rule),
   and refusing ``OPTIONS`` would break CORS preflight.
2. No ``Origin`` header: browsers attach one to every unsafe request, so this is
   curl, a script or a server. It passes unless ``Sec-Fetch-Site`` says a browser
   sent it from another site.
3. ``Origin`` present: it must be an installed browser extension, a listed origin
   (``allowed_origins``), or the same origin as ``Host`` on a host DNS rebinding
   cannot reach. ``null`` matches none of these, so it is refused.

The ``Host`` restriction in step 3 closes DNS rebinding. An attacker's domain can
be re-pointed at 127.0.0.1, and then its page is genuinely same-origin with the
server, ``Origin`` and ``Host`` both naming the attacker's domain. Loopback names
and IP literals are safe because no attacker's page can be served from them
without already being ALMa. Any other hostname must be listed.

It is a pure ASGI middleware, not ``BaseHTTPMiddleware``, so request and response
bodies (uploads, NDJSON streams, ranged PDFs) pass through untouched.
"""

import ipaddress
import json
import logging
import os
from urllib.parse import urlsplit

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

ALLOWED_ORIGINS_ENV = "ALMA_ALLOWED_ORIGINS"

# Local development origins that may call the API from another port.
DEFAULT_ALLOWED_ORIGINS = (
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:8000",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:5173",
    "http://127.0.0.1:8000",
)

# Firefox sends `moz-extension://<uuid>` and Chromium `chrome-extension://<id>`.
# The per-install id cannot be listed, so the scheme is what is matched. A web
# page cannot claim one of these origins. Shared with the CORS configuration.
EXTENSION_ORIGIN_REGEX = r"^(moz-extension|chrome-extension)://.*$"
_EXTENSION_SCHEMES = ("moz-extension", "chrome-extension")

_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_LOCAL_FETCH_SITES = frozenset({"none", "same-origin"})


def _normalize_origin(value: str) -> str:
    """Return ``scheme://host[:port]``, or raise ``ValueError`` if it is not one.

    Lower-cases and drops a trailing slash and a default port, so the entry a
    deployer pastes compares equal to the ``Origin`` a browser sends.
    """
    parts = urlsplit(value.strip().lower())
    has_path = parts.path not in ("", "/")
    if parts.scheme not in ("http", "https") or not parts.hostname or has_path:
        raise ValueError(f"{value!r} is not an origin like https://alma.example.com")
    default_port = 443 if parts.scheme == "https" else 80
    port = parts.port
    host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
    authority = host if port in (None, default_port) else f"{host}:{port}"
    return f"{parts.scheme}://{authority}"


def allowed_origins() -> list[str]:
    """The origins allowed to send writes: defaults plus the deployer's own.

    A deployer adds origins through ``ALMA_ALLOWED_ORIGINS`` (comma-separated).
    ``FRONTEND_URL`` is included when set. A malformed entry raises at startup: if
    it were skipped, the deployer's browser would be refused with no visible cause.
    A wildcard is malformed on purpose, because it would reopen the hole.
    """
    configured = os.getenv(ALLOWED_ORIGINS_ENV, "").split(",")
    configured.append(os.getenv("FRONTEND_URL", ""))
    try:
        extra = [_normalize_origin(entry) for entry in configured if entry.strip()]
    except ValueError as exc:
        raise ValueError(f"Invalid {ALLOWED_ORIGINS_ENV} or FRONTEND_URL: {exc}") from exc
    return list(dict.fromkeys([*DEFAULT_ALLOWED_ORIGINS, *extra]))


def _rebinding_safe(hostname: str) -> bool:
    """True when DNS rebinding cannot put an attacker's page on this hostname."""
    if hostname == "localhost":
        return True
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return True


def origin_allowed(origin: str, host: str, allowed: frozenset[str]) -> bool:
    """Decide whether a browser request from ``origin`` may change state."""
    try:
        parts = urlsplit(origin.lower())
        if parts.scheme in _EXTENSION_SCHEMES:
            return True
        normalized = _normalize_origin(origin)
    except ValueError:
        # Includes "null": a sandboxed iframe, a data: URL, or a page that hid its
        # origin. None of these is ALMa's own page.
        return False
    if normalized in allowed:
        return True
    return parts.netloc == host.lower() and _rebinding_safe(parts.hostname or "")


class OriginGuardMiddleware:
    """Refuse unsafe requests from other websites before any route runs."""

    def __init__(self, app: ASGIApp, origins: list[str]) -> None:
        self.app = app
        self.allowed = frozenset(origins)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] in _SAFE_METHODS:
            await self.app(scope, receive, send)
            return

        headers = {name: value for name, value in scope["headers"]}
        origin = headers.get(b"origin", b"").decode("latin-1")
        host = headers.get(b"host", b"").decode("latin-1")
        fetch_site = headers.get(b"sec-fetch-site", b"").decode("latin-1").lower()

        if origin:
            permitted = origin_allowed(origin, host, self.allowed)
        else:
            permitted = not fetch_site or fetch_site in _LOCAL_FETCH_SITES

        if permitted:
            await self.app(scope, receive, send)
            return

        logger.warning(
            "Refused cross-site %s %s: Origin=%r Host=%r Sec-Fetch-Site=%r. "
            "If this is your own ALMa address, add it to %s.",
            scope["method"],
            scope["path"],
            origin,
            host,
            fetch_site,
            ALLOWED_ORIGINS_ENV,
        )
        await _send_refusal(send, origin or "this request")


async def _send_refusal(send: Send, sender: str) -> None:
    """Answer 403 in the app's error shape, naming the setting that would allow it."""
    message = (
        f"Refused: {sender} may not change data in this ALMa. If this is your own "
        f"ALMa address, add it to {ALLOWED_ORIGINS_ENV}."
    )
    body = json.dumps(
        {"error": "CrossSiteRequestRefused", "message": message, "detail": message}
    ).encode()
    await send(
        {
            "type": "http.response.start",
            "status": 403,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body})
