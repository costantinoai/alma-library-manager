"""Refuse requests that reach ALMa under a strange name or from another website.

ALMa has no login, so a request carries no proof of who sent it. CORS does not
help: it stops a page reading a response, not sending the request, and it does
nothing for a page that the browser believes is same-origin. This guard is the
one place that decides, before any route runs, whether a request is addressed to
this ALMa and, for every method that can change state, whether the sender is
ALMa's own page, a configured origin, or a non-browser client.

The decision, in order:

1. ``Host``, on every method: it must name loopback, an IP literal or an address
   the deployer listed, the health route also answers to the internal service
   name. This closes DNS rebinding. An
   attacker's domain can be re-pointed at 127.0.0.1, and then its page is genuinely
   same-origin with the server: the browser sends no ``Origin`` on a read, and CORS
   never applies. Only the ``Host`` still names the attacker. Loopback names and IP
   literals are safe because no attacker's page can be served from them without
   already being ALMa. Any other name must be listed.
2. ``GET``, ``HEAD`` and ``OPTIONS`` then pass. They must not write (project rule),
   and refusing ``OPTIONS`` would break CORS preflight.
3. No ``Origin`` header: browsers attach one to every unsafe request, so this is
   curl, a script or a server. It passes unless ``Sec-Fetch-Site`` says a browser
   sent it from another site.
4. ``Origin`` present: it must be an installed browser extension, a listed origin
   (``allowed_origins``), or the same origin as ``Host`` on a host DNS rebinding
   cannot reach. ``null`` matches none of these, so it is refused.

Proxies must keep the ``Host`` the browser sent, which is then the listed address.
Tailscale Serve does. A proxy that replaces ``Host`` erases the one fact that gives
a rebound page away, so its upstream name is not accepted for the API, and listing
that name reopens the hole. Forwarded headers are never read: any client can write
them, so they cannot vouch for anything.

It is a pure ASGI middleware, not ``BaseHTTPMiddleware``, so request and response
bodies (uploads, NDJSON streams, ranged PDFs) pass through untouched.
"""

import ipaddress
import json
import logging
import os
import re
from urllib.parse import urlsplit

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)

ALLOWED_ORIGINS_ENV = "ALMA_ALLOWED_ORIGINS"

# The address a sidecar on the same Docker network uses to check on this container:
# every shipped launcher names it `alma`, and the image listens on 8000. It reaches
# the health route and nothing else. A proxy with a default config sends this same
# Host for whatever page reached it, so on any other route it proves nothing, and
# the health route holds no private data.
INTERNAL_SERVICE_AUTHORITY = "alma:8000"
INTERNAL_HEALTH_PATH = "/api/v1/health"

# Firefox sends `moz-extension://<uuid>` and Chromium `chrome-extension://<id>`.
# The per-install id cannot be listed, so the scheme is what is matched. A web
# page cannot claim one of these origins. Shared with the CORS configuration.
EXTENSION_ORIGIN_REGEX = r"^(moz-extension|chrome-extension)://.*$"
_EXTENSION_SCHEMES = ("moz-extension", "chrome-extension")

_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
_LOCAL_FETCH_SITES = frozenset({"none", "same-origin"})
_DEFAULT_PORTS = {"http": 80, "https": 443}

# `name[:port]` or `[ipv6][:port]`, nothing else. Userinfo, a path, a wildcard or a
# stray character fails here instead of being quietly dropped by a URL parser.
_AUTHORITY = re.compile(r"(?P<host>\[[0-9a-f:.]+\]|[a-z0-9._-]+)(?::(?P<port>[0-9]{1,5}))?")


def _split_authority(value: str) -> tuple[str, int | None]:
    """Split a lower-case ``host[:port]``, or raise ``ValueError`` if it is not one."""
    match = _AUTHORITY.fullmatch(value)
    port = int(match["port"]) if match and match["port"] else None
    if match is None or (port is not None and port > 65535):
        raise ValueError(f"{value!r} is not a host with an optional port")
    return match["host"], port


def _normalize_origin(value: str) -> str:
    """Return ``scheme://host[:port]``, or raise ``ValueError`` if it is not one.

    Lower-cases and drops a trailing slash and a default port, so the entry a
    deployer pastes compares equal to the ``Origin`` a browser sends.
    """
    text = value.strip().lower()
    parts = urlsplit(text)
    not_an_origin = (
        parts.scheme not in _DEFAULT_PORTS
        or parts.path not in ("", "/")
        or "?" in text
        or "#" in text
    )
    if not_an_origin:
        raise ValueError(f"{value!r} is not an origin like https://alma.example.com")
    try:
        host, port = _split_authority(parts.netloc)
    except ValueError as exc:
        raise ValueError(f"{value!r} is not an origin like https://alma.example.com") from exc
    authority = host if port in (None, _DEFAULT_PORTS[parts.scheme]) else f"{host}:{port}"
    return f"{parts.scheme}://{authority}"


def allowed_origins() -> list[str]:
    """The origins a deployer listed: the pages allowed to send writes.

    Nothing is listed by default. ALMa's own page is same-origin on loopback or an
    IP, including through the Vite dev server, which keeps the browser's Host. A
    deployer adds origins through ``ALMA_ALLOWED_ORIGINS`` (comma-separated).
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
    return list(dict.fromkeys(extra))


def _origin_authorities(origin: str) -> set[str]:
    """Every ``Host`` spelling of a normalized origin, default port written or not."""
    scheme, _, authority = origin.partition("://")
    if _split_authority(authority)[1] is not None:
        return {authority}
    return {authority, f"{authority}:{_DEFAULT_PORTS[scheme]}"}


def _rebinding_safe(hostname: str) -> bool:
    """True when DNS rebinding cannot put an attacker's page on this hostname."""
    if hostname == "localhost":
        return True
    try:
        ipaddress.ip_address(hostname.strip("[]"))
    except ValueError:
        return False
    return True


def host_allowed(host: str, authorities: frozenset[str]) -> bool:
    """Decide whether a request addressed to ``host`` is meant for this ALMa."""
    host = host.lower()
    try:
        hostname, _ = _split_authority(host)
    except ValueError:
        return False
    return _rebinding_safe(hostname) or host in authorities


def _internal_health_check(host: str, path: str) -> bool:
    """True for a container on the Docker network asking whether ALMa is up."""
    return host.lower() == INTERNAL_SERVICE_AUTHORITY and path == INTERNAL_HEALTH_PATH


def origin_allowed(origin: str, host: str, allowed: frozenset[str]) -> bool:
    """Decide whether a browser request from ``origin`` may change state."""
    try:
        if urlsplit(origin.lower()).scheme in _EXTENSION_SCHEMES:
            return True
        normalized = _normalize_origin(origin)
    except ValueError:
        # Includes "null": a sandboxed iframe, a data: URL, or a page that hid its
        # origin. None of these is ALMa's own page.
        return False
    if normalized in allowed:
        return True
    authority = normalized.partition("://")[2]
    return host.lower() in _origin_authorities(normalized) and _rebinding_safe(
        _split_authority(authority)[0]
    )


class OriginGuardMiddleware:
    """Refuse strange hosts and other websites' writes before any route runs."""

    def __init__(self, app: ASGIApp, origins: list[str]) -> None:
        self.app = app
        self.allowed = frozenset(origins)
        self.authorities = frozenset(
            authority for origin in origins for authority in _origin_authorities(origin)
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # A second Host is a smuggling attempt, never a browser: keeping either one
        # would let the guard and the app read different addresses.
        hosts = [value for name, value in scope["headers"] if name == b"host"]
        host = hosts[0].decode("latin-1") if len(hosts) == 1 else ""
        known = host_allowed(host, self.authorities)
        if not known and not _internal_health_check(host, scope["path"]):
            logger.warning(
                "Refused %s %s: unknown Host=%r. If this is your own ALMa address, "
                "add it to %s.",
                scope["method"],
                scope["path"],
                host,
                ALLOWED_ORIGINS_ENV,
            )
            await _send_refusal(
                send,
                "UnknownHostRefused",
                f"Refused: this ALMa does not answer to {host!r}. If this is your own "
                f"ALMa address, add it to {ALLOWED_ORIGINS_ENV}, for example "
                f"{ALLOWED_ORIGINS_ENV}=https://alma.example.com.",
            )
            return

        if scope["method"] in _SAFE_METHODS:
            await self.app(scope, receive, send)
            return

        headers = {name: value for name, value in scope["headers"]}
        origin = headers.get(b"origin", b"").decode("latin-1")
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
        await _send_refusal(
            send,
            "CrossSiteRequestRefused",
            f"Refused: {origin or 'this request'} may not change data in this ALMa. If "
            f"this is your own ALMa address, add it to {ALLOWED_ORIGINS_ENV}.",
        )


async def _send_refusal(send: Send, error: str, message: str) -> None:
    """Answer 403 in the app's error shape, naming the setting that would allow it."""
    body = json.dumps({"error": error, "message": message, "detail": message}).encode()
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
