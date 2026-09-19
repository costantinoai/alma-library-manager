"""The one gate for addresses and text that come from sites we do not control.

A PDF source hands the core URLs it read from third-party metadata, a
publisher page or a mirror that is actively hostile (pop-up scripts, bot
walls, rewritten links). Before ALMa connects anywhere, or stores a word a
remote server chose, the value passes through here:

* :func:`vet_url` — a URL is syntactically boring (http(s), ports 80/443, no
  userinfo, no control characters, no encoded or numeric-trick host), its host
  RESOLVES, and every address it resolves to is on the public internet
  (:func:`is_public_address`). It returns the canonical URL to send and the
  addresses the connection may use, so the transport can pin them (no DNS
  rebinding between the check and the connect).
* :func:`is_public_address` — stricter than ``ipaddress.is_global``, whose
  answer for IPv4-mapped IPv6 changed between Python 3.11 and 3.12: embedded
  IPv4 (mapped, compatible, 6to4, NAT64) is unwrapped and judged as IPv4, and
  the carrier-grade range Tailscale uses (100.64.0.0/10) is refused outright.
* :func:`clean_remote_text` — a remote string made safe to store and show.
* :func:`safe_charset` — the only encodings a remote page may ask us to use.

Nothing here reads the database or the network except the one resolver call.
"""

from __future__ import annotations

import codecs
import ipaddress
import re
import socket
import unicodedata
from dataclasses import dataclass
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

MAX_URL_CHARS = 2048
ALLOWED_PORTS = frozenset({80, 443})

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

# Ranges refused whatever ``is_global`` says (it has been wrong about some of
# these in some Python versions): "this network", carrier-grade NAT (the
# tailnet), IETF protocol assignments, benchmarking, reserved + broadcast.
_DENY_V4 = tuple(
    ipaddress.ip_network(net)
    for net in ("0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "198.18.0.0/15", "240.0.0.0/4")
)
# Tailscale's own IPv6 range and local-use NAT64 (a gateway into a private net).
_DENY_V6 = tuple(ipaddress.ip_network(net) for net in ("fd7a:115c:a1e0::/48", "64:ff9b:1::/48"))
_NAT64 = ipaddress.ip_network("64:ff9b::/96")
_V4_COMPATIBLE = ipaddress.ip_network("::/96")

_NUMERIC_LABEL = re.compile(r"^(0x[0-9a-f]*|[0-9]+)$", re.IGNORECASE)
_DOTTED_QUAD = re.compile(r"^(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)(\.(25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$")
# Path/query characters kept as they are; everything else is percent-encoded
# (spaces, non-ASCII). ``%`` is kept so existing escapes are not doubled.
_PATH_SAFE = "/%:@!$&'()*+,;=-._~"
_QUERY_SAFE = "/%:@!$&'()*+,;=-._~?"
_SAFE_CHARSETS = frozenset({"utf-8", "utf-16", "cp1252", "iso8859-1", "ascii"})


class UnsafeUrlError(ValueError):
    """A URL ALMa must not fetch. The message is ALMa's own words, never the URL."""


@dataclass(frozen=True)
class VettedUrl:
    """A URL that passed :func:`vet_url`: send ``url``, connect only to ``addresses``."""

    url: str
    host: str
    port: int
    addresses: tuple[str, ...]


def is_public_address(address: IPAddress | str) -> bool:
    """True only for an address on the public internet (see module docstring)."""
    ip = ipaddress.ip_address(address) if isinstance(address, str) else address
    if isinstance(ip, ipaddress.IPv6Address):
        if ip.teredo is not None:
            return False
        embedded = _embedded_ipv4(ip)
        if embedded is not None:
            return is_public_address(embedded)
        if any(ip in net for net in _DENY_V6):
            return False
    elif any(ip in net for net in _DENY_V4):
        return False
    return ip.is_global and not ip.is_multicast


def _embedded_ipv4(ip: ipaddress.IPv6Address) -> ipaddress.IPv4Address | None:
    """The IPv4 address an IPv6 address merely wraps (mapped, compatible, 6to4, NAT64)."""
    if ip.ipv4_mapped is not None:
        return ip.ipv4_mapped
    if ip.sixtofour is not None:
        return ip.sixtofour
    if ip in _NAT64 or ip in _V4_COMPATIBLE:
        return ipaddress.IPv4Address(int(ip) & 0xFFFFFFFF)
    return None


def _resolve(host: str) -> list[str]:
    """Every address ``host`` resolves to (the ONE resolver call; tests map ``*.test``)."""
    infos = socket.getaddrinfo(host, None, proto=socket.IPPROTO_TCP)
    return list(dict.fromkeys(str(info[4][0]).split("%", 1)[0] for info in infos))


def public_addresses(host: str) -> tuple[str, ...]:
    """Resolve ``host`` and return its addresses — only if EVERY one is public.

    An IP literal is judged as is. A name that does not resolve is refused: a
    host the checker cannot resolve may still be one the transport can (curl
    URL-decodes ``%31%32%37.0.0.1``), so "let it through" is never safe.
    """
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None:
        addresses = [str(literal)]
    else:
        try:
            addresses = _resolve(host)
        except (OSError, UnicodeError) as exc:
            raise UnsafeUrlError("The address does not resolve") from exc
    if not addresses:
        raise UnsafeUrlError("The address does not resolve")
    for address in addresses:
        if not is_public_address(address):
            raise UnsafeUrlError("Refused: not a public internet address")
    return tuple(addresses)


def _checked_host(raw_host: str) -> str:
    """The host in the form the transport will use, or :class:`UnsafeUrlError`."""
    host = raw_host.strip()
    if not host or "%" in host or host.endswith("."):
        raise UnsafeUrlError("Refused: a disguised or empty host")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if ip is not None:
        return str(ip)
    labels = host.split(".")
    if all(_NUMERIC_LABEL.match(label) for label in labels) and not _DOTTED_QUAD.match(host):
        # 127.1, 0x7f.1, 2130706433, 0177.0.0.1 — numeric forms resolvers
        # disagree about. Only a plain dotted quad is an IPv4 literal here.
        raise UnsafeUrlError("Refused: a disguised numeric host")
    try:
        import idna

        return idna.encode(host, uts46=True).decode("ascii")
    except (UnicodeError, ValueError) as exc:  # idna.IDNAError is a UnicodeError
        raise UnsafeUrlError("Refused: an invalid host name") from exc


def vet_url(raw: str, *, base: str = "", resolve: bool = True) -> VettedUrl:
    """Validate ``raw`` (resolved against ``base``) and return what may be fetched.

    Raises :class:`UnsafeUrlError` — and nothing else — for a URL that is too
    long, carries control characters, backslashes or credentials, is not
    http(s), names a port other than 80/443, disguises its host, or (with
    ``resolve``) whose host does not resolve or resolves to anything but
    public addresses. ``resolve=False`` checks the syntax only (for settings
    input, which must not trigger DNS lookups on every read).
    """
    value = (raw or "").strip()
    if len(value) > MAX_URL_CHARS or len(base) > MAX_URL_CHARS:
        raise UnsafeUrlError("Refused: an overlong address")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value) or "\\" in value:
        raise UnsafeUrlError("Refused: control characters in the address")
    try:
        joined = urljoin(base, value) if base else value
        parts = urlsplit(joined)
        port = parts.port
    except ValueError as exc:
        raise UnsafeUrlError("Refused: a malformed address") from exc
    if parts.scheme not in ("http", "https"):
        raise UnsafeUrlError("Refused: not a web address")
    if "@" in parts.netloc or parts.username is not None or parts.password is not None:
        raise UnsafeUrlError("Refused: credentials in the address")
    effective_port = port if port is not None else (443 if parts.scheme == "https" else 80)
    if effective_port not in ALLOWED_PORTS:
        raise UnsafeUrlError("Refused: an unusual port")
    host = _checked_host(parts.hostname or "")
    addresses = public_addresses(host) if resolve else ()
    netloc = f"[{host}]" if ":" in host else host
    if port is not None:
        netloc = f"{netloc}:{port}"
    url = urlunsplit((
        parts.scheme,
        netloc,
        quote(parts.path or "/", safe=_PATH_SAFE),
        quote(parts.query, safe=_QUERY_SAFE),
        "",
    ))
    return VettedUrl(url=url, host=host, port=effective_port, addresses=addresses)


def clean_remote_text(value: object, limit: int = 200, *, keep_semicolons: bool = False) -> str:
    """A string a remote server chose, made safe to store, log and display.

    Control characters become spaces, format characters (bidi overrides,
    zero-width joiners) are dropped, whitespace collapses, ``;`` — ALMa's own
    list separator in attempt details — becomes ``,`` (unless
    ``keep_semicolons``: for a whole detail ALMa itself joined from cleaned
    parts), and the result is cut to ``limit`` characters.
    """
    kept: list[str] = []
    for ch in str(value if value is not None else ""):
        category = unicodedata.category(ch)
        if category == "Cc":
            kept.append(" ")
        elif category != "Cf":
            kept.append(ch)
    text = "".join(kept)
    if not keep_semicolons:
        text = text.replace(";", ",")
    return " ".join(text.split())[:limit]


def safe_charset(name: str | None) -> str:
    """A text encoding a remote page may make us decode with (else ``utf-8``).

    Exotic codecs are refused: ``punycode`` decodes in quadratic time, and an
    unknown name would raise mid-download.
    """
    try:
        canonical = codecs.lookup(str(name or "")).name
    except LookupError:
        return "utf-8"
    return canonical if canonical in _SAFE_CHARSETS else "utf-8"


__all__ = [
    "ALLOWED_PORTS",
    "MAX_URL_CHARS",
    "UnsafeUrlError",
    "VettedUrl",
    "clean_remote_text",
    "is_public_address",
    "public_addresses",
    "safe_charset",
    "vet_url",
]
