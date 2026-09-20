"""Shared HTTP transport for third-party source adapters.

This module provides per-source sessions, source-aware throttling, and retry
behavior so discovery and identity workflows do not each reinvent request
handling.
"""

from __future__ import annotations

import logging
import random
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from alma.config import (
    get_app_user_agent,
    get_contact_email,
    get_crossref_mailto,
    get_semantic_scholar_api_key,
)
from alma.core.http_deadline import deadline_sleep, release_socket, remaining_timeout, watch_socket
from alma.core.redaction import redact_sensitive_text
from alma.core.url_safety import VettedUrl, clean_remote_text

logger = logging.getLogger(__name__)

_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

# Hard ceiling on a single honored `Retry-After` sleep (42.2). A throttled
# provider can advertise minutes; a background sweep must never sit in one sleep
# longer than this, or it sleeps past its own deadline. Bounded → worst-case
# overshoot is one cap, then the between-fetch deadline check stops the run.
MAX_RETRY_AFTER_SECONDS = 60.0
_ACTIVE_DIAGNOSTICS: ContextVar[SourceDiagnosticsCollector | None] = ContextVar(
    "alma_active_source_diagnostics",
    default=None,
)


@dataclass(frozen=True)
class SourcePolicy:
    name: str
    base_url: str
    min_interval_seconds: float
    max_concurrency: int = 1
    max_retries: int = 3
    default_timeout: float = 20.0
    default_headers: tuple[tuple[str, str], ...] = ()
    auth_header_factory: Callable[[], dict[str, str]] | None = None
    auth_param_factory: Callable[[], dict[str, str]] | None = None
    # Factories return the *current* rate budget per request. They let
    # polite-pool eligibility (e.g. a contact email added through the
    # Settings UI after startup) take effect immediately, instead of
    # being frozen at import time.
    min_interval_factory: Callable[[], float] | None = None
    max_concurrency_factory: Callable[[], int] | None = None
    # Cap on the per-attempt retry backoff. Some sources (Semantic
    # Scholar's anonymous shared pool, Crossref under load) can stay
    # 429-blocked for tens of seconds; an 8 s cap means we give up
    # right when the upstream is *almost* recovered. Default is the
    # historical 8 s; sources that need more set this higher.
    max_retry_backoff_seconds: float = 8.0
    # Adaptive throttle: when a 429 is observed, hold the *next*
    # request interval at this floor for `adaptive_cooldown_seconds`
    # so we stop hammering an upstream that's signalling overload.
    # Set both to 0.0 to disable.
    adaptive_throttle_floor_seconds: float = 0.0
    adaptive_cooldown_seconds: float = 0.0
    # Transport override. ``None`` = a plain ``requests.Session``. A factory
    # returns any requests-compatible session (e.g. a ``curl_cffi`` session
    # with browser TLS impersonation), so a plugin-owned source still gets
    # the throttle, retry, cooldown, diagnostics and network switch below.
    session_factory: Callable[[], Any] | None = None
    # Extra exception classes the transport raises for connection-level
    # failures. ``requests`` errors are always caught; a non-requests session
    # (curl_cffi) names its own here so they retry and record the same way.
    transport_errors: tuple[type[BaseException], ...] = ()
    # An impersonating transport must keep the browser User-Agent it forges;
    # every ordinary source identifies itself as ALMa.
    send_app_user_agent: bool = True
    # A source that fetches URLs taken from third-party content (metadata,
    # publisher pages, mirrors): every request and every redirect hop goes
    # through ``core.url_safety.vet_url``, the connection is pinned to the
    # addresses that were checked, the transport never follows a redirect on
    # its own, and a non-streamed body is capped. See ``_send_guarded``.
    guard_addresses: bool = False


# --- Guarded transport (sources that fetch third-party-chosen URLs) ---------

GUARDED_MAX_REDIRECTS = 5
#: Cap on a NON-streamed guarded body (a landing page, a mirror's JSON API).
#: Streamed downloads carry their own cap (the PDF store's).
GUARDED_MAX_BODY_BYTES = 5_000_000
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_BODY_CHUNK = 64 * 1024


class ResponseTooLargeError(requests.exceptions.RequestException):
    """A guarded body passed its byte cap (the response is closed)."""


def iter_body(response: Any) -> Iterator[bytes]:
    """A response's body in chunks, for ``requests`` and ``curl_cffi`` alike.

    ``requests`` defaults to 1-byte chunks; curl_cffi sizes its own chunks and
    warns when given a size.
    """
    if isinstance(response, requests.Response):
        return response.iter_content(_BODY_CHUNK)
    return response.iter_content()


def _read_capped(response: Any, max_bytes: int) -> Any:
    """Read a streamed response into memory, refusing more than ``max_bytes``."""
    body = bytearray()
    try:
        for chunk in iter_body(response):
            body += chunk
            if len(body) > max_bytes:
                raise ResponseTooLargeError(f"Response larger than {max_bytes} bytes")
    finally:
        response.close()
    if isinstance(response, requests.Response):
        response._content = bytes(body)
        response._content_consumed = True
    else:  # curl_cffi: `content` is a plain attribute; `text` decodes it lazily
        response.content = bytes(body)
    return response


@contextmanager
def _pinned(session: Any, pin: VettedUrl | None) -> Iterator[None]:
    """For this one request, make a curl session connect only to ``pin.addresses``.

    ``CURLOPT_RESOLVE`` is set on the (thread-local) session's own options and
    restored afterwards. Every request of a guarded curl session is pinned:
    its persistent handle caches DNS and reuses connections by host name, so
    one unpinned call could leave a rebound address behind. A ``requests``
    session is pinned by its :class:`GuardedAdapter` instead.
    """
    if pin is None or not hasattr(session, "curl_options"):
        yield
        return
    from curl_cffi import CurlOpt

    addresses = ",".join(f"[{address}]" if ":" in address else address for address in pin.addresses)
    saved = session.curl_options
    options = {**(saved or {}), CurlOpt.RESOLVE: [f"{pin.host}:{pin.port}:{addresses}"]}
    remaining = remaining_timeout()
    if remaining is not None:
        # curl_cffi stream mode otherwise sets only a LOW_SPEED timeout.
        options[CurlOpt.TIMEOUT_MS] = max(1, int(remaining * 1000))
    session.curl_options = options
    try:
        yield
    finally:
        session.curl_options = saved


def _check_peer(response: Any, vetted: VettedUrl) -> None:
    """Refuse a curl response whose connection went anywhere but a vetted address.

    curl_cffi records the connected peer as ``primary_ip`` once headers are in;
    a ``requests`` response has no such field — its :class:`GuardedAdapter`
    connected to a checked address itself.
    """
    import ipaddress

    from alma.core.url_safety import UnsafeUrlError

    if not hasattr(response, "primary_ip"):
        return
    allowed = {ipaddress.ip_address(address) for address in vetted.addresses}
    try:
        ok = ipaddress.ip_address(str(response.primary_ip or "")) in allowed
    except ValueError:
        ok = False
    if not ok:
        response.close()
        raise UnsafeUrlError("Refused: the connection went to an address that was not checked")


def _guarded_connection(base: type) -> type:
    """A urllib3 connection class that connects only to vetted public addresses."""
    from urllib3.exceptions import ConnectTimeoutError, NewConnectionError
    from urllib3.util.connection import create_connection

    class _Guarded(base):  # type: ignore[misc, valid-type]
        def connect(self):
            super().connect()
            watch_socket(self.sock)

        def request(self, *args, **kwargs):
            # Reused pooled sockets need the new caller's deadline too.
            watch_socket(self.sock)
            return super().request(*args, **kwargs)

        def close(self):
            # Stop watching before the descriptor goes: the watchdog must not
            # be left holding sockets this connection has finished with.
            release_socket(self.sock)
            super().close()

        def _new_conn(self):  # noqa: ANN202 — urllib3's own signature
            from alma.core.url_safety import public_addresses

            # Resolve, check EVERY address, connect to a checked one: no
            # second lookup between the check and the connect. TLS still
            # verifies the certificate against the host name.
            addresses = public_addresses(self._dns_host)  # UnsafeUrlError propagates, un-retried
            last: OSError | None = None
            for address in addresses:
                try:
                    return create_connection(
                        (address, self.port),
                        self.timeout,
                        source_address=self.source_address,
                        socket_options=self.socket_options,
                    )
                except TimeoutError as exc:
                    raise ConnectTimeoutError(self, f"Connection to {self.host} timed out") from exc
                except OSError as exc:
                    last = exc
            raise NewConnectionError(self, f"Failed to establish a new connection: {last}")

    _Guarded.__name__ = f"Guarded{base.__name__}"
    return _Guarded


class GuardedAdapter(requests.adapters.HTTPAdapter):
    """A ``requests`` adapter whose every connection goes to a vetted public address.

    Mounted on the sessions of ``guard_addresses`` policies. It never uses a
    proxy: a proxy would make the connection check meaningless.
    """

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        from urllib3.connection import HTTPConnection, HTTPSConnection
        from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool

        super().init_poolmanager(*args, **kwargs)

        class _Http(HTTPConnectionPool):
            ConnectionCls = _guarded_connection(HTTPConnection)

        class _Https(HTTPSConnectionPool):
            ConnectionCls = _guarded_connection(HTTPSConnection)

        self.poolmanager.pool_classes_by_scheme = {"http": _Http, "https": _Https}

    def proxy_manager_for(self, *args: Any, **kwargs: Any) -> Any:
        from alma.core.url_safety import UnsafeUrlError

        raise UnsafeUrlError("Refused: guarded fetches never go through a proxy")


class SourceDiagnosticsCollector:
    """Thread-safe per-operation summary of external source usage."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sources: dict[str, dict[str, Any]] = {}

    def record(
        self,
        *,
        source: str,
        method: str,
        path: str,
        attempt: int,
        duration_ms: float,
        status_code: int | None = None,
        error: str | None = None,
    ) -> None:
        with self._lock:
            entry = self._sources.setdefault(
                source,
                {
                    "requests": 0,
                    "ok": 0,
                    "http_errors": 0,
                    "transport_errors": 0,
                    "retries": 0,
                    "total_latency_ms": 0.0,
                    "status_counts": {},
                    "endpoint_counts": {},
                    "last_error": None,
                },
            )
            entry["requests"] += 1
            entry["total_latency_ms"] += max(0.0, float(duration_ms or 0.0))
            if attempt > 0:
                entry["retries"] += 1
            normalized_path = (path or "").strip() or "/"
            endpoint_counts = entry["endpoint_counts"]
            endpoint_counts[normalized_path] = int(endpoint_counts.get(normalized_path) or 0) + 1

            if status_code is not None:
                status_key = str(int(status_code))
                status_counts = entry["status_counts"]
                status_counts[status_key] = int(status_counts.get(status_key) or 0) + 1
                if 200 <= int(status_code) < 400:
                    entry["ok"] += 1
                else:
                    entry["http_errors"] += 1
                    if error:
                        entry["last_error"] = error
            elif error:
                entry["transport_errors"] += 1
                entry["last_error"] = error

    def summary(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            out: dict[str, dict[str, Any]] = {}
            for source, raw in self._sources.items():
                requests = int(raw.get("requests") or 0)
                avg_latency_ms = 0.0
                if requests > 0:
                    avg_latency_ms = round(float(raw.get("total_latency_ms") or 0.0) / requests, 2)
                top_endpoints = sorted(
                    (raw.get("endpoint_counts") or {}).items(),
                    key=lambda item: (-int(item[1]), str(item[0])),
                )[:5]
                out[source] = {
                    "requests": requests,
                    "ok": int(raw.get("ok") or 0),
                    "http_errors": int(raw.get("http_errors") or 0),
                    "transport_errors": int(raw.get("transport_errors") or 0),
                    "retries": int(raw.get("retries") or 0),
                    "avg_latency_ms": avg_latency_ms,
                    "status_counts": dict(raw.get("status_counts") or {}),
                    "top_endpoints": [
                        {"path": str(path), "count": int(count)}
                        for path, count in top_endpoints
                    ],
                    "last_error": raw.get("last_error"),
                }
            return out


def get_active_source_diagnostics() -> SourceDiagnosticsCollector | None:
    """Return the active diagnostics collector for the current execution context."""
    return _ACTIVE_DIAGNOSTICS.get()


def bind_source_diagnostics(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Propagate the current diagnostics collector into worker threads."""
    collector = get_active_source_diagnostics()
    if collector is None:
        return fn

    def _wrapped(*args: Any, **kwargs: Any) -> Any:
        token = _ACTIVE_DIAGNOSTICS.set(collector)
        try:
            return fn(*args, **kwargs)
        finally:
            _ACTIVE_DIAGNOSTICS.reset(token)

    return _wrapped


@contextmanager
def source_diagnostics_scope() -> Iterator[SourceDiagnosticsCollector]:
    """Collect per-source HTTP diagnostics for the current operation."""
    collector = SourceDiagnosticsCollector()
    token = _ACTIVE_DIAGNOSTICS.set(collector)
    try:
        yield collector
    finally:
        _ACTIVE_DIAGNOSTICS.reset(token)


def _semantic_headers() -> dict[str, str]:
    key = get_semantic_scholar_api_key()
    if not key:
        return {}
    return {"x-api-key": key}


def _crossref_params() -> dict[str, str]:
    mailto = get_crossref_mailto()
    if not mailto:
        return {}
    return {"mailto": mailto}


def _crossref_min_interval() -> float:
    # Crossref retuned its REST limits on 2025-12-01 (first change since 2013).
    # The *list/search* path — which is what ALMa hits via /works?query — is now
    # the stricter ceiling: polite = 3 req/s (3 concurrent), anonymous = 1 req/s.
    # (Single-record /works/{doi} is looser at 10 req/s polite, but we pace to the
    # search ceiling since search dominates and one client serves both.) Crossref
    # also advertises live limits via X-Rate-Limit-Limit / -Interval headers; a
    # future enhancement is to read those and adapt dynamically.
    return 0.34 if get_crossref_mailto() else 1.05


def _crossref_max_concurrency() -> int:
    return 3 if get_crossref_mailto() else 1


def _orcid_headers() -> dict[str, str]:
    return {"Accept": "application/json"}


_POLICIES: dict[str, SourcePolicy] = {
    "semantic_scholar": SourcePolicy(
        name="semantic_scholar",
        base_url="https://api.semanticscholar.org/graph/v1",
        # 1 request per second documented limit even with an API key
        # (per https://www.semanticscholar.org/product/api/tutorial).
        # 1.05 s gives ~5% headroom against clock skew + GC pauses.
        min_interval_seconds=1.05,
        max_concurrency=1,
        # Bumped from 3 → 5: the anonymous shared pool (5 000 req /
        # 5 min, all users worldwide) can stay congested for tens of
        # seconds; 3 retries × 8 s cap = ~11 s total wait, not enough
        # when the global pool is exhausted. 5 retries × 60 s cap =
        # up to ~2 minutes total, which clears nearly every 429 we've
        # seen in practice.
        max_retries=5,
        max_retry_backoff_seconds=60.0,
        # On any 429, freeze the per-request interval at 10 s for the
        # next 30 s so we stop firing hot calls into a busy upstream.
        # Measured 2026-07-04 (keyed, exact 1.05 s pacing, raw requests):
        # S2 429s ~25% of calls even when fully compliant and sends NO
        # Retry-After, and the next call typically succeeds — so these are
        # transient server-side congestion blips, not "we're too fast".
        # The previous 30 s/60 s setting amplified every blip into a ~6×
        # per-paper slowdown on search sweeps. Resets automatically once
        # the cooldown window elapses; a fresh 429 re-arms it.
        adaptive_throttle_floor_seconds=10.0,
        adaptive_cooldown_seconds=30.0,
        default_headers=(("Accept", "application/json"),),
        auth_header_factory=_semantic_headers,
    ),
    "crossref": SourcePolicy(
        name="crossref",
        base_url="https://api.crossref.org",
        # Static fields are the anonymous-pool fallback; the factories
        # below are consulted per request so the polite pool kicks in
        # the moment a contact email is configured at runtime.
        min_interval_seconds=0.25,
        max_concurrency=1,
        max_retries=3,
        default_headers=(("Accept", "application/json"),),
        auth_param_factory=_crossref_params,
        min_interval_factory=_crossref_min_interval,
        max_concurrency_factory=_crossref_max_concurrency,
    ),
    "datacite": SourcePolicy(
        name="datacite",
        base_url="https://api.datacite.org",
        # DataCite asks for "reasonable" use and publishes no hard figure; this
        # matches the Crossref pacing we already consider polite. The lookup is
        # only ever made for DOIs Crossref does not know, which is a small tail.
        min_interval_seconds=0.25,
        max_concurrency=1,
        max_retries=3,
        default_headers=(("Accept", "application/json"),),
    ),
    "arxiv": SourcePolicy(
        name="arxiv",
        guard_addresses=True,
        base_url="https://export.arxiv.org",
        min_interval_seconds=3.1,
        max_concurrency=1,
        max_retries=2,
        default_headers=(("Accept", "application/atom+xml"),),
    ),
    "biorxiv": SourcePolicy(
        name="biorxiv",
        base_url="https://api.biorxiv.org",
        min_interval_seconds=0.35,
        max_concurrency=1,
        max_retries=2,
        default_headers=(("Accept", "application/json"),),
    ),
    "unpaywall": SourcePolicy(
        name="unpaywall",
        base_url="https://api.unpaywall.org/v2",
        min_interval_seconds=0.12,
        max_concurrency=1,
        max_retries=2,
        default_headers=(("Accept", "application/json"),),
        auth_param_factory=lambda: (
            {"email": get_contact_email()} if get_contact_email() else {}
        ),
    ),
    "europe_pmc": SourcePolicy(
        name="europe_pmc",
        base_url="https://www.ebi.ac.uk/europepmc/webservices/rest",
        # EBI publishes no hard per-second figure for the REST service and asks
        # only that clients be reasonable. ~5 req/s is well inside what the
        # service tolerates and matches how we pace the other unauthenticated
        # open endpoints.
        min_interval_seconds=0.2,
        max_concurrency=2,
        max_retries=3,
        default_headers=(("Accept", "application/json"),),
    ),
    # PMC open-access article files on AWS (the PMC OA web service `oa.fcgi`
    # was retired in Aug 2026; PMC/Europe PMC web PDFs sit behind captchas).
    # Public, anonymous S3: list `metadata/PMC{id}.` for the latest version,
    # then `PMC{id}.{ver}/PMC{id}.{ver}.pdf`.
    "pmc_oa": SourcePolicy(
        name="pmc_oa",
        guard_addresses=True,
        base_url="https://pmc-oa-opendata.s3.amazonaws.com",
        min_interval_seconds=0.2,
        max_concurrency=2,
        max_retries=2,
        default_headers=(("Accept", "*/*"),),
    ),
    "publisher": SourcePolicy(
        name="publisher",
        guard_addresses=True,
        base_url="",
        min_interval_seconds=0.5,
        max_concurrency=1,
        max_retries=1,
        default_headers=(("Accept", "text/html,application/xhtml+xml"),),
    ),
    "orcid": SourcePolicy(
        name="orcid",
        base_url="https://pub.orcid.org/v3.0",
        min_interval_seconds=0.04,
        max_concurrency=1,
        max_retries=2,
        default_headers=(("Accept", "application/json"),),
        auth_header_factory=_orcid_headers,
    ),
}


class SourceHttpClient:
    """Source-specific HTTP client with rate limiting and retries."""

    def __init__(self, policy: SourcePolicy) -> None:
        self._policy = policy
        self._local = threading.local()
        self._rate_lock = threading.RLock()
        self._next_request_at = 0.0
        # Adaptive throttle: when a 429 is observed, this timestamp is
        # set to `now + adaptive_cooldown_seconds`. Until then,
        # `_current_min_interval` returns the larger of its normal
        # value and `adaptive_throttle_floor_seconds`. Reset to 0
        # automatically when the cooldown elapses.
        self._adaptive_floor_until: float = 0.0
        # Concurrency is gated dynamically (see `_concurrency_slot`) so
        # the limit can grow/shrink with runtime config — e.g. Crossref
        # moving between anonymous and polite pool when a contact email
        # is added or removed from the Settings UI.
        self._concurrency_cond = threading.Condition()
        self._active_requests = 0

    @property
    def guards_addresses(self) -> bool:
        """True when every request is vetted and pinned (``SourcePolicy.guard_addresses``)."""
        return self._policy.guard_addresses

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            factory = self._policy.session_factory
            session = factory() if factory is not None else requests.Session()
            if self._policy.guard_addresses and isinstance(session, requests.Session):
                # No environment proxies, and every connection vetted (a curl
                # session is pinned per request in `_pinned` instead).
                session.trust_env = False
                adapter = GuardedAdapter()
                session.mount("http://", adapter)
                session.mount("https://", adapter)
            if self._policy.send_app_user_agent:
                session.headers.update({"User-Agent": get_app_user_agent()})
            for key, value in self._policy.default_headers:
                session.headers[key] = value
            self._local.session = session
        return session

    def _prepare_url(self, path_or_url: str) -> str:
        raw = (path_or_url or "").strip()
        if raw.startswith("http://") or raw.startswith("https://"):
            return raw
        return f"{self._policy.base_url.rstrip('/')}/{raw.lstrip('/')}"

    def _apply_auth_headers(self, headers: dict[str, str] | None) -> dict[str, str]:
        merged = dict(headers or {})
        if self._policy.auth_header_factory:
            merged.update(self._policy.auth_header_factory() or {})
        return merged

    def _apply_auth_params(self, params: dict[str, Any] | None) -> dict[str, Any]:
        merged = dict(params or {})
        if self._policy.auth_param_factory:
            merged.update(self._policy.auth_param_factory() or {})
        return merged

    def _current_min_interval(self) -> float:
        if self._policy.min_interval_factory is not None:
            try:
                base = max(0.0, float(self._policy.min_interval_factory()))
            except Exception:
                base = max(0.0, float(self._policy.min_interval_seconds))
        else:
            base = max(0.0, float(self._policy.min_interval_seconds))
        # Adaptive 429 cooldown: while inside the cooldown window,
        # space requests at the configured floor (e.g. 30 s for S2)
        # so we stop hammering an upstream that just signalled
        # overload.
        floor = float(self._policy.adaptive_throttle_floor_seconds or 0.0)
        if floor > 0.0:
            now = time.monotonic()
            with self._rate_lock:
                if self._adaptive_floor_until > now:
                    return max(base, floor)
                if self._adaptive_floor_until and self._adaptive_floor_until <= now:
                    # Window elapsed; reset so future rate-limit-free
                    # runs don't keep paying the floor.
                    self._adaptive_floor_until = 0.0
        return base

    def _arm_adaptive_throttle(self) -> None:
        """Engage the adaptive cooldown after a 429."""
        cooldown = float(self._policy.adaptive_cooldown_seconds or 0.0)
        if cooldown <= 0.0:
            return
        with self._rate_lock:
            new_until = time.monotonic() + cooldown
            if new_until > self._adaptive_floor_until:
                self._adaptive_floor_until = new_until

    def is_in_adaptive_cooldown(self) -> bool:
        """True while a 429-armed cooldown window is still open.

        Lets callers (e.g. the discovery/feed lane fan-out) skip this source
        for the rest of a refresh pass instead of each lane queuing behind the
        30 s adaptive floor and waiting out its lane deadline. Read-only — does
        not reset the window (that happens lazily in `_current_min_interval`)."""
        if float(self._policy.adaptive_cooldown_seconds or 0.0) <= 0.0:
            return False
        with self._rate_lock:
            return self._adaptive_floor_until > time.monotonic()

    def _current_max_concurrency(self) -> int:
        if self._policy.max_concurrency_factory is not None:
            try:
                return max(1, int(self._policy.max_concurrency_factory()))
            except Exception:
                pass
        return max(1, int(self._policy.max_concurrency))

    @contextmanager
    def _concurrency_slot(self) -> Iterator[None]:
        with self._concurrency_cond:
            while self._active_requests >= self._current_max_concurrency():
                self._concurrency_cond.wait(timeout=remaining_timeout())
            self._active_requests += 1
        try:
            yield
        finally:
            with self._concurrency_cond:
                self._active_requests -= 1
                self._concurrency_cond.notify_all()

    def _wait_for_slot(self) -> None:
        interval = self._current_min_interval()
        with self._rate_lock:
            now = time.monotonic()
            wait = max(0.0, self._next_request_at - now)
            if wait > 0:
                deadline_sleep(wait)
            self._next_request_at = time.monotonic() + interval

    def _retry_wait(self, response: requests.Response | None, attempt: int) -> float:
        if response is not None:
            retry_after = response.headers.get("retry-after")
            if retry_after:
                wait: float | None = None
                try:
                    wait = max(0.0, float(retry_after))
                except (TypeError, ValueError):
                    try:
                        dt = parsedate_to_datetime(retry_after)
                        wait = max(0.0, dt.timestamp() - time.time())
                    except Exception:
                        wait = None
                if wait is not None:
                    # 42.2: honor Retry-After but CAP it. A throttled provider can
                    # send minutes; an uncapped sleep would sit past a background
                    # sweep's whole deadline (its runner only re-checks the
                    # deadline BETWEEN fetches, never mid-sleep). A bounded sleep
                    # means the worst-case overshoot is one cap, then the sweep
                    # stops and stays retryable.
                    return min(wait, MAX_RETRY_AFTER_SECONDS)
        cap = max(1.0, float(self._policy.max_retry_backoff_seconds or 8.0))
        base = min(cap, 0.75 * (2 ** attempt))
        return base + random.uniform(0.0, 0.4)

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        stream: bool = False,
        allow_redirects: bool = True,
        max_body_bytes: int | None = None,
    ) -> requests.Response:
        """Issue one rate-limited, retried request.

        ``max_retries`` overrides the policy's retry budget for this call
        only. Interactive surfaces (e.g. Find & Add search lanes racing a
        lane deadline) pass a small value so a 429/5xx fails fast instead
        of burning the policy's full background-job backoff chain.

        ``stream=True`` returns before the body is read, so a large download
        (a PDF) can be consumed chunk by chunk under a byte cap; the caller
        then owns the response and must ``close()`` it. A streamed response
        that is retried is closed here first so its connection returns to
        the pool. ``allow_redirects=False`` hands 3xx back to the caller.

        A policy with ``guard_addresses`` routes through :meth:`_send_guarded`:
        the URL and every redirect hop are vetted and the connection pinned,
        and a non-streamed body stops at ``max_body_bytes``
        (default :data:`GUARDED_MAX_BODY_BYTES`).
        """
        from alma.core.network_policy import require_network_access

        require_network_access(self._policy.name)
        url = self._prepare_url(path_or_url)
        request_params = self._apply_auth_params(params)
        request_headers = self._apply_auth_headers(headers)
        timeout_value = float(timeout or self._policy.default_timeout)
        retry_budget = (
            max(0, int(max_retries)) if max_retries is not None else max(0, self._policy.max_retries)
        )
        path_label = path_or_url if path_or_url.startswith("/") else url.replace(self._policy.base_url, "", 1) or "/"
        send = dict(
            params=request_params, headers=request_headers, json=json, data=data,
            timeout_value=timeout_value, retry_budget=retry_budget, path_label=path_label,
        )
        if self._policy.guard_addresses:
            return self._send_guarded(
                method, url, stream=stream, allow_redirects=allow_redirects,
                max_body_bytes=max_body_bytes, **send,
            )
        return self._send(method, url, stream=stream, allow_redirects=allow_redirects, **send)

    def _send(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any],
        headers: dict[str, str],
        json: dict[str, Any] | None,
        data: dict[str, Any] | None,
        timeout_value: float,
        retry_budget: int,
        path_label: str,
        stream: bool,
        allow_redirects: bool,
        pin: VettedUrl | None = None,
    ) -> requests.Response:
        """One request with pacing, retries and diagnostics (see :meth:`request`).

        ``pin`` (guarded requests only) makes a curl session connect to the
        vetted addresses and nothing else; a ``requests`` session is pinned by
        its mounted :class:`GuardedAdapter` instead.
        """
        request_params = params
        request_headers = headers
        diagnostics = get_active_source_diagnostics()
        transport_errors = (requests.exceptions.RequestException, *self._policy.transport_errors)

        last_exc: Exception | None = None
        last_resp: requests.Response | None = None
        for attempt in range(retry_budget + 1):
            with self._concurrency_slot():
                self._wait_for_slot()
                started_at = time.monotonic()
                try:
                    with _pinned(self._session(), pin):
                        response = self._session().request(
                            method.upper(),
                            url,
                            params=request_params,
                            headers=request_headers,
                            json=json,
                            data=data,
                            timeout=remaining_timeout(timeout_value),
                            stream=stream,
                            allow_redirects=allow_redirects,
                        )
                    elapsed_ms = (time.monotonic() - started_at) * 1000.0
                    last_resp = response
                    if diagnostics is not None:
                        diagnostics.record(
                            source=self._policy.name,
                            method=method.upper(),
                            path=path_label,
                            attempt=attempt,
                            duration_ms=elapsed_ms,
                            status_code=response.status_code,
                            error=None if response.ok else f"HTTP {response.status_code}",
                        )
                except transport_errors as exc:
                    elapsed_ms = (time.monotonic() - started_at) * 1000.0
                    last_exc = exc
                    if diagnostics is not None:
                        diagnostics.record(
                            source=self._policy.name,
                            method=method.upper(),
                            path=path_label,
                            attempt=attempt,
                            duration_ms=elapsed_ms,
                            error=str(exc),
                        )
                    if attempt >= retry_budget:
                        raise
                    wait = self._retry_wait(None, attempt)
                    logger.debug(
                        "Source request error (%s %s attempt %d/%d): %s; retrying in %.2fs",
                        self._policy.name,
                        url,
                        attempt + 1,
                        retry_budget + 1,
                        exc,
                        wait,
                    )
                    deadline_sleep(wait)
                    continue

            if response.status_code not in _RETRYABLE_STATUSES:
                return response

            # Engage adaptive cooldown the first time we see a 429 in
            # this attempt chain; subsequent retries within the same
            # request will already be paced by `_current_min_interval`.
            if response.status_code == 429:
                self._arm_adaptive_throttle()

            if attempt >= retry_budget:
                return response

            wait = self._retry_wait(response, attempt)
            logger.debug(
                "Source returned retryable status (%s %s attempt %d/%d): HTTP %d; retrying in %.2fs",
                self._policy.name,
                url,
                attempt + 1,
                retry_budget + 1,
                response.status_code,
                wait,
            )
            if stream:
                # The body was never read; release the connection before the
                # retry sleep instead of pinning it for the whole backoff.
                response.close()
            deadline_sleep(wait)

        if last_resp is not None:
            return last_resp
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Unreachable request failure for source {self._policy.name}")

    def _send_guarded(
        self,
        method: str,
        url: str,
        *,
        stream: bool,
        allow_redirects: bool,
        max_body_bytes: int | None,
        **send: Any,
    ) -> requests.Response:
        """A request to an address taken from content we do not control.

        Every hop — the first URL and each ``Location`` — passes
        ``url_safety.vet_url`` and is sent as the canonical URL it returns,
        pinned to the addresses that were checked; after the headers arrive a
        curl connection's peer must be one of them. Redirects are followed
        here (at most :data:`GUARDED_MAX_REDIRECTS`), never by the transport:
        a POST that is redirected continues as a body-less GET, except a
        307/308, which would re-send the body (a login key) elsewhere and is
        refused. A non-streamed body is read under ``max_body_bytes``.
        Raises ``UnsafeUrlError`` for a refused address.
        """
        from alma.core.url_safety import UnsafeUrlError, vet_url

        current_method = method.upper()
        for _hop in range(GUARDED_MAX_REDIRECTS + 1):
            vetted = vet_url(url)
            response = self._send(current_method, vetted.url, stream=True, allow_redirects=False, pin=vetted, **send)
            _check_peer(response, vetted)
            location = response.headers.get("Location") if response.status_code in _REDIRECT_STATUSES else None
            if not location or not allow_redirects:
                return response if stream else _read_capped(response, max_body_bytes or GUARDED_MAX_BODY_BYTES)
            response.close()
            url = vet_url(location, base=vetted.url, resolve=False).url  # resolved on the next pass
            send["params"] = {}  # the query string belonged to the first request
            if current_method not in ("GET", "HEAD"):
                if response.status_code in (307, 308):
                    raise UnsafeUrlError("Refused: a redirect that would re-send the request body")
                current_method, send["json"], send["data"] = "GET", None, None
        raise requests.exceptions.TooManyRedirects("Too many redirects")

    def get(
        self,
        path_or_url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        stream: bool = False,
        allow_redirects: bool = True,
        max_body_bytes: int | None = None,
    ) -> requests.Response:
        return self.request(
            "GET",
            path_or_url,
            params=params,
            headers=headers,
            timeout=timeout,
            max_retries=max_retries,
            stream=stream,
            allow_redirects=allow_redirects,
            max_body_bytes=max_body_bytes,
        )

    def post(
        self,
        path_or_url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> requests.Response:
        """POST a JSON body (``json``) or a form (``data``, e.g. a login form)."""
        return self.request(
            "POST", path_or_url, params=params, headers=headers, json=json, data=data, timeout=timeout
        )


_CLIENTS: dict[str, SourceHttpClient] = {}
_CLIENTS_LOCK = threading.RLock()


def get_source_http_client(source_name: str) -> SourceHttpClient:
    key = (source_name or "").strip().lower()
    if key not in _POLICIES:
        raise KeyError(f"Unknown source client: {source_name}")
    with _CLIENTS_LOCK:
        client = _CLIENTS.get(key)
        if client is None:
            client = SourceHttpClient(_POLICIES[key])
            _CLIENTS[key] = client
        return client


def client_for_policy(policy: SourcePolicy) -> SourceHttpClient:
    """The shared client for a policy owned OUTSIDE this module (a plugin's).

    Built-in sources are named in ``_POLICIES`` and reached through
    :func:`get_source_http_client`. A plugin that talks to a service ALMa does
    not know about (e.g. a user-configured mirror) defines its own
    ``SourcePolicy`` and gets the same throttle, retries, cooldown,
    diagnostics and network switch through here — one client per policy
    name, so pacing is shared across every caller of that source.

    A plugin policy may not shadow a built-in source's name, and one name maps
    to one policy for the life of the process.
    """
    key = (policy.name or "").strip().lower()
    if not key:
        raise ValueError("A source policy needs a name")
    if key in _POLICIES:
        raise ValueError(f"Source policy {key!r} is built in; use get_source_http_client")
    with _CLIENTS_LOCK:
        client = _CLIENTS.get(key)
        if client is None:
            client = SourceHttpClient(policy)
            _CLIENTS[key] = client
        elif client._policy is not policy and client._policy != policy:
            raise ValueError(f"Source policy {key!r} is already registered with different settings")
        return client


# Connection-level failures in words a reader can act on. Matched on the
# exception text because ``requests`` and ``curl_cffi`` wrap the same
# underlying failure in different classes (curl reports "(60) SSL
# certificate problem", requests "CERTIFICATE_VERIFY_FAILED").
_TRANSPORT_FAILURES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("certificate", "ssl", "tls"),
     "secure connection refused — the address's certificate is invalid or expired, "
     "or your network is blocking it"),
    (("could not resolve", "name resolution", "nameresolution", "getaddrinfo", "name or service not known"),
     "the address does not resolve — it is down, or your DNS blocks it"),
    (("timed out", "timeout"), "timed out"),
    (("connection refused", "failed to connect", "newconnectionerror", "connection reset", "remotedisconnected"),
     "connection refused or dropped"),
)


def describe_transport_error(exc: BaseException) -> str:
    """One readable line for a connection-level failure (``exc`` text kept as a hint).

    Used wherever a transport error becomes user-visible text (an Activity log
    line, a stored attempt), so every source reports the same failure the same
    way instead of leaking a raw curl / urllib3 message.
    """
    raw = redact_sensitive_text(str(exc)).strip()
    lowered = f"{type(exc).__name__} {raw}".lower()
    for needles, words in _TRANSPORT_FAILURES:
        if any(needle in lowered for needle in needles):
            return words
    return clean_remote_text(raw) or type(exc).__name__


def openalex_usage_snapshot() -> dict[str, Any]:
    """Capture current OpenAlex client usage counters."""
    try:
        from alma.openalex.http import get_client as get_openalex_client

        client = get_openalex_client()
        return {
            "request_count": int(client.request_count or 0),
            "retry_count": int(client.retry_count or 0),
            "rate_limited_events": int(client.rate_limited_events or 0),
            "calls_saved_by_cache": int(client.calls_saved_by_cache or 0),
            "credits_used": client.credits_used,
            "credits_remaining": client.rate_remaining,
            # Per-pricing-class upstream calls this process + the $ they imply
            # (singleton free / list $0.10/1k / search $1.00/1k).
            "calls_by_class": client.class_counts,
            "estimated_spend_usd": client.estimated_spend_usd,
            "summary": client.credits_summary(),
        }
    except Exception:
        return {
            "request_count": 0,
            "retry_count": 0,
            "rate_limited_events": 0,
            "calls_saved_by_cache": 0,
            "credits_used": None,
            "credits_remaining": None,
            "calls_by_class": {},
            "estimated_spend_usd": 0.0,
            "summary": "unavailable",
        }


# --- Background-op credit reservation (task 37 B) -----------------------------
# A BACKGROUND op must always leave at least this many provider calls for the
# user's own manual operations — it must never consume the whole daily quota.
# The reserve applies ONLY to sources that expose a finite remaining quota
# (OpenAlex today, via the live `X-RateLimit-Remaining` header). Rate-only
# sources (S2, Crossref) have no daily pool to reserve from and are governed by
# their per-second politeness + the idle-gate instead. The reserve is a soft
# floor FOR the user: it gates background ops only — a manual user op may use the
# full remaining quota down to the provider's real limit.
RESERVED_USER_CALLS = 200


def provider_remaining_credits(source: str) -> int | None:
    """Live remaining daily quota for *source*, or None when it exposes none.

    OpenAlex reports `X-RateLimit-Remaining` on every response (its daily budget);
    we read the client's cached value. None means "unknown / no finite pool" —
    callers treat that as "no reserve to enforce" (before the first call we also
    don't yet know the remaining, so we don't block).
    """
    if str(source or "").strip().lower() != "openalex":
        return None
    remaining = openalex_usage_snapshot().get("credits_remaining")
    return int(remaining) if isinstance(remaining, int) else None


def provider_budget_ok(source: str, *, reserve: int = RESERVED_USER_CALLS) -> bool:
    """True when a BACKGROUND op may still call *source* and leave `reserve`
    calls for the user (task 37 B).

    Sources with no finite remaining-quota signal always pass — they're paced by
    per-second politeness + the idle-gate, not a daily reserve.
    """
    remaining = provider_remaining_credits(source)
    if remaining is None:
        return True
    return remaining - int(reserve) > 0


def openalex_usage_delta(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Compute OpenAlex usage deltas for one operation."""
    before_calls = int(before.get("request_count") or 0)
    after_calls = int(after.get("request_count") or 0)
    calls = max(0, after_calls - before_calls)

    before_used = before.get("credits_used")
    after_used = after.get("credits_used")
    credits_delta = None
    if isinstance(before_used, int) and isinstance(after_used, int):
        credits_delta = max(0, after_used - before_used)

    return {
        "openalex_calls": calls,
        "openalex_retries": max(
            0,
            int(after.get("retry_count") or 0) - int(before.get("retry_count") or 0),
        ),
        "openalex_rate_limited_events": max(
            0,
            int(after.get("rate_limited_events") or 0)
            - int(before.get("rate_limited_events") or 0),
        ),
        "openalex_calls_saved_by_cache": max(
            0,
            int(after.get("calls_saved_by_cache") or 0)
            - int(before.get("calls_saved_by_cache") or 0),
        ),
        "openalex_credits_used": credits_delta,
        "openalex_credits_remaining": after.get("credits_remaining"),
        "openalex_summary": after.get("summary"),
    }
