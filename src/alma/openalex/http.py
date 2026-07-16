"""Centralized OpenAlex HTTP client with API-key auth, rate-limit tracking,
and retry logic.

As of Feb 2026, OpenAlex requires an API key for all requests (the polite
pool based on ``mailto`` has been discontinued).  Free tier provides
100,000 credits/day.  Singleton GETs (e.g. ``/works/{id}``) cost 0 credits;
list requests cost 1 credit each.

Usage::

    from alma.openalex.http import get_client

    client = get_client()
    resp = client.get("/works", params={"filter": "author.id:A123"})
    work = client.get_singleton("W12345")

Operation-scoped caching::

    client = get_client()
    with client.operation_cache("enrichment-run-42") as op:
        # All responses within this block are cached for the operation
        # duration.  Negative results (404s) are also cached to avoid
        # re-querying known failures.
        resp = client.get("/works/doi:10.1234/example")
        # ... later, same URL is served from the operation cache.
    # Cache is automatically cleared when the block exits.
    print(op.summary())  # human-readable API-usage summary
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
import os
import random
import re
import threading
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import requests
from requests.structures import CaseInsensitiveDict

from alma.config import get_openalex_api_key, get_openalex_email

logger = logging.getLogger(__name__)

BASE_URL = "https://api.openalex.org"
_UNSET = object()

# Retryable HTTP status codes
_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503})

# Retry configuration
_MAX_RETRIES = 5
_BACKOFF_BASE = 1.0  # seconds
_BACKOFF_MAX = 60.0  # seconds
_JITTER_MAX = 1.0  # seconds

# Credit warning threshold (warn when remaining < 10% of limit)
_CREDIT_WARN_FRACTION = 0.10

# Log credit summary every N requests
_CREDIT_LOG_INTERVAL = 50

# Response cache configuration (best-effort in-process cache).
_CACHE_TTL_SECONDS = 300
_CACHE_MAX_ENTRIES = 1024
_CACHEABLE_STATUSES = frozenset({200, 404})

# OpenAlex per-page hard maximum (Feb-2026 API: was 200, now 100). Enforced
# centrally in `get()` so no caller can trip a 400 with a stale clamp.
_MAX_PER_PAGE = 100

# Cost classes under the Feb-2026 usage-based pricing. Singleton entity GETs
# are free and unlimited; list+filter costs $0.10/1k; search costs $1.00/1k.
_CLASS_COSTS_USD = {"singleton": 0.0, "list": 0.0001, "search": 0.001}

# Credit units (the X-RateLimit headers count the $0.0001 list-call unit; the
# daily $1.00 free budget is 10,000 units) charged per `?search` call. Budget
# gates sizing a search-heavy run MUST multiply their planned call count by
# this — comparing raw paper counts against unit-denominated remaining credits
# understates the need 10× (2026-07-04 onboarding e2e finding).
SEARCH_COST_CREDITS = 10

# Same unit table keyed by cost class, for the drained-pool fail-fast: a 429
# whose remaining credits can't cover THIS call's class cannot succeed on any
# backoff within the run (the pool refills at the daily reset). Singleton GETs
# are free — their 429s are per-second bursts and take the normal backoff.
_CLASS_COST_CREDITS = {"singleton": 0, "list": 1, "search": SEARCH_COST_CREDITS}

# Anything under an entity collection path is a singleton GET — note `.+`
# not `[^/]+`: DOI-form ids (`/works/doi:10.1234/abc`) contain slashes.
_SINGLETON_PATH_RE = re.compile(
    r"^/(works|authors|sources|institutions|topics|publishers|funders|concepts|keywords)/.+"
)


def classify_request(path: str, params: dict[str, Any] | None = None) -> str:
    """Return the OpenAlex cost class of a request: ``singleton`` (free,
    unlimited), ``search`` ($1.00/1k — 10× list), or ``list`` ($0.10/1k).

    ONE canonical classifier — budget gates, usage snapshots, and fallback
    logic must all route through this instead of re-deriving URL shapes.
    """
    p = params or {}
    if str(p.get("search") or "").strip():
        return "search"
    clean = path if path.startswith("/") else f"/{path}"
    clean = clean.split("?", 1)[0].rstrip("/")
    if _SINGLETON_PATH_RE.match(clean) and not str(p.get("filter") or "").strip():
        return "singleton"
    return "list"


@dataclasses.dataclass
class OperationStats:
    """Tracks API-usage statistics for a single logical operation.

    Created automatically by :meth:`OpenAlexClient.operation_cache` and
    populated as requests flow through the client.  Call :meth:`summary` for
    a human-readable one-liner, or read the fields directly.
    """

    name: str = ""
    calls_total: int = 0
    calls_saved_by_cache: int = 0
    calls_saved_by_negative_cache: int = 0
    retry_count: int = 0
    rate_limited_events: int = 0
    batch_requests: int = 0
    batch_items: int = 0

    def summary(self) -> str:
        """Return a concise human-readable summary."""
        parts = [
            f"op={self.name}" if self.name else "op=<unnamed>",
            f"calls={self.calls_total}",
            f"cache_saved={self.calls_saved_by_cache}",
            f"neg_cache_saved={self.calls_saved_by_negative_cache}",
            f"retries={self.retry_count}",
            f"rate_limited={self.rate_limited_events}",
        ]
        if self.batch_requests:
            parts.append(f"batches={self.batch_requests}({self.batch_items} items)")
        return ", ".join(parts)


class OpenAlexClient:
    """HTTP client for the OpenAlex API with auth, rate-limit tracking, and
    automatic retries.

    Parameters
    ----------
    api_key : str or None
        OpenAlex API key.  When omitted, read from settings/env.  When passed
        explicitly as ``None``, disable API-key auth for this instance.
    mailto : str or None
        Contact email (still accepted but no longer the primary auth).
    """

    def __init__(
        self,
        api_key: str | None | object = _UNSET,
        mailto: str | None = None,
    ) -> None:
        self._api_key = _resolve_api_key() if api_key is _UNSET else api_key
        self._mailto = mailto or get_openalex_email()

        # Per-thread sessions so the client is safe to call from a concurrent
        # fetch pool (`core.fetch_pipeline`). A shared `requests.Session` is not
        # guaranteed thread-safe; a thread-local one keeps connection pooling
        # while removing the hazard — same pattern as `SourceHttpClient`.
        self._local = threading.local()
        # Guards the rate-limit / usage counters below against lost updates when
        # several fetch workers update them concurrently.
        self._stats_lock = threading.Lock()

        # Rate-limit bookkeeping
        self._rate_limit: int | None = None  # X-RateLimit-Limit
        self._rate_remaining: int | None = None  # X-RateLimit-Remaining
        self._credits_used: int | None = None  # X-RateLimit-Credits-Used
        self._rate_reset: str | None = None  # X-RateLimit-Reset
        self._request_count: int = 0
        self._retry_count: int = 0
        self._rate_limited_events: int = 0
        self._calls_saved_by_cache: int = 0
        # Upstream calls by cost class (cache hits excluded) — backs the
        # per-class spend estimate in the usage snapshot / ApiBudgetCard.
        self._class_counts: dict[str, int] = {"singleton": 0, "list": 0, "search": 0}
        self._cache_lock = threading.RLock()
        # key -> (expires_at, status_code, headers, content, url, reason, encoding)
        self._response_cache: dict[
            str,
            tuple[float, int, dict[str, str], bytes, str, str, str | None],
        ] = {}

        # Operation-scoped cache (active only inside `operation_cache()` block)
        self._op_cache: dict[str, tuple[int, dict[str, str], bytes, str, str, str | None]] | None = None
        self._op_negative_cache: set[str] | None = None
        self._op_stats: OperationStats | None = None

    def _session(self) -> requests.Session:
        """Return this thread's `requests.Session` (lazily created).

        Thread-local so concurrent fetch-pool workers never share one
        Session; each carries the same ``User-Agent`` and reuses its own
        connection pool.
        """
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update({"User-Agent": "ALMa/2.0"})
            self._local.session = session
        return session

    # ------------------------------------------------------------------
    # Operation-scoped cache
    # ------------------------------------------------------------------

    @contextlib.contextmanager
    def operation_cache(self, name: str = ""):
        """Context manager that enables a per-operation request cache.

        While active, every response (including 404s / negative results)
        is cached in memory.  Duplicate requests within the same operation
        are served from this cache instead of hitting the network.

        The cache is automatically discarded when the block exits.

        Yields an :class:`OperationStats` instance that accumulates
        usage counters for the duration of the operation.

        Example::

            with client.operation_cache("enrich-batch") as stats:
                client.get("/works/doi:10.1234/foo")
                client.get("/works/doi:10.1234/foo")   # served from cache
            print(stats.summary())
        """
        stats = OperationStats(name=name)
        prev_cache = self._op_cache
        prev_neg = self._op_negative_cache
        prev_stats = self._op_stats
        self._op_cache = {}
        self._op_negative_cache = set()
        self._op_stats = stats
        try:
            yield stats
        finally:
            self._op_cache = prev_cache
            self._op_negative_cache = prev_neg
            self._op_stats = prev_stats
            if stats.calls_total > 0:
                logger.info("Operation cache [%s]: %s", name, stats.summary())

    def _op_cache_get(self, key: str) -> requests.Response | None:
        """Serve from the operation-scoped cache if active and key present."""
        if self._op_cache is None:
            return None
        if key in self._op_negative_cache:
            # Negative-cache hit: we know this URL returned 404 earlier.
            if self._op_stats:
                self._op_stats.calls_saved_by_negative_cache += 1
            resp = requests.Response()
            resp.status_code = 404
            resp._content = b""
            resp.headers = CaseInsensitiveDict({})
            resp.url = key
            resp.reason = "Not Found (negative cache)"
            resp.encoding = None
            return resp
        entry = self._op_cache.get(key)
        if entry is None:
            return None
        status_code, headers, content, url, reason, encoding = entry
        if self._op_stats:
            self._op_stats.calls_saved_by_cache += 1
        resp = requests.Response()
        resp.status_code = int(status_code)
        resp._content = content
        resp.headers = CaseInsensitiveDict(headers or {})
        resp.url = url
        resp.reason = reason
        resp.encoding = encoding
        return resp

    def _op_cache_put(self, key: str, resp: requests.Response, *, fallback_url: str) -> None:
        """Store a response in the operation-scoped cache if active."""
        if self._op_cache is None:
            return
        status_code = int(getattr(resp, "status_code", 0) or 0)
        if status_code == 404:
            self._op_negative_cache.add(key)
            return
        if status_code not in _CACHEABLE_STATUSES:
            return
        headers = dict(getattr(resp, "headers", {}) or {})
        content_raw = getattr(resp, "content", b"")
        if isinstance(content_raw, str):
            content = content_raw.encode("utf-8")
        elif isinstance(content_raw, (bytes, bytearray)):
            content = bytes(content_raw)
        else:
            content = b""
        url = self._sanitize_url_for_storage(str(getattr(resp, "url", "") or fallback_url))
        reason = str(getattr(resp, "reason", "") or "")
        encoding = getattr(resp, "encoding", None)
        self._op_cache[key] = (status_code, headers, content, url, reason, encoding)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        timeout: float = 20,
    ) -> requests.Response:
        """Send a GET request to the OpenAlex API.

        ``path`` is appended to ``BASE_URL`` (e.g. ``"/works"``).  Auth
        parameters are injected automatically.  Retries on 429 / 5xx with
        exponential backoff + jitter.
        """
        url = f"{BASE_URL}{path}" if path.startswith("/") else f"{BASE_URL}/{path}"
        merged = self._inject_auth(params)
        # Central per-page clamp: the API maximum dropped 200 → 100 with the
        # Feb-2026 pricing release; enforcing here fixes every caller at once.
        for key in ("per-page", "per_page"):
            if key in merged:
                try:
                    merged[key] = max(1, min(int(merged[key]), _MAX_PER_PAGE))
                except (TypeError, ValueError):
                    pass
        return self._request_with_retry(url, merged, timeout)

    def get_singleton(
        self,
        work_id: str,
        select: str | None = None,
        timeout: float = 20,
    ) -> dict[str, Any] | None:
        """Fetch a single work by ID (0 credits).

        Returns parsed JSON dict, or ``None`` on 404.
        """
        path = f"/works/{work_id}"
        params: dict[str, Any] = {}
        if select:
            params["select"] = select
        resp = self.get(path, params=params, timeout=timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def seed_cache(
        self,
        path: str,
        params: dict[str, Any] | None,
        resp: requests.Response,
    ) -> None:
        """Store *resp* in the response cache as if fetched via ``get(path, params)``.

        Lets a caller that resolved a resource by ONE identifier make the same
        response servable under the resource's OTHER identifier (e.g. a work
        fetched by DOI seeded under its ``/works/W…`` URL), so an immediately
        following resolve by the sibling id is a cache hit instead of a second
        upstream round-trip. Honors the cache's normal TTL, eviction, and
        cacheable-status rules.
        """
        url = f"{BASE_URL}{path}" if path.startswith("/") else f"{BASE_URL}/{path}"
        key = self._cache_key(url, self._inject_auth(params))
        self._store_cached_response(key, resp, fallback_url=url)
        self._op_cache_put(key, resp, fallback_url=url)

    def get_rate_limit_status(self, timeout: float = 10) -> dict[str, Any] | None:
        """Return authoritative rate-limit status from OpenAlex ``/rate-limit``.

        Requires a configured API key. Returns ``None`` if unavailable.
        Always bypasses the response cache to return fresh data.
        """
        if not self._api_key:
            return None
        try:
            # Direct request — bypass cache so we always get live values.
            url = f"{BASE_URL}/rate-limit"
            merged = self._inject_auth(None)
            resp = self._session().get(url, params=merged, timeout=timeout)
            self._update_rate_limits(resp)
            if resp.status_code != 200:
                return None
            data = resp.json() or {}
            if isinstance(data, dict):
                return data
            return None
        except Exception:
            return None

    def probe_credentials(self, timeout: float = 8) -> dict[str, Any]:
        """Live check of the configured OpenAlex API key.

        Mirrors the Semantic Scholar status contract consumed by
        Settings → Connections (``configured`` / ``valid`` / ``detail``),
        so the OpenAlex card can render the same connection pill:

          - ``configured=False`` — no API key set.
          - ``valid=True``       — OpenAlex accepted the key (200, or 429,
                                   which still proves the key authenticated
                                   before the rate limiter kicked in).
          - ``valid=False``      — OpenAlex rejected the key (401 / 403).
          - ``valid=None``       — probe could not complete or returned an
                                   unexpected status; validity unknown.

        Probes ``/rate-limit`` directly (bypasses the response cache, like
        :meth:`get_rate_limit_status`) so a manual re-check always re-probes
        with the current key.
        """
        if not self._api_key:
            return {"configured": False, "valid": None, "detail": "No API key set."}
        try:
            url = f"{BASE_URL}/rate-limit"
            merged = self._inject_auth(None)
            resp = self._session().get(url, params=merged, timeout=timeout)
            self._update_rate_limits(resp)
        except Exception as exc:
            return {
                "configured": True,
                "valid": None,
                "detail": f"Could not reach OpenAlex ({exc.__class__.__name__}).",
            }
        if resp.status_code == 200:
            return {"configured": True, "valid": True, "detail": "Key accepted."}
        if resp.status_code == 429:
            return {
                "configured": True,
                "valid": True,
                "detail": "Key accepted (rate-limited right now).",
            }
        if resp.status_code in (401, 403):
            return {
                "configured": True,
                "valid": False,
                "detail": "OpenAlex rejected the key (invalid or unauthorized).",
            }
        return {
            "configured": True,
            "valid": None,
            "detail": f"Unexpected OpenAlex response ({resp.status_code}).",
        }

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _inject_auth(
        self, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Return a copy of *params* with the ``api_key`` added.

        The ``mailto``/polite-pool parameter was discontinued by OpenAlex on
        2026-02-13 and is now ignored server-side — we no longer send it (the
        configured contact email still feeds the Crossref polite pool + the
        User-Agent elsewhere). The API key is the sole identity mechanism.
        """
        merged = dict(params) if params else {}
        if self._api_key:
            merged["api_key"] = self._api_key
        return merged

    # ------------------------------------------------------------------
    # Retry logic
    # ------------------------------------------------------------------

    def _request_with_retry(
        self,
        url: str,
        params: dict[str, Any],
        timeout: float,
    ) -> requests.Response:
        """Execute a GET with bounded exponential backoff on retryable errors."""
        cache_key = self._cache_key(url, params)

        # 1) Check operation-scoped cache first (tighter scope, no TTL)
        op_cached = self._op_cache_get(cache_key)
        if op_cached is not None:
            return op_cached

        # 2) Check persistent response cache
        cached = self._get_cached_response(cache_key)
        if cached is not None:
            return cached

        # Track call in operation stats
        if self._op_stats:
            self._op_stats.calls_total += 1

        # Count one upstream call per logical request (cache hits returned
        # above; retries below don't re-count) under its pricing class.
        cost_class = classify_request(urlsplit(url).path, params)
        with self._stats_lock:
            self._class_counts[cost_class] = self._class_counts.get(cost_class, 0) + 1

        last_exc: Exception | None = None
        last_resp: requests.Response | None = None

        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = self._session().get(url, params=params, timeout=timeout)
                self._update_rate_limits(resp)
                last_resp = resp

                if resp.status_code not in _RETRYABLE_STATUSES:
                    self._store_cached_response(cache_key, resp, fallback_url=url)
                    self._op_cache_put(cache_key, resp, fallback_url=url)
                    return resp

                if resp.status_code == 429:
                    self._rate_limited_events += 1
                    if self._op_stats:
                        self._op_stats.rate_limited_events += 1
                    # Daily quota exhausted: this 429's header (just parsed by
                    # _update_rate_limits above) reports 0 remaining, and the pool
                    # only refills at X-RateLimit-Reset (hours away) — no backoff
                    # within this run can succeed. Fail fast instead of sleeping
                    # _MAX_RETRIES × up to 60s, which is the "hangs forever" on a
                    # drained key (seen live on the id-candidates path). A
                    # transient per-second burst 429 still reports remaining > 0
                    # and takes the normal backoff path below.
                    with self._stats_lock:
                        remaining = self._rate_remaining
                    # "Exhausted" is class-relative: a search needs
                    # SEARCH_COST_CREDITS units, so a pool sitting at 1-9
                    # remaining is just as dead for searches as 0 — the old
                    # `<= 0` check let drained-pool searches grind full backoff
                    # ladders for hours (2026-07-04 onboarding e2e wedge).
                    daily_exhausted = remaining is not None and remaining < _CLASS_COST_CREDITS.get(
                        cost_class, 1
                    )
                    if daily_exhausted:
                        logger.warning(
                            "OpenAlex daily quota exhausted (0 remaining, resets %s) — "
                            "failing fast on %s instead of retrying",
                            self._rate_reset or "unknown",
                            url,
                        )
                        break

                if attempt >= _MAX_RETRIES:
                    break

                # Retryable status -- back off
                self._retry_count += 1
                if self._op_stats:
                    self._op_stats.retry_count += 1
                wait = self._retry_wait(resp, attempt)
                logger.warning(
                    "OpenAlex %d on %s (attempt %d/%d), retrying in %.1fs",
                    resp.status_code,
                    url,
                    attempt + 1,
                    _MAX_RETRIES + 1,
                    wait,
                )
                time.sleep(wait)

            except requests.exceptions.RequestException as exc:
                last_exc = exc
                if attempt >= _MAX_RETRIES:
                    break
                self._retry_count += 1
                if self._op_stats:
                    self._op_stats.retry_count += 1
                wait = self._backoff_wait(attempt)
                logger.warning(
                    "OpenAlex request error on %s (attempt %d/%d): %s, retrying in %.1fs",
                    url,
                    attempt + 1,
                    _MAX_RETRIES + 1,
                    exc,
                    wait,
                )
                time.sleep(wait)

        # Exhausted retries
        if last_exc is not None:
            raise last_exc
        if last_resp is not None:
            raise requests.exceptions.HTTPError(
                f"OpenAlex returned {last_resp.status_code} after {_MAX_RETRIES + 1} attempts: {url}",
                response=last_resp,
            )
        # If we got here from status-code retries, raise for the last response
        raise requests.exceptions.HTTPError(
            f"OpenAlex request failed after {_MAX_RETRIES + 1} attempts: {url}"
        )

    @staticmethod
    def _cache_key(url: str, params: dict[str, Any]) -> str:
        """Build a stable cache key from URL and query params."""
        safe_params = {k: v for k, v in (params or {}).items() if str(k).lower() != "api_key"}
        try:
            serialized = json.dumps(safe_params, sort_keys=True, separators=(",", ":"), default=str)
        except Exception:
            serialized = str(sorted(safe_params.items()))
        return f"{url}?{serialized}"

    @staticmethod
    def _sanitize_url_for_storage(url: str) -> str:
        """Return URL with sensitive query keys removed."""
        raw = (url or "").strip()
        if not raw:
            return raw
        try:
            parts = urlsplit(raw)
            safe_query = [
                (k, v)
                for k, v in parse_qsl(parts.query, keep_blank_values=True)
                if str(k).lower() not in {"api_key"}
            ]
            return urlunsplit(
                (
                    parts.scheme,
                    parts.netloc,
                    parts.path,
                    urlencode(safe_query, doseq=True),
                    parts.fragment,
                )
            )
        except Exception:
            return raw

    def _get_cached_response(self, key: str) -> requests.Response | None:
        """Return a cached response if present and unexpired."""
        now = time.time()
        with self._cache_lock:
            entry = self._response_cache.get(key)
            if not entry:
                return None
            expires_at, status_code, headers, content, url, reason, encoding = entry
            if expires_at <= now:
                self._response_cache.pop(key, None)
                return None
            self._calls_saved_by_cache += 1
        resp = requests.Response()
        resp.status_code = int(status_code)
        resp._content = content if isinstance(content, bytes) else bytes(content or b"")
        resp.headers = CaseInsensitiveDict(headers or {})
        resp.url = url
        resp.reason = reason
        resp.encoding = encoding
        return resp

    def _store_cached_response(
        self, key: str, resp: requests.Response, *, fallback_url: str
    ) -> None:
        """Store a cacheable response snapshot."""
        status_code = int(getattr(resp, "status_code", 0) or 0)
        if status_code not in _CACHEABLE_STATUSES or _CACHE_TTL_SECONDS <= 0:
            return
        headers = dict(getattr(resp, "headers", {}) or {})
        content_raw = getattr(resp, "content", b"")
        if isinstance(content_raw, str):
            content = content_raw.encode("utf-8")
        elif isinstance(content_raw, (bytes, bytearray)):
            content = bytes(content_raw)
        else:
            content = b""
        url = self._sanitize_url_for_storage(str(getattr(resp, "url", "") or fallback_url))
        reason = str(getattr(resp, "reason", "") or "")
        encoding = getattr(resp, "encoding", None)
        expires_at = time.time() + float(_CACHE_TTL_SECONDS)

        with self._cache_lock:
            self._response_cache[key] = (
                expires_at,
                status_code,
                headers,
                content,
                url,
                reason,
                encoding,
            )
            if len(self._response_cache) > _CACHE_MAX_ENTRIES:
                # Remove up to 10% oldest/expired entries to keep memory bounded.
                items = sorted(self._response_cache.items(), key=lambda kv: kv[1][0])
                trim = max(1, _CACHE_MAX_ENTRIES // 10)
                for stale_key, _entry in items[:trim]:
                    self._response_cache.pop(stale_key, None)

    @staticmethod
    def _backoff_wait(attempt: int) -> float:
        """Compute wait time: exponential backoff capped at ``_BACKOFF_MAX``
        plus random jitter."""
        exp = min(_BACKOFF_BASE * (2 ** attempt), _BACKOFF_MAX)
        jitter = random.uniform(0, _JITTER_MAX)
        return exp + jitter

    @staticmethod
    def _retry_wait(resp: requests.Response, attempt: int) -> float:
        """Compute retry wait time honoring Retry-After when present.

        ``Retry-After`` can be an integer seconds value or an HTTP-date.
        Falls back to exponential backoff + jitter.
        """
        retry_after = (resp.headers.get("Retry-After") or "").strip()
        if retry_after:
            try:
                seconds = float(retry_after)
                if seconds >= 0:
                    return min(seconds, _BACKOFF_MAX)
            except ValueError:
                try:
                    target = parsedate_to_datetime(retry_after)
                    if target.tzinfo is None:
                        target = target.replace(tzinfo=timezone.utc)
                    now = datetime.now(timezone.utc)
                    seconds = (target - now).total_seconds()
                    if seconds >= 0:
                        return min(seconds, _BACKOFF_MAX)
                except Exception:
                    pass
        return OpenAlexClient._backoff_wait(attempt)

    # ------------------------------------------------------------------
    # Rate-limit tracking
    # ------------------------------------------------------------------

    def _update_rate_limits(self, resp: requests.Response) -> None:
        """Parse rate-limit headers and log warnings when credits run low."""
        headers = resp.headers

        limit_str = headers.get("X-RateLimit-Limit")
        remaining_str = headers.get("X-RateLimit-Remaining")
        reset_str = headers.get("X-RateLimit-Reset")

        # Lock the counter/field block so concurrent fetch workers don't lose
        # updates (these counters back the usage snapshot in Settings/Activity).
        with self._stats_lock:
            if limit_str is not None:
                try:
                    self._rate_limit = int(limit_str)
                except ValueError:
                    pass
            if remaining_str is not None:
                try:
                    self._rate_remaining = int(remaining_str)
                except ValueError:
                    pass
            # X-RateLimit-Credits-Used is the cost of *this* request (0 or 1),
            # not cumulative.  Derive cumulative used from limit - remaining.
            if self._rate_limit is not None and self._rate_remaining is not None:
                self._credits_used = self._rate_limit - self._rate_remaining
            if reset_str is not None:
                self._rate_reset = reset_str

            self._request_count += 1

        # Warn when remaining credits are low
        if (
            self._rate_limit is not None
            and self._rate_remaining is not None
            and self._rate_limit > 0
        ):
            fraction_remaining = self._rate_remaining / self._rate_limit
            if fraction_remaining < _CREDIT_WARN_FRACTION:
                logger.warning(
                    "OpenAlex credits running low: %d/%d remaining (resets %s)",
                    self._rate_remaining,
                    self._rate_limit,
                    self._rate_reset or "unknown",
                )

        # Periodic credit usage log
        if self._request_count % _CREDIT_LOG_INTERVAL == 0:
            logger.info(
                "OpenAlex usage after %d requests: %s credits used, %s remaining (limit %s)",
                self._request_count,
                self._credits_used if self._credits_used is not None else "?",
                self._rate_remaining if self._rate_remaining is not None else "?",
                self._rate_limit if self._rate_limit is not None else "?",
            )

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def rate_limit(self) -> int | None:
        """Total credit budget in the current window, or ``None`` if unknown."""
        return self._rate_limit

    @property
    def rate_remaining(self) -> int | None:
        """Credits remaining in the current window, or ``None`` if unknown."""
        return self._rate_remaining

    @property
    def credits_used(self) -> int | None:
        """Total credits used in the current window, or ``None`` if unknown."""
        return self._credits_used

    @property
    def rate_reset(self) -> str | None:
        """Raw ``X-RateLimit-Reset`` string from the last response, or ``None``."""
        return self._rate_reset

    @property
    def request_count(self) -> int:
        """Number of requests made through this client instance."""
        return self._request_count

    @property
    def retry_count(self) -> int:
        """Number of retry attempts performed by this client instance."""
        return self._retry_count

    @property
    def rate_limited_events(self) -> int:
        """Number of 429 events observed by this client instance."""
        return self._rate_limited_events

    @property
    def calls_saved_by_cache(self) -> int:
        """Number of upstream API calls avoided due to local response cache."""
        return self._calls_saved_by_cache

    @property
    def class_counts(self) -> dict[str, int]:
        """Upstream calls by pricing class since process start."""
        with self._stats_lock:
            return dict(self._class_counts)

    @property
    def estimated_spend_usd(self) -> float:
        """Estimated $ spent this process, from per-class call counts."""
        with self._stats_lock:
            return round(
                sum(
                    count * _CLASS_COSTS_USD.get(cls, 0.0)
                    for cls, count in self._class_counts.items()
                ),
                4,
            )

    def budget_drained(self, reserve: int = 0) -> bool:
        """True when the server-reported remaining credits are known and at or
        below *reserve*. Singleton requests stay free regardless — callers use
        this to decide when to flip known-ID fetches to the singleton path."""
        remaining = self._rate_remaining
        return remaining is not None and remaining <= max(0, int(reserve))

    def credits_summary(self) -> str:
        """Return a human-readable credit usage summary."""
        def _v(x: Any) -> str:
            return str(x) if x is not None else "?"
        return (
            f"{_v(self._credits_used)} credits used, "
            f"{_v(self._rate_remaining)} remaining "
            f"(limit {_v(self._rate_limit)}, {self._request_count} requests, "
            f"{self._retry_count} retries, {self._rate_limited_events} rate-limited, "
            f"{self._calls_saved_by_cache} cache-saved)"
        )


# ----------------------------------------------------------------------
# Module-level singleton
# ----------------------------------------------------------------------

_client: OpenAlexClient | None = None
_client_lock = threading.Lock()


def get_client() -> OpenAlexClient:
    """Return (or lazily create) the module-level ``OpenAlexClient`` singleton.

    Thread-safe.  The session is reused across calls for connection pooling.
    """
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        # Double-check after acquiring lock
        if _client is None:
            _client = OpenAlexClient()
            logger.debug("OpenAlexClient singleton initialized")
        return _client


def reset_client() -> None:
    """Discard the current singleton so the next :func:`get_client` call
    picks up fresh settings (e.g. after API key change in Settings page)."""
    global _client
    with _client_lock:
        _client = None
    logger.debug("OpenAlexClient singleton reset")


# ----------------------------------------------------------------------
# Internal helpers
# ----------------------------------------------------------------------


def _resolve_api_key() -> str | None:
    """Read the OpenAlex API key from environment or settings.

    Priority:
    1. ``OPENALEX_API_KEY`` environment variable
    2. OpenAlex key from the unified secret store
    """
    env_key = os.getenv("OPENALEX_API_KEY")
    if env_key:
        return env_key
    return get_openalex_api_key()
