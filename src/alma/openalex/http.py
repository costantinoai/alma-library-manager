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
import logging
import os
import random
import threading
import time
import json
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Optional

import requests
from requests.structures import CaseInsensitiveDict

from alma.config import get_openalex_email, get_openalex_api_key

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
        api_key: Optional[str] | object = _UNSET,
        mailto: Optional[str] = None,
    ) -> None:
        self._api_key = _resolve_api_key() if api_key is _UNSET else api_key
        self._mailto = mailto or get_openalex_email()

        self._session = requests.Session()
        self._session.headers.update({"User-Agent": "ALMa/2.0"})

        # Rate-limit bookkeeping
        self._rate_limit: Optional[int] = None  # X-RateLimit-Limit
        self._rate_remaining: Optional[int] = None  # X-RateLimit-Remaining
        self._credits_used: Optional[int] = None  # X-RateLimit-Credits-Used
        self._rate_reset: Optional[str] = None  # X-RateLimit-Reset
        self._request_count: int = 0
        self._retry_count: int = 0
        self._rate_limited_events: int = 0
        self._calls_saved_by_cache: int = 0
        self._cache_lock = threading.RLock()
        # key -> (expires_at, status_code, headers, content, url, reason, encoding)
        self._response_cache: Dict[
            str,
            tuple[float, int, Dict[str, str], bytes, str, str, Optional[str]],
        ] = {}

        # Operation-scoped cache (active only inside `operation_cache()` block)
        self._op_cache: Optional[Dict[str, tuple[int, Dict[str, str], bytes, str, str, Optional[str]]]] = None
        self._op_negative_cache: Optional[set[str]] = None
        self._op_stats: Optional[OperationStats] = None

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

    def _op_cache_get(self, key: str) -> Optional[requests.Response]:
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
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 20,
    ) -> requests.Response:
        """Send a GET request to the OpenAlex API.

        ``path`` is appended to ``BASE_URL`` (e.g. ``"/works"``).  Auth
        parameters are injected automatically.  Retries on 429 / 5xx with
        exponential backoff + jitter.
        """
        url = f"{BASE_URL}{path}" if path.startswith("/") else f"{BASE_URL}/{path}"
        merged = self._inject_auth(params)
        return self._request_with_retry(url, merged, timeout)

    def get_singleton(
        self,
        work_id: str,
        select: Optional[str] = None,
        timeout: float = 20,
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single work by ID (0 credits).

        Returns parsed JSON dict, or ``None`` on 404.
        """
        path = f"/works/{work_id}"
        params: Dict[str, Any] = {}
        if select:
            params["select"] = select
        resp = self.get(path, params=params, timeout=timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def get_list(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: float = 30,
    ) -> requests.Response:
        """Send a GET request for a list endpoint (1 credit each).

        Identical to :meth:`get` but uses a longer default timeout suitable
        for paginated list queries.
        """
        return self.get(path, params=params, timeout=timeout)

    def get_rate_limit_status(self, timeout: float = 10) -> Optional[Dict[str, Any]]:
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
            resp = self._session.get(url, params=merged, timeout=timeout)
            self._update_rate_limits(resp)
            if resp.status_code != 200:
                return None
            data = resp.json() or {}
            if isinstance(data, dict):
                return data
            return None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Auth helpers
    # ------------------------------------------------------------------

    def _inject_auth(
        self, params: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Return a copy of *params* with ``api_key`` and ``mailto`` added."""
        merged = dict(params) if params else {}
        if self._api_key:
            merged["api_key"] = self._api_key
        if self._mailto:
            merged["mailto"] = self._mailto
        return merged

    # ------------------------------------------------------------------
    # Retry logic
    # ------------------------------------------------------------------

    def _request_with_retry(
        self,
        url: str,
        params: Dict[str, Any],
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

        last_exc: Optional[Exception] = None
        last_resp: Optional[requests.Response] = None

        for attempt in range(_MAX_RETRIES + 1):
            try:
                resp = self._session.get(url, params=params, timeout=timeout)
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
    def _cache_key(url: str, params: Dict[str, Any]) -> str:
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

    def _get_cached_response(self, key: str) -> Optional[requests.Response]:
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
        used_str = headers.get("X-RateLimit-Credits-Used")
        reset_str = headers.get("X-RateLimit-Reset")

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
    def rate_limit(self) -> Optional[int]:
        """Total credit budget in the current window, or ``None`` if unknown."""
        return self._rate_limit

    @property
    def rate_remaining(self) -> Optional[int]:
        """Credits remaining in the current window, or ``None`` if unknown."""
        return self._rate_remaining

    @property
    def credits_used(self) -> Optional[int]:
        """Total credits used in the current window, or ``None`` if unknown."""
        return self._credits_used

    @property
    def rate_reset(self) -> Optional[str]:
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

_client: Optional[OpenAlexClient] = None
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


def _resolve_api_key() -> Optional[str]:
    """Read the OpenAlex API key from environment or settings.

    Priority:
    1. ``OPENALEX_API_KEY`` environment variable
    2. OpenAlex key from the unified secret store
    """
    env_key = os.getenv("OPENALEX_API_KEY")
    if env_key:
        return env_key
    return get_openalex_api_key()
