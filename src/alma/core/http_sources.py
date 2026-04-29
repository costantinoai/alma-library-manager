"""Shared HTTP transport for third-party source adapters.

This module provides per-source sessions, source-aware throttling, and retry
behavior so discovery and identity workflows do not each reinvent request
handling.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
import logging
import random
import threading
import time
from typing import Any, Callable, Iterator, Optional

import requests

from alma.config import (
    get_app_user_agent,
    get_contact_email,
    get_crossref_mailto,
    get_semantic_scholar_api_key,
)

logger = logging.getLogger(__name__)

_RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})
_ACTIVE_DIAGNOSTICS: ContextVar["SourceDiagnosticsCollector | None"] = ContextVar(
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
    auth_header_factory: Optional[Callable[[], dict[str, str]]] = None
    auth_param_factory: Optional[Callable[[], dict[str, str]]] = None
    # Factories return the *current* rate budget per request. They let
    # polite-pool eligibility (e.g. a contact email added through the
    # Settings UI after startup) take effect immediately, instead of
    # being frozen at import time.
    min_interval_factory: Optional[Callable[[], float]] = None
    max_concurrency_factory: Optional[Callable[[], int]] = None


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


def get_active_source_diagnostics() -> "SourceDiagnosticsCollector | None":
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
    return 0.12 if get_crossref_mailto() else 0.25


def _crossref_max_concurrency() -> int:
    return 3 if get_crossref_mailto() else 1


def _orcid_headers() -> dict[str, str]:
    return {"Accept": "application/json"}


_POLICIES: dict[str, SourcePolicy] = {
    "semantic_scholar": SourcePolicy(
        name="semantic_scholar",
        base_url="https://api.semanticscholar.org/graph/v1",
        min_interval_seconds=1.05,
        max_concurrency=1,
        max_retries=3,
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
    "arxiv": SourcePolicy(
        name="arxiv",
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
    "orcid": SourcePolicy(
        name="orcid",
        base_url="https://pub.orcid.org/v3.0",
        min_interval_seconds=0.35,
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
        # Concurrency is gated dynamically (see `_concurrency_slot`) so
        # the limit can grow/shrink with runtime config — e.g. Crossref
        # moving between anonymous and polite pool when a contact email
        # is added or removed from the Settings UI.
        self._concurrency_cond = threading.Condition()
        self._active_requests = 0

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
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

    def _apply_auth_headers(self, headers: Optional[dict[str, str]]) -> dict[str, str]:
        merged = dict(headers or {})
        if self._policy.auth_header_factory:
            merged.update(self._policy.auth_header_factory() or {})
        return merged

    def _apply_auth_params(self, params: Optional[dict[str, Any]]) -> dict[str, Any]:
        merged = dict(params or {})
        if self._policy.auth_param_factory:
            merged.update(self._policy.auth_param_factory() or {})
        return merged

    def _current_min_interval(self) -> float:
        if self._policy.min_interval_factory is not None:
            try:
                return max(0.0, float(self._policy.min_interval_factory()))
            except Exception:
                pass
        return max(0.0, float(self._policy.min_interval_seconds))

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
                self._concurrency_cond.wait()
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
                time.sleep(wait)
            self._next_request_at = time.monotonic() + interval

    def _retry_wait(self, response: Optional[requests.Response], attempt: int) -> float:
        if response is not None:
            retry_after = response.headers.get("retry-after")
            if retry_after:
                try:
                    return max(0.0, float(retry_after))
                except (TypeError, ValueError):
                    try:
                        dt = parsedate_to_datetime(retry_after)
                        return max(0.0, dt.timestamp() - time.time())
                    except Exception:
                        pass
        base = min(8.0, 0.75 * (2 ** attempt))
        return base + random.uniform(0.0, 0.4)

    def request(
        self,
        method: str,
        path_or_url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> requests.Response:
        url = self._prepare_url(path_or_url)
        request_params = self._apply_auth_params(params)
        request_headers = self._apply_auth_headers(headers)
        timeout_value = float(timeout or self._policy.default_timeout)
        diagnostics = get_active_source_diagnostics()
        path_label = path_or_url if path_or_url.startswith("/") else url.replace(self._policy.base_url, "", 1) or "/"

        last_exc: Optional[Exception] = None
        last_resp: Optional[requests.Response] = None
        for attempt in range(max(0, self._policy.max_retries) + 1):
            with self._concurrency_slot():
                self._wait_for_slot()
                started_at = time.monotonic()
                try:
                    response = self._session().request(
                        method.upper(),
                        url,
                        params=request_params,
                        headers=request_headers,
                        json=json,
                        timeout=timeout_value,
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
                except requests.exceptions.RequestException as exc:
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
                    if attempt >= self._policy.max_retries:
                        raise
                    wait = self._retry_wait(None, attempt)
                    logger.debug(
                        "Source request error (%s %s attempt %d/%d): %s; retrying in %.2fs",
                        self._policy.name,
                        url,
                        attempt + 1,
                        self._policy.max_retries + 1,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                    continue

            if response.status_code not in _RETRYABLE_STATUSES:
                return response

            if attempt >= self._policy.max_retries:
                return response

            wait = self._retry_wait(response, attempt)
            logger.debug(
                "Source returned retryable status (%s %s attempt %d/%d): HTTP %d; retrying in %.2fs",
                self._policy.name,
                url,
                attempt + 1,
                self._policy.max_retries + 1,
                response.status_code,
                wait,
            )
            time.sleep(wait)

        if last_resp is not None:
            return last_resp
        if last_exc is not None:
            raise last_exc
        raise RuntimeError(f"Unreachable request failure for source {self._policy.name}")

    def get(
        self,
        path_or_url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> requests.Response:
        return self.request("GET", path_or_url, params=params, headers=headers, timeout=timeout)

    def post(
        self,
        path_or_url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        json: Optional[dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> requests.Response:
        return self.request("POST", path_or_url, params=params, headers=headers, json=json, timeout=timeout)


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
            "summary": "unavailable",
        }


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
