"""Author management API endpoints."""

import json
import logging
import os
import re
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FuturesTimeout
from datetime import datetime
from difflib import SequenceMatcher
from types import SimpleNamespace
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from alma.api.deps import (  # internal helpers for path resolution
    _data_dir,
    get_current_user,
    get_db,
    normalize_author_id,
    open_db_connection,
)
from alma.api.helpers import background_mode_requested, raise_internal
from alma.api.models import (
    AuthorCreate,
    AuthorFollowFromPaperRequest,
    AuthorFollowFromPaperResponse,
    AuthorResponse,
    AuthorSuggestionResponse,
    ErrorResponse,
)
from alma.application import authors as authors_app
from alma.application.author_identity import sticky_resolution_status
from alma.application.author_merge import MergeRefused
from alma.application.followed_authors import (
    apply_follow_state,
    schedule_followed_author_historical_backfill,
)
from alma.config import get_all_settings, get_fetch_year
from alma.core.backend import fetch_publications_by_id
from alma.core.concurrency import bounded_thread_pool
from alma.core.db_retry import commit_with_retry
from alma.core.db_write import run_write_unit, write_section
from alma.core.identifier_resolution import (
    resolve_scholar_candidates_from_sources,
    scholar_url_for_id,
)
from alma.core.operations import OperationOutcome, OperationRunner
from alma.core.operations.batch_outcome import batch_outcome
from alma.core.resolution import (
    get_author_sample_titles as _shared_get_author_sample_titles,
)
from alma.core.resolution import (
    resolve_author_identity,
    resolve_paper_openalex_work,
    summarize_author_resolution,
)
from alma.core.scholar_access import ScholarIntent, scholarly_client
from alma.core.sql_helpers import standalone_paper_sql
from alma.core.time import utcnow
from alma.core.utils import (
    canonical_lookup_doi,
    normalize_openalex_id,
    normalize_orcid,
    repair_display_text,
)
from alma.openalex.client import _normalize_openalex_author_id as _norm_oaid
from alma.openalex.client import resolve_openalex_candidates_from_scholar as _resolve_oa

logger = logging.getLogger(__name__)

# Backward-compatible patch target retained for identifier-resolution tests.
_resolve_oa_meta = _resolve_oa


def _log_author_action(
    db: sqlite3.Connection,
    action: str,
    message: str,
    *,
    author_id: str | None = None,
    data: dict | None = None,
    job_id: str | None = None,
) -> None:
    """Record ONE Activity entry for a foreground author "…"-menu action.

    The secondary author operations (confirm OpenAlex / identifiers, set
    identifiers / type, soft-remove, empty cache, resolve conflict, follow-
    from-paper, merge) run synchronously with no background-job envelope, yet
    the user needs them visible and auditable in Activity. This writes a
    single *completed* ``operation_status`` row, keyed under
    ``authors.action.<action>`` so it groups with the background author jobs;
    `list_all_job_statuses` merges the `operation_status` table into the
    Activity list, so the row (its `message` + structured `result`) shows up.

    Written through the request's OWN ``db`` connection inside a short
    ``run_write_unit`` — NOT via the scheduler's separate connection. That
    matters: the scheduler's `set_job_status` opens a plain (SQLite
    ``BEGIN DEFERRED``) transaction on a second connection, which acquires the
    write lock lazily and so loses the read→write upgrade race under a burst of
    near-simultaneous actions (silently dropping the row). Routing the write
    through the gated `db` instead means it is serialized behind the writer
    gate (``BEGIN IMMEDIATE`` + lock retry, both free from `run_write_unit`)
    and can never hit that hazard.

    Call this AFTER the action's own write unit has returned: it commits as its
    own independent unit, so a logging hiccup never touches the user's write
    (the whole thing is best-effort — failures are swallowed). ``job_id`` lets
    a caller that already owns a job id (the merge route, whose per-alt audit
    lines the application layer logs under that id) reuse it so the status row
    and those logs form ONE Activity entry.

    The write itself is the shared foreground-action writer
    (:func:`alma.core.operations.activity.record_foreground_action`); this
    adapter only fixes the author key namespace and result shape.
    """
    from alma.core.operations.activity import record_foreground_action

    record_foreground_action(
        db,
        operation_key=f"authors.action.{action}",
        message=message,
        result={"author_id": author_id, **(data or {})},
        job_id=job_id or f"author_action_{uuid.uuid4().hex[:10]}",
    )

def _deep_refresh_max_workers_default() -> int:
    """Resolve the deep-refresh concurrency cap from the env, with a
    safe default.

    Default is 4 — sweet spot between OpenAlex politeness (≈10 req/s
    recommended for the polite pool) and SQLite WAL contention. Every
    worker holds its own connection and
    `refresh_author_works_and_vectors` already commits between phases,
    so 4 parallel writers don't stall on the busy_timeout.

    Override via `ALMA_DEEP_REFRESH_WORKERS=N`. Clamped to [1, 16].
    """
    raw = os.environ.get("ALMA_DEEP_REFRESH_WORKERS", "").strip()
    if not raw:
        return 4
    try:
        n = int(raw)
    except ValueError:
        return 4
    return max(1, min(16, n))


_DEEP_REFRESH_MAX_WORKERS = _deep_refresh_max_workers_default()

# Explicit budget for the admin "process every eligible row in one run" sweep
# endpoints (refresh-all / dedup-all / gc-all / rehydrate-all). The bounded
# maintenance runners now take a REQUIRED limit (no hidden unbounded mode); these
# routes intentionally process the whole eligible set, so they pass this explicit,
# greppable full-sweep budget rather than relying on a None default. It matches
# the runners' internal 100k clamp ceiling, i.e. "all" for a single-user corpus.
_FULL_SWEEP_BUDGET = 100_000


_RESOLUTION_STATUSES = {
    "unresolved",
    "resolved_auto",
    "resolved_manual",
    "needs_manual_review",
    "no_match",
    "error",
    # User-acknowledged terminal: "this author can't be identified — stop
    # flagging it." Excluded from needs-attention + author-health counts.
    "dismissed",
}

# Interactive identifier-candidate lookup (the Identifiers tab fires this on
# open). It fans out to OpenAlex / Semantic Scholar / ORCID, each of which can
# retry with backoff (up to minutes) when an upstream is rate-limiting — so the
# synchronous call could appear to "hang forever". We run it on a small,
# persistent pool with a hard wall-clock deadline: past the deadline the request
# returns with `timed_out=True` (empty candidates + a Retry affordance) instead
# of blocking the tab. A timed-out worker keeps running to completion on its OWN
# short-lived connection (never the request's `db`, which is closed on return),
# so it can neither wedge the response nor race the request connection's close.
_ID_CANDIDATE_EXECUTOR = ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="alma-id-candidates"
)
_ID_CANDIDATE_DEADLINE_S = 25.0


def _immediate_job_response(
    job_id: str,
    *,
    operation_key: str,
    queued_message: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return queued activity payload, or completed result when already available."""
    from alma.api.scheduler import activity_envelope, get_job_status

    status = get_job_status(job_id) or {}
    job_status = str(status.get("status") or "queued")
    payload = activity_envelope(
        job_id,
        status=job_status if job_status in {"completed", "failed"} else "queued",
        operation_key=operation_key,
        message=str(status.get("message") or queued_message),
    )
    if extra:
        payload.update(extra)
    for field in ("started_at", "finished_at", "updated_at", "processed", "total", "current_author", "error"):
        if status.get(field) is not None:
            payload[field] = status.get(field)
    result = status.get("result")
    if isinstance(result, dict):
        payload.update(result)
    elif result is not None:
        payload["result"] = result
    return payload


def _sync_follow_state(db: sqlite3.Connection, author_id: str, *, followed: bool) -> bool:
    """Keep followed_authors, authors.author_type, and feed_monitors in sync.

    Thin wrapper around the canonical ``apply_follow_state`` helper so every
    follow/unfollow flow funnels through one place and no surface sees
    drift between the three tables. Returns True when a hydration row was
    enqueued — the caller must schedule the hydration sweep AFTER its
    transaction commits (in-transaction scheduling self-deadlocks against
    the scheduler's own connection; see ``apply_follow_state``).
    """
    return apply_follow_state(db, author_id, followed=followed)


def _id_resolution_settings() -> dict[str, bool]:
    """Identifier resolution toggles from settings.json (with safe defaults)."""
    cfg = get_all_settings()
    return {
        "semantic_scholar_enabled": bool(cfg.get("id_resolution_semantic_scholar_enabled", True)),
        "orcid_enabled": bool(cfg.get("id_resolution_orcid_enabled", True)),
        "scholar_scrape_auto_enabled": bool(cfg.get("id_resolution_scholar_scrape_auto_enabled", False)),
        "scholar_scrape_manual_enabled": bool(cfg.get("id_resolution_scholar_scrape_manual_enabled", False)),
    }


def _merge_scholar_candidates(*groups: list[dict]) -> list[dict]:
    by_id: dict[str, dict] = {}
    for group in groups:
        for cand in group or []:
            sid = (str(cand.get("scholar_id") or "")).strip()
            if not sid:
                continue
            score = float(cand.get("score") or 0.0)
            src = str(cand.get("source") or "").strip()
            existing = by_id.get(sid)
            if existing is None:
                c = dict(cand)
                if not c.get("scholar_url"):
                    c["scholar_url"] = scholar_url_for_id(sid)
                by_id[sid] = c
                continue
            ex_score = float(existing.get("score") or 0.0)
            source_set = {
                str(existing.get("source") or "").strip(),
                src,
            }
            if score > ex_score:
                c = dict(cand)
                c["source"] = ",".join(sorted(s for s in source_set if s))
                if not c.get("scholar_url"):
                    c["scholar_url"] = scholar_url_for_id(sid)
                by_id[sid] = c
            else:
                existing["source"] = ",".join(sorted(s for s in source_set if s))
    out = list(by_id.values())
    out.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    return out[:8]

router = APIRouter(
    prefix="/authors",
    tags=["authors"],
    responses={
        401: {"model": ErrorResponse, "description": "Unauthorized"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


def _get_author_sample_titles(db: sqlite3.Connection, author_id: str, limit: int = 3) -> list[str]:
    """Return representative titles for an author from the shared resolver layer."""
    return _shared_get_author_sample_titles(db, author_id, limit=limit)


def _apply_author_resolution_result(
    db: sqlite3.Connection,
    author_id: str,
    result,
) -> None:
    """Persist a shared author-resolution result onto an author row."""
    updates: list[str] = []
    params: list[Any] = []

    if result.author_name:
        updates.append("name = COALESCE(NULLIF(name, ''), ?)")
        params.append(result.author_name)
    if result.openalex_id:
        updates.append("openalex_id = COALESCE(NULLIF(openalex_id, ''), ?)")
        params.append(_norm_oaid(result.openalex_id))
    if result.scholar_id:
        updates.append("scholar_id = COALESCE(NULLIF(scholar_id, ''), ?)")
        params.append(result.scholar_id)
    if result.orcid:
        normalized_orcid = normalize_orcid(result.orcid)
        if normalized_orcid:
            owner = db.execute(
                """
                SELECT id
                FROM authors
                WHERE lower(trim(orcid)) = lower(?)
                  AND id != ?
                LIMIT 1
                """,
                (normalized_orcid, author_id),
            ).fetchone()
            if owner:
                logger.warning(
                    "Skipping ORCID %s for %s during author resolution; already owned by %s",
                    normalized_orcid,
                    author_id,
                    owner["id"] if isinstance(owner, sqlite3.Row) else owner[0],
                )
            else:
                updates.append("orcid = COALESCE(NULLIF(orcid, ''), ?)")
                params.append(normalized_orcid)

    profile = result.openalex_profile or {}
    institution = str(profile.get("institution") or "").strip()
    if institution:
        updates.append("affiliation = COALESCE(NULLIF(affiliation, ''), ?)")
        params.append(institution)
    works_count = profile.get("works_count")
    if works_count is not None:
        updates.append("works_count = MAX(COALESCE(works_count, 0), ?)")
        params.append(int(works_count or 0))
    cited_by_count = profile.get("cited_by_count")
    if cited_by_count is not None:
        updates.append("citedby = MAX(COALESCE(citedby, 0), ?)")
        params.append(int(cited_by_count or 0))
    h_index = profile.get("h_index")
    if h_index is not None:
        updates.append("h_index = MAX(COALESCE(h_index, 0), ?)")
        params.append(int(h_index or 0))
    topics = profile.get("topics") or []
    if topics:
        topic_terms = [str(item.get("term") or "").strip() for item in topics if str(item.get("term") or "").strip()]
        if topic_terms:
            updates.append("interests = COALESCE(NULLIF(interests, ''), ?)")
            params.append(json.dumps(topic_terms))

    updates.append("id_resolution_status = ?")
    params.append(sticky_resolution_status(db, author_id, result.status))
    updates.append("id_resolution_reason = ?")
    params.append(summarize_author_resolution(result))
    updates.append("id_resolution_updated_at = ?")
    params.append(utcnow().isoformat())

    params.append(author_id)
    db.execute(f"UPDATE authors SET {', '.join(updates)} WHERE id = ?", tuple(params))
    if result.openalex_id or result.scholar_id or result.orcid:
        # Identifier acquisition is the author equivalent of a paper insert:
        # durably join the normal hydration lane now, rather than requiring a
        # later maintenance sweep to discover the new work.
        from alma.services.author_hydrate import enqueue_pending_author_hydration

        enqueue_pending_author_hydration(
            db,
            author_id,
            priority="low",
            reason="identity_resolved",
        )


def _summarize_hierarchical_identity(result) -> str:
    """Build the persisted reason from the current hierarchical identity bundle."""
    details = [
        str(e.detail or "").strip()
        for e in list(getattr(result, "evidence", []) or [])[:4]
        if str(e.detail or "").strip()
    ]
    if not details:
        if getattr(result, "author_name", None):
            details.append(f"No confident identity match found for {result.author_name}")
        else:
            details.append("No confident identity match found")
    confidence = float(getattr(result, "confidence", 0.0) or 0.0)
    if confidence:
        details.append(f"confidence={confidence:.2f}")
    openalex_id = str(getattr(result, "openalex_id", "") or "").strip()
    if openalex_id:
        details.append(f"openalex={_norm_oaid(openalex_id)}")
    scholar_id = str(getattr(result, "scholar_id", "") or "").strip()
    if scholar_id:
        details.append(f"scholar={scholar_id}")
    orcid = normalize_orcid(str(getattr(result, "orcid", "") or "")) or ""
    if orcid:
        details.append(f"orcid={orcid}")
    return "; ".join(details)[:1000]


def _load_author_refresh_target(
    db: sqlite3.Connection,
    requested_author_id: str,
) -> tuple[str, sqlite3.Row]:
    """Load the exact local author row used by refresh jobs."""
    requested = str(requested_author_id or "").strip()
    cursor = db.execute(
        "SELECT name, openalex_id, scholar_id, orcid, last_fetched_at FROM authors WHERE id=?",
        (requested,),
    )
    row = cursor.fetchone()
    if row:
        return requested, row

    raise HTTPException(status_code=404, detail="Author not found")


def _resolve_and_persist_author_identity(
    db: sqlite3.Connection,
    author_id: str,
    row: sqlite3.Row,
    *,
    job_id: str | None = None,
    use_semantic_scholar_evidence: bool | None = None,
):
    """Run the current identity resolver once and persist its own evidence."""
    from alma.api.scheduler import add_job_log
    from alma.application.author_identity import (
        persist_identity_result,
        resolve_identity_hierarchical,
    )

    before_openalex = _norm_oaid(str(row["openalex_id"] or "").strip()) or None
    before_scholar = str(row["scholar_id"] or "").strip() or None
    before_orcid = normalize_orcid(str(row["orcid"] or "")) or None

    _id_settings = _id_resolution_settings()
    semantic_scholar_enabled = (
        _id_settings["semantic_scholar_enabled"]
        if use_semantic_scholar_evidence is None
        else bool(use_semantic_scholar_evidence)
    )
    if job_id and not semantic_scholar_enabled:
        add_job_log(
            job_id,
            "Semantic Scholar identity evidence skipped: existing OpenAlex/ORCID identity is sufficient for this intent",
            step="identity_scope",
            data={"use_semantic_scholar": False},
        )
    hierarchical = resolve_identity_hierarchical(
        db,
        author_id=author_id,
        author_name=str(row["name"] or "").strip() or None,
        openalex_id=before_openalex,
        scholar_id=before_scholar,
        orcid=before_orcid,
        use_semantic_scholar=semantic_scholar_enabled,
        use_orcid=_id_settings["orcid_enabled"],
        use_preprints=semantic_scholar_enabled,
    )
    # Identity resolution (network) is done above — gate the two writes
    # (evidence row + reason) as one IMMEDIATE unit. persist_identity_result
    # uses commit_unless_gated, so it no-ops inside this section (we own the
    # commit) instead of committing mid-unit.
    with write_section(db, label="resolve_and_persist_author_identity"):
        persist_identity_result(db, author_id, hierarchical)
        db.execute(
            "UPDATE authors SET id_resolution_reason = ? WHERE id = ?",
            (_summarize_hierarchical_identity(hierarchical), author_id),
        )

    # What the row now holds, not what the resolver named: persistence keeps a stored
    # id, so a different one here would send the refresh to an identity no author
    # row owns, and the works outcome, which keys on the row, would match nothing.
    after_openalex = before_openalex or _norm_oaid(str(hierarchical.openalex_id or "").strip()) or None
    after_scholar = str(hierarchical.scholar_id or before_scholar or "").strip() or None
    after_orcid = normalize_orcid(str(hierarchical.orcid or before_orcid or "")) or None
    identity_changed = (
        before_openalex != after_openalex
        or before_scholar != after_scholar
        or before_orcid != after_orcid
    )

    if job_id:
        add_job_log(
            job_id,
            (
                f"Identity resolution: method={hierarchical.method}, "
                f"confidence={round(hierarchical.confidence, 3)}, "
                f"status={hierarchical.status}"
            ),
            step="identity_resolution",
            data=hierarchical.to_dict(),
        )

    return {
        "identity": hierarchical,
        "openalex_id": after_openalex,
        "identity_changed": identity_changed,
    }


def _apply_author_profile_from_openalex(
    db: sqlite3.Connection,
    author_id: str,
    *,
    openalex_id: str | None,
    author_name: str,
    job_id: str | None = None,
    profile_cache: dict | None = None,
    preloaded_profile: dict | None = None,
) -> dict[str, Any]:
    """Fetch or reuse one OpenAlex profile and persist it through the canonical writer."""
    from alma.api.scheduler import add_job_log
    from alma.application.author_profile import apply_author_profile_update

    profile = None
    if preloaded_profile is not None:
        profile = preloaded_profile
    elif isinstance(profile_cache, dict) and profile_cache and openalex_id:
        profile = profile_cache.get(_norm_oaid(openalex_id))
    elif openalex_id:
        from alma.openalex.client import fetch_author_profile

        profile = fetch_author_profile(openalex_id)

    # Persist through the canonical writer in ONE gated section (BEGIN IMMEDIATE),
    # with the network fetch above already done — the writer lock is never held
    # across OpenAlex I/O, and concurrent author refreshes serialize through the
    # gate instead of racing to "database is locked". Both callers (cache refresh
    # + identity/profile refresh) are ungated, so self-gating here is the one DRY
    # place this write obeys the SQLite write discipline.
    with write_section(db, label="author profile: apply"):
        profile_summary = apply_author_profile_update(db, author_id, profile)
    if job_id:
        add_job_log(
            job_id,
            f"Profile refresh for {author_name}: updated {', '.join(profile_summary.get('updated') or ['nothing'])}",
            step="profile_refresh",
            data=profile_summary,
        )
    return profile_summary


def _normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _parse_interests(raw: Any) -> list[str] | None:
    """Parse interests from JSON or legacy comma-separated storage."""
    if raw is None:
        return None
    if isinstance(raw, list):
        items = [str(x).strip() for x in raw if str(x).strip()]
        return items or None

    text = str(raw).strip()
    if not text:
        return None

    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        parsed = None

    if isinstance(parsed, list):
        items = [str(x).strip() for x in parsed if str(x).strip()]
        return items or None
    if isinstance(parsed, str):
        text = parsed.strip()
        if not text:
            return None
        return [text]
    if parsed is None:
        # Legacy rows may store interests as "topic a, topic b".
        parts = [part.strip() for part in text.split(",") if part.strip()]
        return parts or None
    return None


def _author_response_from_data(d: dict) -> AuthorResponse:
    return AuthorResponse(
        id=d["id"],
        name=d["name"],
        added_at=d.get("added_at"),
        publication_count=int(d.get("publication_count") or 0),
        affiliation=d.get("affiliation"),
        email_domain=d.get("email_domain"),
        citedby=d.get("citedby"),
        h_index=d.get("h_index"),
        interests=_parse_interests(d.get("interests")),
        url_picture=d.get("url_picture"),
        works_count=d.get("works_count"),
        last_fetched_at=d.get("last_fetched_at"),
        orcid=d.get("orcid"),
        openalex_id=d.get("openalex_id"),
        scholar_id=d.get("scholar_id"),
        author_type=d.get("author_type"),
        id_resolution_status=d.get("id_resolution_status"),
        id_resolution_reason=d.get("id_resolution_reason"),
        id_resolution_updated_at=d.get("id_resolution_updated_at"),
        id_resolution_method=d.get("id_resolution_method"),
        id_resolution_confidence=(
            float(d["id_resolution_confidence"])
            if d.get("id_resolution_confidence") is not None
            else None
        ),
        monitor_health=d.get("monitor_health"),
        monitor_health_reason=d.get("monitor_health_reason"),
        monitor_last_checked_at=d.get("monitor_last_checked_at"),
        monitor_last_success_at=d.get("monitor_last_success_at"),
        monitor_last_status=d.get("monitor_last_status"),
        monitor_last_error=d.get("monitor_last_error"),
        monitor_last_result=d.get("monitor_last_result"),
        monitor_papers_found=d.get("monitor_papers_found"),
        monitor_items_created=d.get("monitor_items_created"),
    )


def _normalize_person_name(name: str) -> str:
    return " ".join(str(name or "").strip().lower().split())


def _person_name_alignment(author_name: str, candidate_name: str) -> float:
    target = _normalize_person_name(author_name)
    candidate = _normalize_person_name(candidate_name)
    if not target or not candidate:
        return 0.0
    if target == candidate:
        return 1.0

    target_parts = target.split()
    candidate_parts = candidate.split()
    if not target_parts or not candidate_parts:
        return 0.0
    if target_parts[-1] != candidate_parts[-1]:
        return 0.0

    target_first = target_parts[0]
    candidate_first = candidate_parts[0]
    ratio = SequenceMatcher(a=target, b=candidate).ratio()
    if target_first == candidate_first:
        return max(0.9, ratio)
    if target_first[:1] and target_first[:1] == candidate_first[:1]:
        if len(target_first) == 1 or len(candidate_first) == 1:
            return max(0.82, min(0.92, ratio))
        shorter = min(len(target_first), len(candidate_first))
        if shorter >= 3 and (target_first.startswith(candidate_first) or candidate_first.startswith(target_first)):
            return max(0.84, min(0.94, ratio))
        return max(0.62, min(0.72, ratio))
    return 0.0


def _best_publication_author_match(
    db: sqlite3.Connection,
    *,
    paper_id: str,
    author_name: str,
) -> sqlite3.Row | None:
    try:
        rows = db.execute(
            """
            SELECT openalex_id, display_name, orcid
            FROM publication_authors
            WHERE paper_id = ?
            """,
            (paper_id,),
        ).fetchall()
    except sqlite3.OperationalError:
        return None
    if not rows:
        return None

    target = _normalize_person_name(author_name)
    if not target:
        return None

    best_row: sqlite3.Row | None = None
    best_score = 0.0
    for row in rows:
        candidate = _normalize_person_name(str(row["display_name"] or ""))
        if not candidate:
            continue
        if candidate == target:
            return row
        score = _person_name_alignment(target, candidate)
        if score > best_score:
            best_score = score
            best_row = row
    return best_row if best_score >= 0.82 else None


def _find_existing_author_row(
    db: sqlite3.Connection,
    *,
    author_id: str | None = None,
    openalex_id: str | None = None,
    scholar_id: str | None = None,
    orcid: str | None = None,
) -> sqlite3.Row | None:
    candidates: list[tuple[str, object]] = []
    if author_id:
        candidates.append(("SELECT * FROM authors WHERE id = ? LIMIT 1", author_id))
    if openalex_id:
        candidates.append(("SELECT * FROM authors WHERE lower(openalex_id) = lower(?) LIMIT 1", _norm_oaid(openalex_id)))
    if scholar_id:
        candidates.append(("SELECT * FROM authors WHERE scholar_id = ? LIMIT 1", scholar_id))
    if orcid:
        candidates.append(("SELECT * FROM authors WHERE lower(orcid) = lower(?) LIMIT 1", orcid))
    for query, value in candidates:
        row = db.execute(query, (value,)).fetchone()
        if row:
            return row
    return None


def _resolve_scholar_candidates_from_local(
    author_name: str,
    sample_titles: list[str],
    *,
    intent: ScholarIntent,
) -> list[dict]:
    """Best-effort Scholar candidate resolution from name + sample titles."""
    _sch = scholarly_client(intent)
    if _sch is None:
        return []

    name = (author_name or "").strip()
    if not name:
        return []

    out: list[dict] = []
    try:
        iterator = _sch.search_author(name)
        for idx, cand in enumerate(iterator):
            if idx >= 8:
                break
            c_name = (cand.get("name") or "").strip()
            c_aff = (cand.get("affiliation") or "").strip()
            c_id = (cand.get("scholar_id") or "").strip()
            if not c_id or not c_name:
                continue

            score = 0.0
            ratio = SequenceMatcher(None, _normalize_text(name), _normalize_text(c_name)).ratio()
            score += ratio * 6.0

            pub_titles = []
            for p in (cand.get("publications") or [])[:10]:
                bib = (p or {}).get("bib") or {}
                t = (bib.get("title") or "").strip()
                if t:
                    pub_titles.append(t)

            overlap = 0
            for t in sample_titles:
                nt = _normalize_text(t)
                if not nt:
                    continue
                for pt in pub_titles:
                    npt = _normalize_text(pt)
                    if not npt:
                        continue
                    if nt == npt or nt in npt or npt in nt:
                        overlap += 1
                        break
            score += overlap * 2.0

            out.append(
                {
                    "scholar_id": c_id,
                    "display_name": c_name,
                    "affiliation": c_aff,
                    "score": round(score, 3),
                    "title_overlap": overlap,
                    "source": "scholarly",
                    "scholar_url": scholar_url_for_id(c_id),
                }
            )
    except Exception:
        return []

    out.sort(key=lambda x: float(x.get("score") or 0.0), reverse=True)
    return out[:5]


def _resolve_scholar_candidates(
    author_name: str,
    sample_titles: list[str],
    *,
    openalex_id: str | None = None,
    orcid: str | None = None,
    mode: str = "auto",
) -> list[dict]:
    """Resolve Scholar candidates using configured providers.

    ``mode='auto'``: API sources first, optional scrape fallback only when enabled.
    ``mode='manual'``: include scrape results when manual scraping is enabled.
    """
    settings = _id_resolution_settings()
    from_api = resolve_scholar_candidates_from_sources(
        author_name,
        openalex_id=openalex_id,
        orcid=orcid,
        sample_titles=sample_titles,
        use_semantic_scholar=settings["semantic_scholar_enabled"],
        use_orcid=settings["orcid_enabled"],
    )

    from_local: list[dict] = []
    if mode in ("auto", "manual"):
        from_local = _resolve_scholar_candidates_from_local(
            author_name, sample_titles, intent=mode
        )

    return _merge_scholar_candidates(from_api, from_local)


def _pick_top_candidate(candidates: list[dict], min_score: float, min_margin: float) -> dict | None:
    if not candidates:
        return None
    top = candidates[0]
    top_score = float(top.get("score") or 0.0)
    second_score = float(candidates[1].get("score") or 0.0) if len(candidates) > 1 else -1.0
    if top_score < min_score:
        return None
    if len(candidates) > 1 and (top_score - second_score) < min_margin:
        return None
    return top


def _set_resolution_status(
    db: sqlite3.Connection,
    author_id: str,
    status_value: str,
    reason: str,
) -> None:
    status_norm = (status_value or "unresolved").strip().lower()
    if status_norm not in _RESOLUTION_STATUSES:
        status_norm = "unresolved"
    status_norm = sticky_resolution_status(db, author_id, status_norm)
    db.execute(
        """
        UPDATE authors
        SET id_resolution_status = ?, id_resolution_reason = ?, id_resolution_updated_at = ?
        WHERE id = ?
        """,
        (status_norm, reason[:1000], utcnow().isoformat(), author_id),
    )


def _refresh_author_cache_impl(
    db: sqlite3.Connection,
    author_id: str,
    *,
    mode: str,
    job_id: str | None = None,
    profile_cache: dict[str, dict[str, Any]] | None = None,
    is_batch_member: bool = False,
) -> dict:
    """Run incremental/deep refresh for one author.

    `profile_cache` is an optional mapping from normalized OpenAlex
    author ID → profile dict (same shape as `fetch_author_profile`).
    Bulk callers pre-fetch this via
    `openalex_client.batch_get_author_profiles` and pass it down so
    every per-author profile call collapses into one pipe-filter
    roundtrip per ~50 authors.

    `is_batch_member=True` flags that this call is one of N parallel
    workers under `_deep_refresh_all_impl`. In that mode the inner
    progress-forwarding ctx becomes a no-op for `set_job_status` —
    otherwise 4 workers all stomp on the same job row's
    processed/total/current_author/message fields, clobbering the
    aggregator's authoritative `X/Y · Z done · S skipped · F err`
    progress line. Add-only `add_job_log` entries (refresh_start,
    identity_resolution, refresh_route_openalex…) still fire so each
    author leaves an audit trail in `operation_logs`.
    """
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status

    if job_id and is_cancellation_requested(job_id):
        set_job_status(
            job_id,
            status="cancelled",
            finished_at=utcnow().isoformat(),
            message="Author refresh cancelled before execution",
        )
        add_job_log(job_id, "Cancelled before fetch start", step="cancelled")
        return {"success": False, "author_id": author_id, "mode": mode, "cancelled": True}

    author_id, row = _load_author_refresh_target(db, str(author_id or "").strip())
    author_name = row["name"]
    last_fetched = row["last_fetched_at"]

    identity_result = _resolve_and_persist_author_identity(db, author_id, row, job_id=job_id)
    openalex_id = identity_result["openalex_id"] or (str(row["openalex_id"] or "").strip() or None)
    followed_row = db.execute(
        "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1",
        (author_id,),
    ).fetchone()
    is_followed_author = followed_row is not None

    from_year = get_fetch_year()
    if mode == "deep" and is_followed_author:
        from_year = None
    elif mode == "incremental" and last_fetched:
        try:
            from_year = datetime.fromisoformat(last_fetched).year
        except Exception:
            pass

    if job_id:
        add_job_log(
            job_id,
            f"{mode} refresh started for {author_name} ({author_id}), from_year={from_year}",
            step="refresh_start",
        )

    # Phase C (2026-04-24): prefer the modern OpenAlex-backed backfill when
    # we have an openalex_id. It's collision-safe (canonical-triple dedup +
    # INSERT OR IGNORE + per-batch commits), fetches S2 SPECTER2 vectors, and
    # refreshes the author centroid as a side-effect. Google Scholar remains
    # the fallback for scholar-only authors without an openalex_id.
    pubs: list = []
    used_modern_backfill = False
    # Profile fetched inside `refresh_author_works_and_vectors` (Phase 1)
    # — reused below to avoid a second `fetch_author_profile` round-trip
    # per author on bulk deep refresh. None when modern backfill didn't
    # run or its profile fetch errored.
    backfill_profile: dict | None = None
    openalex_failure: Exception | None = None
    if openalex_id:
        try:
            from alma.api.deps import _db_path
            from alma.application.author_backfill import refresh_author_works_and_vectors

            class _DeepRefreshCtx:
                """Forwards backfill progress to operation_status via set_job_status.

                In batch mode (parallel deep_refresh_all), this becomes a
                no-op so concurrent workers don't clobber the aggregator's
                authoritative progress line.
                """

                def log_step(self, step, *, message=None, processed=None, total=None, **_):
                    if not job_id or is_batch_member:
                        return
                    try:
                        set_job_status(
                            job_id,
                            status="running",
                            message=message,
                            processed=processed,
                            total=total,
                        )
                    except Exception:
                        pass

            if job_id:
                add_job_log(
                    job_id,
                    f"Routing deep refresh for {author_name} through OpenAlex backfill",
                    step="refresh_route_openalex",
                    data={"openalex_id": openalex_id},
                )
            backfill_summary = refresh_author_works_and_vectors(
                db_path=_db_path(),
                author_openalex_id=openalex_id,
                ctx=_DeepRefreshCtx(),
                full_refetch=(mode == "deep"),
                profile_cache=profile_cache,
            )
            used_modern_backfill = True
            backfill_profile = backfill_summary.get("profile") if isinstance(backfill_summary, dict) else None
            if job_id:
                add_job_log(
                    job_id,
                    (
                        f"OpenAlex backfill complete for {author_name}: "
                        f"{backfill_summary.get('works_fetched', 0)} works fetched, "
                        f"{backfill_summary.get('papers_new', 0)} new / "
                        f"{backfill_summary.get('papers_updated', 0)} updated, "
                        f"{backfill_summary.get('vectors_fetched', 0)} vectors"
                    ),
                    step="refresh_openalex_done",
                    data=backfill_summary,
                )
        except Exception as exc:
            openalex_failure = exc
            logger.warning(
                "OpenAlex backfill failed for %s (%s): %s — falling back to Scholar",
                author_name,
                openalex_id,
                exc,
            )
            if job_id:
                add_job_log(
                    job_id,
                    f"OpenAlex backfill failed for {author_name}: {exc} — falling back to Scholar",
                    level="WARNING",
                    step="refresh_openalex_fallback",
                )

    if not used_modern_backfill:
        scholar_lookup_id = str(row["scholar_id"] or "").strip()
        if scholar_lookup_id:
            try:
                pubs = fetch_publications_by_id(
                    scholar_lookup_id,
                    output_folder=_data_dir(),
                    args=SimpleNamespace(update_cache=True, test_fetching=False),
                    from_year=from_year,
                )
            except Exception as exc:
                logger.warning(
                    "Scholar fallback failed for %s (%s): %s",
                    author_name,
                    scholar_lookup_id,
                    exc,
                )
                if job_id:
                    add_job_log(
                        job_id,
                        f"Scholar fallback warning for {author_name}: {exc}",
                        level="WARNING",
                        step="refresh_scholar_fallback",
                    )
                pubs = []
        else:
            if job_id:
                add_job_log(
                    job_id,
                    f"Skipped Scholar fallback for {author_name}: no Scholar ID on author row",
                    level="WARNING",
                    step="refresh_scholar_fallback",
                )
            pubs = []

    # No source delivered, so this is a failed refresh: the caller's job must end
    # failed, and `last_fetched_at` must not claim a fetch that never happened.
    if openalex_failure is not None and not pubs:
        raise openalex_failure

    now = utcnow().isoformat()
    with write_section(db, label="author refresh: last_fetched_at"):
        db.execute("UPDATE authors SET last_fetched_at = ? WHERE id = ?", (now, author_id))

    # Consolidated author-profile refresh (2026-04-24): writes every
    # field OpenAlex returns — canonical display_name, affiliation,
    # citations, h-index, works_count, interests/topics,
    # institution history, ORCID, cited-by-year, thumbnail — via the
    # single `apply_author_profile_update` helper so string and numeric
    # fields get consistent overwrite semantics (COALESCE for strings,
    # MAX for monotonic counters). Also refreshes the author's SPECTER2
    # centroid unconditionally, so Scholar-only authors benefit from any
    # embeddings we have while modern-backfill authors get a second-pass
    # update after their works landed.
    from alma.application.author_profile import refresh_author_centroid_safe

    if openalex_id:
        try:
            _apply_author_profile_from_openalex(
                db,
                author_id,
                openalex_id=openalex_id,
                author_name=str(author_name or "").strip() or author_id,
                job_id=job_id,
                profile_cache=profile_cache,
                preloaded_profile=backfill_profile,
            )
        except Exception as e:
            logger.warning("Failed to refresh author profile for %s: %s", author_id, e)
            if job_id:
                add_job_log(
                    job_id,
                    f"Profile refresh warning for {author_name}: {e}",
                    level="WARNING",
                    step="profile_refresh",
                )

    with write_section(db, label="author refresh: centroid"):
        centroid_refreshed = refresh_author_centroid_safe(db, openalex_id)
    if job_id:
        add_job_log(
            job_id,
            (
                f"Centroid recompute for {author_name}: "
                f"{'updated' if centroid_refreshed else 'no embeddings yet / skipped'}"
            ),
            step="centroid_refresh",
            data={"openalex_id": openalex_id, "updated": bool(centroid_refreshed)},
        )

    total_count = authors_app.get_author_publication_count(
        db,
        author_id=author_id,
        author_name=str(author_name or "").strip(),
        openalex_id=str(openalex_id or "").strip(),
    )
    new_count = len(pubs or [])
    if job_id:
        add_job_log(
            job_id,
            f"{mode} refresh done for {author_name}: new={new_count}, total={total_count}",
            step="refresh_done",
        )

    return {
        "success": True,
        "author_id": author_id,
        "count": total_count,
        "new_count": new_count,
        "mode": mode,
        # Terminal Activity-row message — the scheduler wrapper falls
        # back to a bland default if absent; explicit summary keeps the
        # row legible after completion (lesson § "Terminal-state message
        # must not leak from in-progress logs").
        "message": (
            f"Deep refresh done for {author_name}: "
            f"{new_count} new / {total_count} total"
            if mode == "deep"
            else f"Refreshed {author_name}: {new_count} new / {total_count} total"
        ),
    }


def _refresh_author_identity_profile_impl(
    db: sqlite3.Connection,
    author_id: str,
    *,
    job_id: str | None = None,
) -> dict:
    """Refresh author identity/profile only; no works, vectors, or broad rebuild."""
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status
    from alma.application.author_profile import refresh_author_centroid_safe

    if job_id and is_cancellation_requested(job_id):
        set_job_status(
            job_id,
            status="cancelled",
            finished_at=utcnow().isoformat(),
            message="Author identity/profile refresh cancelled before execution",
        )
        add_job_log(job_id, "Cancelled before identity/profile refresh start", step="cancelled")
        return {"success": False, "author_id": author_id, "mode": "identity_profile", "cancelled": True}

    author_id, row = _load_author_refresh_target(db, str(author_id or "").strip())
    author_name = str(row["name"] or "").strip() or author_id

    if job_id:
        add_job_log(
            job_id,
            (
                f"Identity/profile refresh started for {author_name} ({author_id}); "
                "works, S2 vector fetch, and full centroid rebuild are skipped"
            ),
            step="identity_profile_start",
            data={"intent": "identity_profile", "skips": ["works", "s2_vectors", "centroid_full_rebuild"]},
        )

    has_stable_identity = bool(
        _norm_oaid(str(row["openalex_id"] or "").strip())
        and normalize_orcid(str(row["orcid"] or ""))
    )
    identity_result = _resolve_and_persist_author_identity(
        db,
        author_id,
        row,
        job_id=job_id,
        use_semantic_scholar_evidence=not has_stable_identity,
    )
    openalex_id = identity_result["openalex_id"] or (str(row["openalex_id"] or "").strip() or None)

    if openalex_id:
        try:
            _apply_author_profile_from_openalex(
                db,
                author_id,
                openalex_id=openalex_id,
                author_name=author_name,
                job_id=job_id,
            )
        except Exception as exc:
            logger.warning("Identity/profile OpenAlex profile update failed for %s: %s", author_id, exc)
            if job_id:
                add_job_log(
                    job_id,
                    f"Profile refresh warning for {author_name}: {exc}",
                    level="WARNING",
                    step="profile_refresh",
                )
    elif job_id:
        add_job_log(
            job_id,
            f"Skipped profile refresh for {author_name}: no OpenAlex ID after identity resolution",
            level="WARNING",
            step="profile_refresh",
        )

    centroid_refreshed = False
    if identity_result["identity_changed"] and openalex_id:
        with write_section(db, label="author identity refresh: centroid"):
            centroid_refreshed = refresh_author_centroid_safe(db, openalex_id)
        if job_id:
            add_job_log(
                job_id,
                (
                    f"Centroid recompute for {author_name}: "
                    f"{'updated' if centroid_refreshed else 'no embeddings yet / skipped'}"
                ),
                step="centroid_refresh",
                data={"openalex_id": openalex_id, "updated": bool(centroid_refreshed), "reason": "identity_changed"},
            )
    elif job_id:
        add_job_log(
            job_id,
            f"Centroid recompute skipped for {author_name}: identity unchanged",
            step="centroid_refresh_skipped",
            data={"openalex_id": openalex_id, "reason": "identity_unchanged"},
        )

    # No commit needed: the identity persist (_resolve_and_persist_author_identity)
    # and the centroid refresh above each run in their own write_section.

    total_count = authors_app.get_author_publication_count(
        db,
        author_id=author_id,
        author_name=author_name,
        openalex_id=str(openalex_id or "").strip(),
    )
    if job_id:
        add_job_log(
            job_id,
            f"Identity/profile refresh done for {author_name}: total={total_count}",
            step="identity_profile_done",
        )

    return {
        "success": True,
        "author_id": author_id,
        "count": total_count,
        "new_count": 0,
        "mode": "identity_profile",
        "centroid_refreshed": bool(centroid_refreshed),
        "message": f"Identity/profile refresh done for {author_name}: {total_count} total",
    }


def _deep_refresh_all_impl(
    db: sqlite3.Connection,
    *,
    job_id: str | None = None,
    scope: str = "corpus",
    limit: int,
) -> dict:
    """Run full deep refresh for all refreshable authors.

    DRY contract (2026-04-24): the per-author iteration funnels through
    `_refresh_author_cache_impl` — the **exact** pipeline the popup's
    "Refresh author" button uses. That runs (1) hierarchical identity
    resolution + persistence of method/confidence/evidence, (2) works +
    SPECTER2 vectors backfill via `refresh_author_works_and_vectors`
    (modern path) with Scholar fallback, (3) profile refresh via
    `apply_author_profile_update` (name, affiliation, institutions,
    citedby, h_index, works_count, interests, cited_by_year, orcid),
    (4) author centroid recompute.

    Scope (`library` / `followed` / `corpus`) narrows the author pool so
    the user can keep heavy full-corpus runs separate from fast
    library-only sweeps.

    Concurrency (2026-04-25): authors are processed in parallel via a
    `ThreadPoolExecutor` with `_DEEP_REFRESH_MAX_WORKERS` workers. Each
    worker opens its own SQLite connection (WAL-friendly) and the
    OpenAlex `requests.Session` is thread-safe, so the wall-clock win
    is roughly Nx workers minus rate-limit / centroid-recompute
    serialisation. The caller's `db` connection is only used for the
    initial author roll-up read.

    Per-unit contract (lesson "Bulk background jobs must commit per unit
    of work"): every worker opens/closes its own transactions, the
    aggregator emits `set_job_status(processed, total, current_author)`
    under a lock + a digest `add_job_log` every 25 completions, and
    per-author failures are absorbed so a single bad OpenAlex fetch
    doesn't abort the batch.
    """
    from alma.api.scheduler import add_job_log, is_cancellation_requested, set_job_status

    # Author-pool selection lives in author_lifecycle so the Health maintenance
    # ``refresh_authors`` count/ETA shares exactly this definition (DRY — one
    # source of truth for "which authors does scope X touch").
    from alma.application.author_lifecycle import select_authors_for_scope

    scope = (scope or "followed").strip().lower()
    rows = select_authors_for_scope(db, scope)
    eligible = len(rows)
    # One total budget (REQUIRED, explicit): bound the author pool to the run's
    # cap — the unit is one author. The maintenance tick passes the registry cap;
    # the admin route passes its full-sweep budget. Scope picks the pool, the cap
    # bounds how many of it this run touches. There is no unbounded mode.
    cap = max(1, int(limit))
    if len(rows) > cap:
        rows = rows[:cap]
    total = len(rows)
    if total == 0:
        if job_id:
            add_job_log(job_id, f"No authors found for deep refresh (scope={scope})", step="preflight")
        return {"success": True, "total": 0, "refreshed": 0, "skipped": 0, "failed": 0, "failures": [], "scope": scope, "eligible": eligible}

    refreshed = 0
    skipped = 0
    failed = 0
    failures: list[dict] = []
    processed = 0

    if job_id:
        bounded = f" (capped from {eligible} eligible)" if eligible > total else ""
        add_job_log(
            job_id,
            f"Deep refresh all started for {total} authors (scope={scope}){bounded}",
            step="preflight",
        )
        set_job_status(
            job_id,
            status="running",
            processed=0,
            total=total,
            message=f"Deep refreshing {total} authors ({scope})",
        )

    # Release any pending writes on the caller's connection before we
    # spawn workers — each worker opens its own connection, but if the
    # caller is mid-transaction here we'd hold a writer lock that
    # serialises every worker behind us. commit_with_retry flushes with
    # transient-lock retry instead of a raw, un-retried commit.
    commit_with_retry(db, label="deep_refresh_all preflight flush")

    # Pre-classify rows. Only truly malformed rows (empty id) are
    # skipped up-front; import-only placeholders without upstream IDs
    # are *kept* so the inner identity-resolution cascade gets a
    # chance to find their OpenAlex ID by searching by name across the
    # papers we already have for them (`resolve_identity_hierarchical`
    # → `resolve_openalex_candidates_from_metadata`, which queries
    # OpenAlex works by title and inspects each work's `authorships`
    # to match the placeholder's display name).
    work_items: list[tuple[str, str, str | None]] = []
    for row in rows:
        author_id = (row["id"] or "").strip()
        author_name = row["name"] or author_id
        author_oid = (row["openalex_id"] or "").strip() or None
        if not author_id:
            skipped += 1
            processed += 1
            if job_id:
                add_job_log(job_id, "Skipped author row with empty id", level="WARNING", step="skip")
            continue
        work_items.append((author_id, author_name, author_oid))

    # Workers that started without an upstream ID are tracked here so
    # we can report how many got resolved this run. Set comprehension
    # is fine — `author_id` is unique per row.
    needs_upstream = {aid for aid, _, oid in work_items if not oid}
    resolved_upstream = 0

    # Pre-flight: pipe-filter every refreshable author's profile into
    # one cache so each worker skips its own per-author profile fetch.
    # Cuts 1 OpenAlex roundtrip per author down to ~1 batched call per
    # 50 authors. Failure is non-fatal — workers fall back to per-author
    # fetches via the existing path.
    profile_cache: dict[str, dict[str, Any]] = {}
    cache_oids = [oid for _, _, oid in work_items if oid]
    if cache_oids:
        try:
            from alma.openalex.client import batch_get_author_profiles

            t_pre = utcnow()
            profile_cache = batch_get_author_profiles(cache_oids, batch_size=50, max_workers=4)
            if job_id:
                add_job_log(
                    job_id,
                    (
                        f"Pre-fetched {len(profile_cache)}/{len(cache_oids)} author profiles "
                        f"in batched pipe-filter calls "
                        f"({(utcnow() - t_pre).total_seconds():.1f}s)"
                    ),
                    step="profile_prefetch",
                )
        except Exception as exc:
            logger.warning("Profile pre-fetch failed (workers will fall back per-author): %s", exc)
            if job_id:
                add_job_log(
                    job_id,
                    f"Profile pre-fetch failed: {exc} — workers will fetch per-author",
                    level="WARNING",
                    step="profile_prefetch",
                )
            profile_cache = {}

    progress_lock = threading.Lock()
    cancelled = False

    def _run_one_author(author_id: str, author_name: str) -> tuple[str, str, str, Exception | None, bool]:
        """Worker body: open a private DB connection and refresh one author.

        Each worker thread holds its own SQLite connection so the per-author
        commits inside `_refresh_author_cache_impl` don't stall behind the
        caller's connection. Returns a (status, author_id, author_name,
        exception?, gained_upstream) tuple for the aggregator on the main
        thread. `gained_upstream` is True when the author started without
        an OpenAlex ID and identity resolution found one.
        """
        nonlocal cancelled
        # Cheap pre-flight cancel check — once a cancel is requested we
        # let already-queued workers no-op out so the pool drains fast.
        if cancelled or (job_id and is_cancellation_requested(job_id)):
            cancelled = True
            return ("cancelled", author_id, author_name, None, False)
        worker_db = open_db_connection()
        try:
            # DRY: same pipeline as the popup "Refresh author" button —
            # hierarchical identity resolve → works backfill (OpenAlex or
            # Scholar fallback) → profile update → centroid recompute.
            # Runs on the worker's own connection;
            # `profile_cache` lets backfill skip its own profile fetch.
            # `is_batch_member=True` mutes the inner ctx so workers
            # don't clobber the aggregator's job-row progress fields.
            _refresh_author_cache_impl(
                worker_db,
                author_id,
                mode="deep",
                job_id=job_id,
                profile_cache=profile_cache,
                is_batch_member=True,
            )
            # Did identity resolution land an OpenAlex ID where there
            # was none before? Re-read the row on the worker's
            # connection — `_refresh_author_cache_impl` commits the
            # resolved ID before backfill runs, so this read is post-
            # commit and never holds a writer lock.
            gained = False
            if author_id in needs_upstream:
                row = worker_db.execute(
                    "SELECT openalex_id FROM authors WHERE id = ?", (author_id,),
                ).fetchone()
                gained = bool(row and (row["openalex_id"] or "").strip())
            return ("ok", author_id, author_name, None, gained)
        except Exception as exc:
            try:
                if worker_db.in_transaction:
                    worker_db.rollback()
            except Exception:
                pass
            return ("fail", author_id, author_name, exc, False)
        finally:
            try:
                worker_db.close()
            except Exception:
                pass

    with bounded_thread_pool(
        _DEEP_REFRESH_MAX_WORKERS,
        thread_name_prefix="deep-refresh",
    ) as executor:
        futures = {
            executor.submit(_run_one_author, aid, an): (aid, an)
            for aid, an, _ in work_items
        }
        for fut in as_completed(futures):
            aid, an = futures[fut]
            if fut.cancelled():
                status, exc, gained = "cancelled", None, False
            else:
                status, aid, an, exc, gained = fut.result()
            with progress_lock:
                cancel_requested = bool(job_id and is_cancellation_requested(job_id))
                if cancel_requested:
                    cancelled = True
                    for pending in futures:
                        if not pending.done():
                            pending.cancel()
                processed += 1
                if status == "ok":
                    refreshed += 1
                    if gained:
                        resolved_upstream += 1
                        if job_id:
                            add_job_log(
                                job_id,
                                f"Resolved upstream OpenAlex ID for {an} via paper authorships",
                                step="upstream_resolved",
                                data={"author_id": aid, "name": an},
                            )
                elif status == "fail":
                    failed += 1
                    failure = {"author_id": aid, "name": an, "error": str(exc)}
                    failures.append(failure)
                    logger.warning("Deep refresh failed for %s (%s): %s", an, aid, exc)
                    if job_id:
                        add_job_log(
                            job_id,
                            f"Failed author {an} ({aid}): {exc}",
                            level="ERROR",
                            step="author_error",
                            data=failure,
                        )
                elif status == "cancelled":
                    # Worker bailed because cancellation was requested.
                    # We still tick `processed` so the progress bar
                    # finishes counting — and surface a cancelled-summary
                    # below.
                    pass

                if job_id and status != "cancelled" and not cancelled:
                    set_job_status(
                        job_id,
                        status="running",
                        processed=processed,
                        total=total,
                        current_author=an,
                        message=(
                            f"{processed}/{total} · {refreshed} done · "
                            f"{resolved_upstream} resolved · "
                            f"{skipped} skipped · {failed} err"
                        ),
                    )
                if job_id and (processed % 25 == 0 or processed == total):
                    add_job_log(
                        job_id,
                        (
                            f"Progress {processed}/{total} · "
                            f"{refreshed} refreshed · "
                            f"{resolved_upstream} upstream IDs resolved · "
                            f"{skipped} skipped · {failed} failed"
                        ),
                        step="progress",
                    )

    if cancelled:
        cancel_summary = {
            "success": False,
            "total": total,
            "refreshed": refreshed,
            "resolved_upstream": resolved_upstream,
            "skipped": skipped,
            "failed": failed,
            "cancelled": True,
            "processed": processed,
            "failures": failures[:25],
            "scope": scope,
        }
        if job_id:
            set_job_status(
                job_id,
                status="cancelled",
                finished_at=utcnow().isoformat(),
                processed=processed,
                total=total,
                message="Deep refresh all cancelled by user",
                result=cancel_summary,
            )
            add_job_log(
                job_id,
                "Cancellation requested; pool drained",
                step="cancelled",
                data=cancel_summary,
            )
        return cancel_summary

    # Final flush of the orchestrator connection (workers committed on their own).
    commit_with_retry(db, label="deep_refresh_all final flush")
    summary = {
        "success": failed == 0,
        "total": total,
        "refreshed": refreshed,
        "resolved_upstream": resolved_upstream,
        "skipped": skipped,
        "failed": failed,
        "failures": failures[:25],
        "scope": scope,
        # Terminal-state message — see scheduler `_wrapped` contract.
        "message": (
            f"Deep refresh all ({scope}): "
            f"{refreshed} refreshed · "
            f"{resolved_upstream} upstream IDs resolved · "
            f"{skipped} skipped · {failed} failed "
            f"({total} total)"
        ),
        "batch": batch_outcome(attempted=refreshed + failed, failed=failed, noun="authors"),
    }
    if job_id:
        add_job_log(
            job_id,
            (
                f"Deep refresh all finished (refreshed={refreshed}, "
                f"resolved_upstream={resolved_upstream}, skipped={skipped}, "
                f"failed={failed}, scope={scope})"
            ),
            step="done",
            data=summary,
        )
    return summary

@router.get(
    "",
    response_model=list[AuthorResponse],
    summary="List all authors",
    description="Retrieve a list of all monitored authors with their metadata.",
)
def list_authors(
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """List all monitored authors.

    Returns:
        List[AuthorResponse]: List of all authors with publication counts

    Example:
        ```bash
        curl http://localhost:8000/api/v1/authors
        ```
    """
    try:
        # ALMa is single-user; the Authors page renders the full corpus
        # (Suggested rail + Followed grid + Corpus table). A default 100-row
        # cap silently truncated the list and in particular dropped followed
        # authors past the first 100 alphabetically — the symptom the user
        # saw as "only 3 followed authors". Fetch the whole table.
        rows, _total = authors_app.list_authors(db, limit=1_000_000)
        result = []
        for d in rows:
            result.append(_author_response_from_data(d))
        logger.info("Retrieved %d authors", len(result))
        return result

    except Exception as e:
        raise_internal("Failed to retrieve authors", e)


@router.get(
    "/lookup",
    response_model=AuthorResponse,
    summary="Look up an author by display name",
    description=(
        "Return a compact author record matched by normalized display "
        "name. Intended for lightweight UI previews (hover cards on paper "
        "bylines). Returns 404 when no corpus row matches the name."
    ),
)
def lookup_author(
    name: str = Query(..., min_length=1, description="Author display name"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        data = authors_app.lookup_author_by_name(db, name)
        if not data:
            raise HTTPException(status_code=404, detail="Author not found")
        return _author_response_from_data(data)
    except HTTPException:
        raise
    except Exception as e:
        raise_internal("Failed to look up author", e)


@router.get(
    "/suggestions",
    response_model=list[AuthorSuggestionResponse],
    summary="List collaborator and adjacent author suggestions",
)
def list_author_suggestions(
    limit: int = Query(default=5, ge=1, le=30),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        suggestions = authors_app.list_author_suggestions(db, limit=limit)
        return [AuthorSuggestionResponse(**item) for item in suggestions]
    except Exception as e:
        raise_internal("Failed to build author suggestions", e)


class RejectSuggestionRequest(BaseModel):
    openalex_id: str = Field(..., min_length=1, description="OpenAlex author ID of the suggestion to reject")
    suggestion_bucket: str | None = Field(
        default=None,
        description=(
            "Originating rail bucket (library_core / cited_by_high_signal / "
            "adjacent / semantic_similar / openalex_related / s2_related). "
            "Optional — used by outcome calibration to reweight per-bucket "
            "scoring; NULL is fine for non-rail rejections."
        ),
    )


class MergeSuggestionRequest(BaseModel):
    openalex_id: str
    primary_author_id: str


@router.post(
    "/suggestions/merge-into",
    summary="Import a duplicate suggestion into the followed author it matches",
    description=(
        "For a suggestion flagged `duplicate_of` an existing followed author: fold "
        "the suggested external OpenAlex profile into that author (reassign its "
        "papers, record its id as an alias) instead of following it as a new "
        "person. Foreground write via run_write_unit."
    ),
)
def merge_suggestion_into_endpoint(
    req: MergeSuggestionRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import import_suggested_author
    from alma.core.db_write import run_write_unit

    result = run_write_unit(
        db,
        lambda: import_suggested_author(db, req.primary_author_id, req.openalex_id),
        label="import_suggested_author",
    )
    if not result.get("success"):
        raise HTTPException(status_code=404, detail=f"Merge failed: {result.get('reason')}")
    return result


class NotDuplicateRequest(BaseModel):
    openalex_id: str


@router.post(
    "/suggestions/not-duplicate",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Mark a flagged duplicate suggestion as NOT a duplicate",
    description=(
        "The user says a suggestion flagged `duplicate_of` is actually a different "
        "person. Records the OpenAlex id so it's no longer flagged — it returns to "
        "the normal 'follow a new author' rail instead of being suppressed. "
        "Foreground write via run_write_unit."
    ),
)
def mark_suggestion_not_duplicate_endpoint(
    req: NotDuplicateRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.core.db_write import run_write_unit

    oid = (req.openalex_id or "").strip().lower()
    if not oid:
        raise HTTPException(status_code=400, detail="openalex_id required")

    def _persist() -> None:
        db.execute(
            "INSERT OR IGNORE INTO author_suggestion_not_duplicate (openalex_id, created_at) "
            "VALUES (?, datetime('now'))",
            (oid,),
        )

    run_write_unit(db, _persist, label="suggestion_not_duplicate")


@router.post(
    "/suggestions/reject",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Reject an author suggestion",
    description="Records a negative signal for this OpenAlex author so they are suppressed from future /authors/suggestions responses.",
)
def reject_author_suggestion(
    req: RejectSuggestionRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.gap_radar import record_missing_author_remove
    from alma.core.db_write import run_write_unit

    def _persist() -> None:
        # Serialized write unit (writer gate + BEGIN IMMEDIATE + lock retry);
        # a retry re-issues the INSERT on a clean transaction (no dup row).
        record_missing_author_remove(
            db, req.openalex_id, suggestion_bucket=req.suggestion_bucket
        )

    try:
        run_write_unit(db, _persist, label="reject_author_suggestion")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as e:
        raise_internal("Failed to reject author suggestion", e)


class TrackFollowSuggestionRequest(BaseModel):
    openalex_id: str = Field(..., min_length=1, description="OpenAlex author ID of the followed suggestion")
    suggestion_bucket: str | None = Field(
        default=None,
        description="Originating rail bucket label (see RejectSuggestionRequest).",
    )


@router.post(
    "/suggestions/track-follow",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Log a rail-originated author follow for outcome calibration",
    description=(
        "Fire-and-forget call from the Suggested Authors rail after a "
        "follow succeeds, so per-bucket outcome calibration can attribute "
        "the positive event to the bucket that surfaced the author. The "
        "actual follow write goes through the existing follow / author-create "
        "routes — this is the calibration log only."
    ),
)
def track_followed_suggestion(
    req: TrackFollowSuggestionRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.gap_radar import record_followed_from_suggestion
    from alma.core.db_write import run_write_unit

    def _persist() -> None:
        record_followed_from_suggestion(db, req.openalex_id, req.suggestion_bucket)

    try:
        run_write_unit(db, _persist, label="track_followed_suggestion")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as e:
        raise_internal("Failed to log followed suggestion", e)


# ----------------------------------------------------------------------
# D12 Phase C — refresh the two network-backed suggestion buckets
# ----------------------------------------------------------------------

@router.post(
    "/follow-from-paper",
    response_model=AuthorFollowFromPaperResponse,
    summary="Follow an author directly from a paper card",
)
def follow_author_from_paper(
    payload: AuthorFollowFromPaperRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    try:
        from alma.openalex.client import upsert_work_sidecars

        # Contract canonicalization is NOT done here (43.1): it ran ungated on
        # this POST, before any run_write_unit. The follow write below goes
        # through apply_follow_state, which ensures the contract inside its
        # gated unit — so the heal happens correctly, once, in-transaction.
        paper_row = db.execute(
            """
            SELECT id, title, authors, abstract, doi, openalex_id, year, publication_date, journal, url
            FROM papers
            WHERE id = ?
            """,
            (payload.paper_id,),
        ).fetchone()
        if not paper_row:
            raise HTTPException(status_code=404, detail="Paper not found")

        requested_author_name = str(payload.author_name or "").strip()
        if not requested_author_name:
            raise HTTPException(status_code=422, detail="author_name cannot be empty")

        # ---- Phase 1: resolve the paper's author identity --------------
        # All network resolution happens OUTSIDE the write units; each write
        # window is its own gated `run_write_unit`, so the writer is never
        # held across OpenAlex I/O (write-discipline rule #2). Previously the
        # paper UPDATE below opened an implicit transaction that stayed open
        # across `resolve_author_identity` — exactly the held-across-network
        # hazard this split removes.
        authorship = _best_publication_author_match(
            db,
            paper_id=payload.paper_id,
            author_name=requested_author_name,
        )
        matched_via = "paper_authorship" if authorship else "resolution"
        if authorship is None:
            # NETWORK: resolve the paper's OpenAlex work (no DB writes here).
            paper_resolution = resolve_paper_openalex_work(
                {
                    "id": str(paper_row["id"] or "").strip(),
                    "title": str(paper_row["title"] or "").strip(),
                    "authors": str(paper_row["authors"] or "").strip(),
                    "abstract": str(paper_row["abstract"] or "").strip(),
                    "doi": str(paper_row["doi"] or "").strip(),
                    "openalex_id": str(paper_row["openalex_id"] or "").strip(),
                    "year": paper_row["year"],
                    "publication_date": str(paper_row["publication_date"] or "").strip(),
                    "journal": str(paper_row["journal"] or "").strip(),
                    "url": str(paper_row["url"] or "").strip(),
                }
            )
            resolved_work = paper_resolution.work or {}
            if resolved_work:
                # Write unit 1: persist the paper enrichment + sidecars
                # atomically (fill-only). Idempotent — COALESCE/NULLIF + an
                # upsert — so a lock retry re-runs cleanly.
                def _persist_paper() -> None:
                    db.execute(
                        """
                        UPDATE papers
                        SET openalex_id = COALESCE(NULLIF(openalex_id, ''), ?),
                            doi = COALESCE(NULLIF(doi, ''), ?),
                            abstract = COALESCE(NULLIF(abstract, ''), ?),
                            year = COALESCE(year, ?),
                            publication_date = COALESCE(NULLIF(publication_date, ''), ?),
                            journal = COALESCE(NULLIF(journal, ''), ?),
                            authors = COALESCE(NULLIF(authors, ''), ?)
                        WHERE id = ?
                        """,
                        (
                            str(resolved_work.get("openalex_id") or "").strip() or None,
                            str(resolved_work.get("doi") or "").strip() or None,
                            str(resolved_work.get("abstract") or "").strip() or None,
                            resolved_work.get("year"),
                            str(resolved_work.get("publication_date") or "").strip() or None,
                            str(resolved_work.get("journal") or "").strip() or None,
                            str(resolved_work.get("authors") or "").strip() or None,
                            payload.paper_id,
                        ),
                    )
                    upsert_work_sidecars(
                        db,
                        payload.paper_id,
                        topics=resolved_work.get("topics"),
                        institutions=resolved_work.get("institutions"),
                        authorships=resolved_work.get("authorships"),
                        referenced_works=resolved_work.get("referenced_works"),
                    )

                run_write_unit(db, _persist_paper, label="follow_from_paper:paper_enrich")
                # Re-match now that the sidecar authorships are committed (read).
                authorship = _best_publication_author_match(
                    db,
                    paper_id=payload.paper_id,
                    author_name=requested_author_name,
                )
                if authorship is not None:
                    matched_via = "paper_openalex_work"

        matched_name = str((authorship["display_name"] if authorship else requested_author_name) or requested_author_name).strip()
        matched_openalex = str((authorship["openalex_id"] if authorship else "") or "").strip() or None
        matched_orcid = str((authorship["orcid"] if authorship else "") or "").strip() or None

        # Identity. When the paper's authorship already gives us the author's
        # OpenAlex id (the common case — it is the canonical author id in ALMa),
        # follow on THAT id immediately and DEFER the hierarchical identity
        # enrichment (S2 / ORCID / Scholar) to the background hydration sweep
        # scheduled after the commit below. resolve_author_identity is a
        # synchronous, multi-source, rate-limit-prone network walk — observed at
        # ~86 s on the request path when OpenAlex/S2 were saturated by a
        # maintenance burst — and the follow does not need it to be correct: the
        # OpenAlex id alone identifies the author and dedups existing rows. We
        # only resolve synchronously when the paper yields no OpenAlex id at all
        # (rare: the author isn't in the resolved work's authorships).
        if matched_openalex:
            resolution = None
            resolved_openalex = _norm_oaid(matched_openalex)
            resolved_scholar = None
            resolved_orcid = normalize_orcid(matched_orcid) if matched_orcid else None
            primary_id = resolved_openalex
            status_value = "resolved_manual"
            reason_value = "Author followed from paper (identity enrichment deferred to background)"
        else:
            # NETWORK (rare): no authorship id — resolve a stable identity now,
            # because we have nothing but a display name to follow on.
            settings = _id_resolution_settings()
            resolution = resolve_author_identity(
                db,
                author_name=matched_name or requested_author_name,
                openalex_id=matched_openalex,
                orcid=matched_orcid,
                sample_titles=[str(paper_row["title"] or "").strip()],
                use_semantic_scholar=settings["semantic_scholar_enabled"],
                use_orcid=settings["orcid_enabled"],
                use_scholar_scrape_auto=settings["scholar_scrape_auto_enabled"],
            )
            resolved_openalex = resolution.openalex_id or (_norm_oaid(matched_openalex) if matched_openalex else None)
            resolved_scholar = resolution.scholar_id or None
            resolved_orcid = resolution.orcid or matched_orcid
            primary_id = resolved_scholar or resolved_openalex or resolved_orcid
            status_value = resolution.status if resolution.status != "no_match" else "resolved_manual"
            reason_value = summarize_author_resolution(resolution) or "Author followed from paper"
        if not primary_id:
            raise HTTPException(
                status_code=422,
                detail="Could not resolve a stable author identity from this paper author",
            )

        now_iso = utcnow().isoformat()
        existing = _find_existing_author_row(
            db,
            author_id=primary_id,
            openalex_id=resolved_openalex,
            scholar_id=resolved_scholar,
            orcid=resolved_orcid,
        )
        created = existing is None
        author_id = (str(existing["id"] or "").strip() or primary_id) if existing else primary_id

        # ---- Phase 2: persist the follow (one atomic write unit) -------
        # Upsert the author row, snapshot the prior follow state, then sync
        # the three follow tables and clear any gap-radar negative signal —
        # all in one transaction. Returns (was_followed, needs_sweep) so the
        # post-commit scheduling below runs with the gate released.
        def _persist_follow() -> tuple[bool, bool]:
            if existing:
                # resolution is None on the fast path (identity came straight from
                # the paper authorship) — the background sweep enriches instead.
                if resolution is not None:
                    _apply_author_resolution_result(db, author_id, resolution)
                db.execute(
                    """
                    UPDATE authors
                    SET name = COALESCE(NULLIF(name, ''), ?),
                        id_resolution_status = COALESCE(NULLIF(id_resolution_status, ''), ?),
                        id_resolution_reason = COALESCE(NULLIF(id_resolution_reason, ''), ?),
                        id_resolution_updated_at = ?,
                        author_type = 'followed'
                    WHERE id = ?
                    """,
                    (
                        matched_name or requested_author_name,
                        status_value,
                        reason_value[:1000],
                        now_iso,
                        author_id,
                    ),
                )
            else:
                db.execute(
                    """
                    INSERT INTO authors (
                        name, id, openalex_id, scholar_id, orcid,
                        id_resolution_status, id_resolution_reason, id_resolution_updated_at,
                        author_type, added_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'followed', ?)
                    """,
                    (
                        repair_display_text(matched_name or requested_author_name or primary_id),
                        primary_id,
                        _norm_oaid(resolved_openalex) if resolved_openalex else None,
                        resolved_scholar,
                        normalize_orcid(resolved_orcid),
                        status_value,
                        reason_value[:1000],
                        now_iso,
                        now_iso,
                    ),
                )

            # Snapshot follow state BEFORE _sync_follow_state may insert it.
            was_followed = db.execute(
                "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1",
                (author_id,),
            ).fetchone() is not None
            needs_sweep = _sync_follow_state(db, author_id, followed=True)

            # Clear any "missing author" negative signal (best-effort; same
            # connection/txn, so it can't deadlock — only swallow data errors).
            try:
                feedback_author_id = _norm_oaid(resolved_openalex) if resolved_openalex else author_id
                if feedback_author_id:
                    from alma.application.gap_radar import clear_missing_author_feedback

                    clear_missing_author_feedback(db, feedback_author_id)
            except Exception:
                logger.debug("clear_missing_author_feedback skipped for %s", author_id, exc_info=True)

            return (was_followed, needs_sweep)

        already_followed, needs_hydration_sweep = run_write_unit(
            db, _persist_follow, label="follow_author_from_paper"
        )

        # Post-commit (gate + write lock released): scheduler-connection
        # writes can't contend with us now (apply_follow_state self-deadlock).
        # Also sweep when we DEFERRED identity resolution (resolution is None on
        # the fast path) so the background completes the enrichment we skipped.
        if needs_hydration_sweep or resolution is None:
            try:
                from alma.services.author_hydrate import (
                    schedule_pending_author_hydration_sweep,
                )

                schedule_pending_author_hydration_sweep(
                    reason="author_follow",
                    target_author_ids=[author_id],
                )
            except Exception as exc:
                logger.debug("author hydration sweep skipped for %s: %s", author_id, exc)
        if not already_followed:
            try:
                schedule_followed_author_historical_backfill(author_id, trigger="paper_author_follow")
            except Exception as exc:
                logger.debug("Could not queue historical backfill for %s: %s", author_id, exc)
        _log_author_action(
            db,
            "follow_from_paper",
            f"Followed {matched_name or requested_author_name} from paper {payload.paper_id}",
            author_id=author_id,
            data={"created": created, "matched_via": matched_via},
        )

        author_data = authors_app.get_author(db, author_id)
        if author_data is None:
            raise HTTPException(status_code=404, detail="Author could not be loaded after follow")
        return AuthorFollowFromPaperResponse(
            author=_author_response_from_data(author_data),
            created=created,
            already_followed=already_followed,
            matched_via=matched_via,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise_internal("Failed to follow author from paper", e)


@router.post(
    "",
    response_model=AuthorResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add a new author",
    description="Add a new author to monitor by their Google Scholar ID.",
)
def create_author(
    author: AuthorCreate,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Add a new author to monitor.

    This will fetch the author's name from Google Scholar and add them
    to the database.

    Args:
        author: Author creation request with scholar_id

    Returns:
        AuthorResponse: The created author information

    Raises:
        HTTPException: If author already exists or Scholar ID is invalid

    Example:
        ```bash
        curl -X POST http://localhost:8000/api/v1/authors \\
             -H "Content-Type: application/json" \\
             -d '{"scholar_id": "abc123xyz"}'
        ```
    """
    try:
        from alma.openalex.client import get_author_name_by_id

        scholar_id = (author.scholar_id or "").strip() or None
        openalex_id = normalize_author_id((author.openalex_id or "").strip()) or None
        orcid = (getattr(author, "orcid", None) or "").strip() or None
        provided_name = (getattr(author, "name", None) or "").strip() or None
        if not scholar_id and not openalex_id and not orcid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide one of: scholar_id, openalex_id, or orcid")

        # Human-readable handle for error details — failures must name WHO
        # could not be added, not just that "an author" failed.
        author_label = provided_name or openalex_id or orcid or scholar_id

        # Network identity resolution stays OUTSIDE the write transaction
        # below (SQLite write locks must never be held across network I/O).
        # Upstream outages surface as a clear 502 naming the author, not as
        # an opaque 500.
        settings = _id_resolution_settings()
        try:
            resolution = resolve_author_identity(
                db,
                author_name=provided_name,
                openalex_id=openalex_id,
                scholar_id=scholar_id,
                orcid=orcid,
                use_semantic_scholar=settings["semantic_scholar_enabled"],
                use_orcid=settings["orcid_enabled"],
                use_scholar_scrape_auto=settings["scholar_scrape_auto_enabled"],
            )
        except Exception as exc:
            logger.warning(
                "Identity resolution failed for %s: %s", author_label, exc, exc_info=exc
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=(
                    f"Identity lookup for '{author_label}' failed — an upstream "
                    "service (OpenAlex / ORCID / Semantic Scholar) did not "
                    "respond. Try again in a moment."
                ),
            )

        resolved_openalex = resolution.openalex_id or (_norm_oaid(openalex_id) if openalex_id else None)
        resolved_scholar = resolution.scholar_id or scholar_id
        resolved_orcid = resolution.orcid or orcid
        primary_id = resolved_scholar or resolved_openalex or resolved_orcid
        if not primary_id:
            raise HTTPException(
                status_code=404,
                detail=f"Could not resolve a stable identity for '{author_label}' — check the identifier",
            )

        name = (
            provided_name
            or resolution.author_name
            or (get_author_name_by_id(resolved_openalex) if resolved_openalex else None)
            or primary_id
        )
        now_iso = utcnow().isoformat()
        status_value = resolution.status if resolution.status != "no_match" else "resolved_manual"
        reason_value = summarize_author_resolution(resolution) or "Author created"

        # One serialized write unit (process writer gate + BEGIN IMMEDIATE +
        # lock retry via run_write_unit). The existence checks live INSIDE
        # the unit so two concurrent adds of the same person can't race past
        # each other into a UNIQUE violation. Re-adding an author who already
        # has a row PROMOTES that row to followed instead of erroring — the
        # user's intent ("monitor this person") is satisfiable, and rejecting
        # duplicates only produced confusing failures (D4 "promote, don't
        # reject" spirit). Only "already followed" remains a 409, and it
        # names the author.
        from alma.core.db_write import run_write_unit

        def _persist_create() -> tuple[str, bool, bool]:
            """Insert or promote; returns (author_id, was_promoted, needs_sweep)."""
            existing_row = db.execute(
                "SELECT id, name FROM authors WHERE id = ?", (primary_id,)
            ).fetchone()
            if existing_row is None and resolved_openalex:
                existing_row = db.execute(
                    "SELECT id, name FROM authors WHERE lower(openalex_id) = lower(?)",
                    (_norm_oaid(resolved_openalex),),
                ).fetchone()
            if existing_row is not None:
                existing_id = str(existing_row["id"] if isinstance(existing_row, sqlite3.Row) else existing_row[0])
                existing_name = str((existing_row["name"] if isinstance(existing_row, sqlite3.Row) else existing_row[1]) or existing_id)
                already_followed = db.execute(
                    "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1",
                    (existing_id,),
                ).fetchone() is not None
                if already_followed:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"You already follow {existing_name}",
                    )
                needs_sweep = _sync_follow_state(db, existing_id, followed=True)
                return existing_id, True, needs_sweep

            db.execute(
                """
                INSERT INTO authors (
                    name, id, openalex_id, scholar_id, orcid,
                    id_resolution_status, id_resolution_reason, id_resolution_updated_at,
                    author_type, added_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'followed', ?)
                """,
                (
                    name,
                    primary_id,
                    _norm_oaid(resolved_openalex) if resolved_openalex else None,
                    resolved_scholar,
                    normalize_orcid(resolved_orcid),
                    status_value,
                    reason_value[:1000],
                    now_iso,
                    now_iso,
                ),
            )
            needs_sweep = _sync_follow_state(db, primary_id, followed=True)
            return primary_id, False, needs_sweep

        primary_id, was_promoted, needs_hydration_sweep = run_write_unit(
            db, _persist_create, label="create_author"
        )
        # Post-commit: schedule the hydration sweep for the row enqueued by
        # apply_follow_state, now that the writer lock is released
        # (in-transaction scheduling self-deadlocks — see apply_follow_state).
        if needs_hydration_sweep:
            try:
                from alma.services.author_hydrate import (
                    schedule_pending_author_hydration_sweep,
                )

                schedule_pending_author_hydration_sweep(
                    reason="author_follow",
                    target_author_ids=[primary_id],
                )
            except Exception as exc:
                logger.debug("author hydration sweep skipped for %s: %s", primary_id, exc)
        if was_promoted:
            # Promotion keeps the existing row (its id + identifiers win);
            # refresh the response fields from that row.
            row = db.execute(
                """
                SELECT name, openalex_id, scholar_id, orcid,
                       id_resolution_status, id_resolution_reason
                FROM authors WHERE id = ?
                """,
                (primary_id,),
            ).fetchone()
            if row is not None:
                name = str(row["name"] or name)
                resolved_openalex = row["openalex_id"] or resolved_openalex
                resolved_scholar = row["scholar_id"] or resolved_scholar
                resolved_orcid = row["orcid"] or resolved_orcid
                status_value = row["id_resolution_status"] or status_value
                reason_value = row["id_resolution_reason"] or reason_value
        try:
            schedule_followed_author_historical_backfill(primary_id, trigger="author_create")
        except Exception as exc:
            logger.debug("Could not queue historical backfill for %s: %s", primary_id, exc)
        logger.info(
            "%s author: %s (%s)", "Promoted" if was_promoted else "Added", name, primary_id
        )

        runner = OperationRunner(db)

        def _op(_ctx):
            return OperationOutcome(
                status="completed",
                message=(
                    f"Author promoted to followed: {name}"
                    if was_promoted
                    else f"Author created: {name}"
                ),
                result={"author_id": primary_id, "name": name, "promoted": was_promoted},
            )

        runner.run(
            operation_key="authors.create",
            handler=_op,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
        )

        return AuthorResponse(
            id=primary_id,
            name=name,
            added_at=None,
            publication_count=0,
            author_type="followed",
            openalex_id=resolved_openalex,
            scholar_id=resolved_scholar,
            orcid=resolved_orcid,
            id_resolution_status=status_value,
            id_resolution_reason=reason_value[:1000],
        )

    except HTTPException:
        raise
    except Exception as e:
        # Build the label from the raw request so it's available no matter
        # where in the flow the failure happened.
        label = (
            (getattr(author, "name", None) or "").strip()
            or (author.openalex_id or "").strip()
            or (getattr(author, "orcid", None) or "").strip()
            or (author.scholar_id or "").strip()
            or "author"
        )
        raise_internal(f"Failed to add author '{label}'", e)


@router.get(
    "/needs-attention",
    summary="Authors whose identity / refresh needs manual attention",
    description=(
        "Surface authors that the automatic resolver couldn't finish: "
        "ambiguous name-based matches, OpenAlex 404s, last-refresh errors. "
        "Each row carries a human-readable reason + suggested next action "
        "so the UI can render a direct fix button."
    ),
)
def list_authors_needs_attention(
    limit: int = Query(50, ge=1, le=500),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Return the rows the user should look at manually.

    Shape per row: ``{author_id, author_name, reason_code, reason_text,
    suggested_action, confidence, updated_at, openalex_id}``. The UI
    consumes `reason_code` to pick an icon/tone and `suggested_action`
    to label the fix button. Keep it sorted by severity (error first,
    then no_match, then needs_manual_review, then missing-openalex-on-
    followed), within each bucket by most-recently-seen.
    """
    from alma.services import author_attention

    total, rows = author_attention.needs_attention_page(db, limit=limit)
    return {"total": total, "items": rows}


def _validated_manual_identifiers(
    db: sqlite3.Connection,
    author_id: str,
    *,
    orcid: str | None,
    openalex_id: str | None,
    scholar_id: str | None,
) -> dict[str, str]:
    """The identifiers a person typed, normalized and safe to write.

    Blank fields are absent. Anything malformed is a 422, because storing it
    would mark the author resolved with no identity behind it (a bad ORCID
    normalizes to NULL). An ORCID or OpenAlex id another author already holds
    is a 409 that names them: the way out is a merge, not a second owner.
    """
    clean: dict[str, str] = {}
    if (orcid or "").strip():
        normalized = normalize_orcid(orcid)
        if not normalized:
            raise HTTPException(
                status_code=422,
                detail=f"'{orcid.strip()}' is not an ORCID iD (0000-0000-0000-0000).",
            )
        clean["orcid"] = normalized
    if (openalex_id or "").strip():
        normalized = normalize_openalex_id(openalex_id)
        if not re.fullmatch(r"A\d+", normalized):
            raise HTTPException(
                status_code=422,
                detail=f"'{openalex_id.strip()}' is not an OpenAlex author ID (A1234567890).",
            )
        clean["openalex_id"] = normalized
    if (scholar_id or "").strip():
        clean["scholar_id"] = scholar_id.strip()

    for column, label in (("orcid", "ORCID"), ("openalex_id", "OpenAlex ID")):
        value = clean.get(column)
        if not value:
            continue
        owner = db.execute(
            f"""
            SELECT name FROM authors
            WHERE lower(trim({column})) = lower(?)
              AND id != ?
              AND COALESCE(status, 'active') != 'removed'
            LIMIT 1
            """,  # noqa: S608 — column comes from the literal tuple above
            (value, author_id),
        ).fetchone()
        if owner:
            raise HTTPException(
                status_code=409,
                detail=f"{label} {value} already belongs to {owner['name']}. Merge the two authors instead.",
            )
    return clean


class SetIdentifiersRequest(BaseModel):
    """Manual paste of authoritative identifiers for an author who
    failed automatic resolution. Any subset is accepted; absent fields
    are left unchanged. ORCID + OpenAlex + Scholar IDs are normalized
    server-side (URL prefix stripped, lowercased)."""

    orcid: str | None = Field(default=None, description="ORCID iD (with or without https prefix)")
    openalex_id: str | None = Field(default=None, description="OpenAlex author ID (A1234… or full URL)")
    scholar_id: str | None = Field(default=None, description="Google Scholar user id")


@router.post(
    "/{author_id}/identifiers",
    summary="Manually set authoritative identifiers for an author",
    description=(
        "Used by the Authors Needs-Attention 'Add identifier' action. "
        "Updates ORCID / OpenAlex / Scholar id on the row, sets "
        "id_resolution_status='resolved_manual', and clears any "
        "previous needs-attention reason. Subsequent refreshes use the "
        "provided OpenAlex id directly without re-running the resolver."
    ),
)
def set_author_identifiers(
    author_id: str,
    body: SetIdentifiersRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.services.author_hydrate import (
        enqueue_pending_author_hydration,
        schedule_pending_author_hydration_sweep,
    )

    row = db.execute("SELECT id FROM authors WHERE id = ?", (author_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Author not found")

    clean = _validated_manual_identifiers(
        db,
        author_id,
        orcid=body.orcid,
        openalex_id=body.openalex_id,
        scholar_id=body.scholar_id,
    )
    if not clean:
        raise HTTPException(status_code=400, detail="No identifiers supplied")
    updates = [f"{column} = ?" for column in clean]  # keys are this function's literals
    params: list[object] = list(clean.values())

    now = utcnow().isoformat()
    updates.extend([
        "id_resolution_status = ?",
        "id_resolution_method = ?",
        "id_resolution_confidence = ?",
        "id_resolution_reason = ?",
        "id_resolution_updated_at = ?",
    ])
    params.extend([
        "resolved_manual",
        "manual_paste",
        1.0,
        "User-supplied identifiers",
        now,
    ])
    params.append(author_id)

    # One atomic, serialized write unit for the single identifier UPDATE.
    # `params` is fully built above (once), so re-running the unit on a lock
    # retry simply re-issues the same statement — idempotent.
    def _persist() -> None:
        db.execute(
            f"UPDATE authors SET {', '.join(updates)} WHERE id = ?",
            params,
        )
        # A new identifier is the author equivalent of a paper insert: it joins
        # the hydration lane now, the same as a follow or a merge.
        enqueue_pending_author_hydration(
            db, author_id, priority="high", reason="manual_identifiers"
        )

    run_write_unit(db, _persist, label="set_author_identifiers")
    schedule_pending_author_hydration_sweep(
        reason="manual_identifiers", target_author_ids=[author_id]
    )
    _log_author_action(
        db,
        "set_identifiers",
        f"Set manual identifiers for author {author_id}",
        author_id=author_id,
        data={
            "orcid": body.orcid,
            "openalex_id": body.openalex_id,
            "scholar_id": body.scholar_id,
        },
    )

    # Read-back for the response — OUTSIDE the unit (reads need no gate).
    refreshed = db.execute(
        "SELECT id, name, openalex_id, orcid, scholar_id, id_resolution_status FROM authors WHERE id = ?",
        (author_id,),
    ).fetchone()
    return {
        "author_id": author_id,
        "openalex_id": refreshed["openalex_id"],
        "orcid": refreshed["orcid"],
        "scholar_id": refreshed["scholar_id"],
        "id_resolution_status": refreshed["id_resolution_status"],
    }


class MergeProfilesRequest(BaseModel):
    """Body for `POST /authors/{primary_id}/merge-profiles`. Each
    `alt_author_ids` entry is an `authors.id` value (NOT an OpenAlex
    ID) so the frontend can pass through `alt_profiles[].author_id`
    from the needs-attention payload directly. `alt_openalex_ids`
    absorbs identities with NO local row (suggestion-rail duplicates):
    papers reattach + the id is recorded as an alias of the primary.
    At least one of the two lists must be non-empty."""

    alt_author_ids: list[str] = Field(default_factory=list)
    alt_openalex_ids: list[str] = Field(default_factory=list)
    field_choices: dict[str, dict[str, str]] = Field(default_factory=dict)


@router.post(
    "/{author_id}/merge-profiles",
    summary="Merge alt OpenAlex profiles into a primary author",
    description=(
        "Used by the Authors Needs-Attention 'Review profiles' dialog "
        "to collapse multiple OpenAlex IDs that represent the same "
        "human into one canonical row. For each alt: papers reattach "
        "to the primary's openalex_id, the alt's followed_authors / "
        "feed_monitors entries are dropped, the alt row is soft-removed "
        "(D3 lifecycle), and an `author_alt_identifiers` row records the "
        "alias so suggestion-rail dedup never resurfaces it."
    ),
)
def merge_author_profiles_route(
    author_id: str,
    body: MergeProfilesRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import merge_author_profiles

    if not body.alt_author_ids and not body.alt_openalex_ids:
        raise HTTPException(
            status_code=400,
            detail="Provide at least one of alt_author_ids or alt_openalex_ids",
        )

    # One job id ties the application-layer per-alt audit logs (emitted by
    # merge_author_profiles post-commit) and the user-facing Activity status
    # row below into a SINGLE Activity entry.
    job_id = f"author_merge_{uuid.uuid4().hex[:10]}"

    def _do_merge() -> dict:
        # The merge is one atomic transaction, serialized behind the writer
        # gate (run_write_unit: BEGIN IMMEDIATE + lock retry), so a retry
        # safely re-runs the whole thing. ValueError (validation) is
        # non-lock and propagates at once.
        return merge_author_profiles(
            db,
            author_id,
            body.alt_author_ids,
            alt_openalex_ids=body.alt_openalex_ids,
            field_choices=body.field_choices,
            job_id=job_id,
        )

    try:
        summary = run_write_unit(db, _do_merge, label="merge_author_profiles")
    except MergeRefused as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise_internal(f"Failed to merge profiles into {author_id}", exc)

    # Surface the merge in the Activity list. The rich per-alt audit lines
    # are already logged by the application layer under this same job_id;
    # here we add the completed status row (its headline status line) so the
    # merge appears as one coherent Activity entry.
    _log_author_action(
        db,
        "merge",
        (
            f"Merged {summary['alts_processed']} profile(s) into {author_id}: "
            f"{summary['papers_reassigned']} papers reassigned, "
            f"{len(summary['conflicts'])} conflict(s) to review"
        ),
        author_id=author_id,
        data={
            "alts_processed": summary["alts_processed"],
            "papers_reassigned": summary["papers_reassigned"],
            "conflicts": summary["conflicts"],
        },
        job_id=job_id,
    )
    return summary


class ResolveConflictRequest(BaseModel):
    """Body for `POST /authors/conflicts/{conflict_id}/resolve`.
    `choice` = which side wins ('primary' or 'alt'). When 'alt' wins,
    the primary author row's column gets overwritten with the alt's
    value before the conflict is marked resolved. `dismiss` skips
    both — the conflict goes away without changing the row."""

    choice: str = Field(..., pattern="^(primary|alt|dismiss)$")


@router.post(
    "/conflicts/{conflict_id}/resolve",
    summary="Resolve a merge conflict by picking one side or dismissing",
)
def resolve_merge_conflict(
    conflict_id: str,
    body: ResolveConflictRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import ensure_alt_identifiers_table

    ensure_alt_identifiers_table(db)
    row = db.execute(
        "SELECT id, primary_author_id, field, primary_value, alt_value, status "
        "FROM author_merge_conflicts WHERE id = ?",
        (conflict_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Conflict not found")
    if row["status"] != "unresolved":
        raise HTTPException(status_code=400, detail=f"Conflict already {row['status']}")

    # Decide the outcome up front (pure compute on the already-read row);
    # the field allowlist + 400 are control-flow, kept OUTSIDE the unit.
    now = utcnow().isoformat()
    target_field: str | None = None
    new_value: object = None
    if body.choice == "primary":
        new_status = "resolved_keep_primary"
    elif body.choice == "alt":
        # When 'alt' wins, overwrite the primary author's column with the
        # alt's value. Field is allowlisted — only hard-identifier columns
        # get into author_merge_conflicts in the first place.
        field = str(row["field"])
        if field not in {"orcid", "scholar_id", "semantic_scholar_id"}:
            raise HTTPException(status_code=400, detail=f"Unknown conflict field: {field}")
        target_field = field
        new_value = row["alt_value"]
        if field == "orcid":
            # Same refusal as a typed ORCID: another author may hold this one.
            new_value = _validated_manual_identifiers(
                db,
                str(row["primary_author_id"]),
                orcid=row["alt_value"],
                openalex_id=None,
                scholar_id=None,
            )["orcid"]
        new_status = "resolved_use_alt"
    else:
        new_status = "dismissed"

    # One atomic write unit: apply the chosen value to the primary (only when
    # 'alt' won) AND mark the conflict resolved together, serialized behind
    # the writer gate.
    def _persist() -> None:
        if target_field is not None:
            db.execute(
                f"UPDATE authors SET {target_field} = ? WHERE id = ?",  # noqa: S608 — allowlist above
                (new_value, row["primary_author_id"]),
            )
        db.execute(
            "UPDATE author_merge_conflicts SET status = ?, resolved_at = ? WHERE id = ?",
            (new_status, now, conflict_id),
        )

    run_write_unit(db, _persist, label="resolve_merge_conflict")
    _log_author_action(
        db,
        "resolve_conflict",
        f"Resolved merge conflict {conflict_id}: {new_status}",
        author_id=str(row["primary_author_id"]),
        data={"conflict_id": conflict_id, "choice": body.choice, "status": new_status},
    )
    return {
        "conflict_id": conflict_id,
        "status": new_status,
        "primary_author_id": row["primary_author_id"],
    }


class NotSamePersonRequest(BaseModel):
    """Body for `POST /authors/{id}/not-same-person`: the `authors.id` values
    the user says are different people from the path author."""

    alt_author_ids: list[str] = Field(..., min_length=1)


@router.post(
    "/{author_id}/not-same-person",
    summary="Record that authors sharing a name are different people",
    description=(
        "The other answer to a split-profile warning, next to merge. Stores the "
        "unordered pair in `author_merge_rejections` (D17: permanent), so neither "
        "the warning nor any dedup detector proposes it again."
    ),
)
def not_same_person(
    author_id: str,
    body: NotSamePersonRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import reject_author_pair

    alt_ids = [alt for alt in dict.fromkeys(body.alt_author_ids) if alt != author_id]
    if not alt_ids:
        raise HTTPException(status_code=400, detail="An author cannot be rejected against itself")
    placeholders = ",".join("?" for _ in [author_id, *alt_ids])
    known = {
        row["id"]
        for row in db.execute(
            f"SELECT id FROM authors WHERE id IN ({placeholders})",  # noqa: S608 — placeholders only
            (author_id, *alt_ids),
        ).fetchall()
    }
    if len(known) != len(alt_ids) + 1:
        raise HTTPException(status_code=404, detail="Author not found")

    def _persist() -> None:
        for alt_id in alt_ids:
            reject_author_pair(db, author_id, alt_id)

    run_write_unit(db, _persist, label="not_same_person")
    _log_author_action(
        db,
        "not_same_person",
        f"Marked {author_id} as a different person from {len(alt_ids)} author(s)",
        author_id=author_id,
        data={"alt_author_ids": alt_ids},
    )
    return {"author_id": author_id, "rejected": alt_ids}


@router.post(
    "/{author_id}/discover-aliases",
    summary="Discover OpenAlex split profiles via ORCID",
    description=(
        "Looks up the author's ORCID on OpenAlex and queries every "
        "OpenAlex author profile sharing that ORCID. ORCID is "
        "person-level and verified, so co-occurrence is high-"
        "confidence evidence of a split profile. Returns the alias "
        "list for the user to merge via /merge-profiles. Read-only — "
        "does not modify any rows."
    ),
)
def discover_author_aliases(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import annotate_discovered_aliases, discover_aliases_via_orcid

    row = db.execute(
        "SELECT id, name, openalex_id FROM authors WHERE id = ?", (author_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Author not found")
    primary_oid = str(row["openalex_id"] or "").strip()
    if not primary_oid:
        raise HTTPException(
            status_code=400,
            detail="Author has no OpenAlex ID — set one first via /identifiers",
        )
    summary = discover_aliases_via_orcid(primary_oid)
    summary["aliases"] = annotate_discovered_aliases(db, author_id, summary["aliases"])
    summary["primary_author_id"] = author_id
    summary["primary_display_name"] = str(row["name"] or "")
    return summary


@router.get(
    "/merge-candidates",
    summary="List pending author merge candidates (the review queue)",
    description=(
        "The duplicate author pairs the scan recorded and that haven't been merged "
        "yet — each with both names, its `source` ('orcid'/'name') + `confidence`, "
        "and the number of papers the merge would reassign. Drives the Merge card's "
        "review dialog. Defined before /{author_id} so the static path isn't shadowed."
    ),
)
def list_merge_candidates_endpoint(
    limit: int = Query(200, ge=1, le=1000),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import list_merge_candidates

    return {"candidates": list_merge_candidates(db, limit=limit)}


@router.post(
    "/merge-candidates/{candidate_id}/reject",
    summary="Reject one merge suggestion (permanent — never resurfaced)",
    description=(
        "The user says 'these two are NOT the same person'. Records a permanent "
        "rejection of the unordered author pair and drops the candidate, so every "
        "future scan skips it. Foreground write through the request's gated "
        "connection (run_write_unit)."
    ),
)
def reject_merge_candidate_endpoint(
    candidate_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import reject_merge_candidate
    from alma.core.db_write import run_write_unit

    result = run_write_unit(
        db, lambda: reject_merge_candidate(db, candidate_id), label="reject_merge_candidate"
    )
    if not result.get("success"):
        raise HTTPException(status_code=404, detail="Merge candidate not found")
    return result


@router.post(
    "/merge-candidates/{candidate_id}/merge",
    summary="Apply one merge suggestion (the per-row ✓ Merge)",
    description=(
        "Merge this candidate's alt → primary (richer/followed profile wins, the "
        "other's papers reassigned) and consume the candidate. Destructive, not "
        "reversed automatically. Foreground write through the request's gated "
        "connection (run_write_unit)."
    ),
)
def merge_one_candidate_endpoint(
    candidate_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.author_merge import merge_one_candidate
    from alma.core.db_write import run_write_unit

    result = run_write_unit(
        db, lambda: merge_one_candidate(db, candidate_id), label="merge_one_candidate"
    )
    if not result.get("success"):
        raise HTTPException(status_code=404, detail="Merge candidate not found")
    return result


@router.delete(
    "/{author_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an author",
    description=(
        "Permanently delete an author, keeping every paper in the Library or "
        "touched by the user. Only plain tracked papers no other author covers "
        "go with them."
    ),
)
def delete_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Delete an author and the corpus papers nobody else has a claim on.

    Args:
        author_id: ID of the author to delete

    Raises:
        HTTPException: If author is not found

    Example:
        ```bash
        curl -X DELETE http://localhost:8000/api/v1/authors/abc123xyz
        ```
    """
    runner = OperationRunner(db)
    try:
        def _handler(_ctx):
            deleted = authors_app.delete_author(db, author_id)
            if deleted is None:
                return OperationOutcome(status="noop", message=f"Author {author_id} not found", result={"author_id": author_id})
            return OperationOutcome(
                status="completed",
                message=f"Deleted author {author_id}",
                result={"author_id": author_id, "name": deleted["name"]},
            )

        op = runner.run(
            operation_key=f"authors.delete:{author_id}",
            handler=_handler,
            trigger_source="user",
            actor=str(user.get("username") or "api_user"),
            write_unit=True,
        )
        if op["status"] == "noop":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Author with ID {author_id} not found",
            )
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to delete author {author_id}", e)


@router.post(
    "/{author_id}/remove",
    summary="Soft-remove an author",
    description=(
        "Soft-remove an author (status='removed', D3): the row + provenance stay "
        "for the Corpus explorer and Discovery's negative signal, and it drops out "
        "of active author views. Reversible. This is the normal-flow removal (e.g. "
        "the suggestion-detail Delete, alongside the suggestion dismiss); the hard "
        "DELETE is reserved for explicit purges."
    ),
)
def remove_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.author_lifecycle import soft_remove_author

    followed = db.execute(
        "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1", (author_id,)
    ).fetchone()
    if followed is not None:
        raise HTTPException(status_code=409, detail="This author is followed. Unfollow them first.")

    try:
        # soft_remove_author is caller-owns-transaction; wrap it in one
        # serialized, atomic unit (D3 soft-remove: status='removed'). It is
        # idempotent, so a lock retry re-runs it cleanly.
        changed = run_write_unit(
            db,
            lambda: soft_remove_author(db, author_id, reason="manual remove (author detail)"),
            label="remove_author",
        )
        if changed:
            _log_author_action(
                db,
                "remove",
                f"Soft-removed author {author_id}",
                author_id=author_id,
            )
        return {"author_id": author_id, "status": "removed" if changed else "noop"}
    except Exception as e:
        raise_internal(f"Failed to remove author {author_id}", e)


class ConfirmIdentifiersRequest(BaseModel):
    openalex_id: str | None = None
    scholar_id: str | None = None
    orcid: str | None = None
    status: str = "resolved_manual"
    reason: str | None = None


class ResolveIdentifiersRequest(BaseModel):
    limit: int = 200
    only_unresolved: bool = True
    background: bool = True
    scope: str = Field(
        "corpus",
        description=(
            "Which authors to run resolution on: "
            "`library` (authors linked to Library papers), "
            "`followed` (followed authors only), "
            "`corpus` (every author row)."
        ),
    )


def _resolve_identifiers_bulk_optimized(
    db: sqlite3.Connection,
    authors: list[tuple[str, str]],
    job_id: str,
    max_workers: int = 10,
) -> dict:
    """Robust bulk identifier resolution using the shared author resolver.

    Per-unit contract (see `tasks/lessons.md` "Activity progress must push
    to operation_status"): every author iteration commits its own writes
    and pushes `set_job_status(processed=idx, total=total, current_author=...)`
    so the UI's progress bar advances live instead of waiting for the
    whole batch. Per-row try/except absorbs one bad author (OpenAlex 404,
    malformed name, …) without killing the batch.
    """
    import time

    from alma.api.scheduler import add_job_log, set_job_status

    total = len(authors)
    if total == 0:
        return {"total": 0, "resolved_auto": 0, "needs_manual_review": 0, "no_match": 0, "duplicate": 0, "error": 0}

    t0 = time.monotonic()
    summary = {
        "total": total,
        "resolved_auto": 0,
        "needs_manual_review": 0,
        "no_match": 0,
        "duplicate": 0,
        "error": 0,
        "enriched": 0,
        "elapsed_seconds": 0.0,
    }

    add_job_log(job_id, f"Resolving identifiers for {total} authors via shared resolver", step="id_resolution_start")
    set_job_status(job_id, status="running", processed=0, total=total)

    for idx, (author_id, author_name) in enumerate(authors, 1):
        try:
            # Network OUTSIDE the writer gate: identity resolution (OpenAlex /
            # Semantic Scholar / ORCID) + the sample-title read.
            resolution = resolve_author_identity(
                db,
                author_id=author_id,
                author_name=author_name,
                sample_titles=_get_author_sample_titles(db, author_id, limit=4),
                use_semantic_scholar=_id_resolution_settings()["semantic_scholar_enabled"],
                use_orcid=_id_resolution_settings()["orcid_enabled"],
            )

            # Gate this author's write window (BEGIN IMMEDIATE + writer gate);
            # the duplicate-check SELECT inside sees the unit's own snapshot.
            with write_section(db, label="bulk id resolution"):
                duplicate = False
                resolved_openalex = _norm_oaid(str(resolution.openalex_id or ""))
                if resolved_openalex:
                    existing = db.execute(
                        "SELECT id, name FROM authors WHERE lower(openalex_id) = lower(?) AND id != ?",
                        (resolved_openalex, author_id),
                    ).fetchone()
                    if existing:
                        duplicate = True
                        _set_resolution_status(
                            db,
                            author_id,
                            "needs_manual_review",
                            f"duplicate openalex_id {resolved_openalex} — already assigned to '{existing['name']}' ({existing['id']})",
                        )
                        summary["duplicate"] += 1

                if not duplicate:
                    _apply_author_resolution_result(db, author_id, resolution)
                    if resolution.openalex_profile:
                        summary["enriched"] += 1
                    if resolution.status == "resolved_auto":
                        summary["resolved_auto"] += 1
                    elif resolution.status == "needs_manual_review":
                        summary["needs_manual_review"] += 1
                    elif resolution.status == "no_match":
                        summary["no_match"] += 1
                    else:
                        summary["resolved_auto"] += 1 if resolution.updates else 0
        except Exception as exc:
            summary["error"] += 1
            try:
                # The failed unit already rolled back; record the error status
                # in its own gated write window.
                with write_section(db, label="bulk id resolution error"):
                    _set_resolution_status(db, author_id, "error", f"resolver error: {type(exc).__name__}: {exc}")
            except Exception:
                logger.debug("Failed to persist resolver error status", exc_info=True)
            if summary["error"] <= 10:
                add_job_log(
                    job_id,
                    f"Identifier resolution error for '{author_name}' ({author_id}): {type(exc).__name__}: {exc}",
                    level="ERROR",
                    step="id_resolution_error",
                )

        # Per-unit progress — let the Activity UI tick live.
        set_job_status(
            job_id,
            status="running",
            processed=idx,
            total=total,
            current_author=author_name,
            message=(
                f"{idx}/{total} · {summary['resolved_auto']} resolved · "
                f"{summary['needs_manual_review']} review · {summary['no_match']} no_match · "
                f"{summary['duplicate']} dup · {summary['error']} err"
            ),
        )
        # Keep the structured log digest for Activity timeline — every 25
        # rows gives a readable history without spamming the log table.
        if idx % 25 == 0 or idx == total:
            add_job_log(
                job_id,
                (
                    f"Resolution progress {idx}/{total}: "
                    f"{summary['resolved_auto']} resolved, {summary['needs_manual_review']} review, "
                    f"{summary['no_match']} no_match, {summary['duplicate']} duplicate, {summary['error']} error"
                ),
                step="id_resolution_progress",
            )

    # No final flush: each author committed inside its own write_section.
    summary["elapsed_seconds"] = round(time.monotonic() - t0, 1)
    if summary["resolved_auto"] or summary["enriched"]:
        try:
            from alma.services.author_hydrate import schedule_pending_author_hydration_sweep

            schedule_pending_author_hydration_sweep(reason="identity_resolution")
        except Exception as exc:
            logger.debug("Resolved-author hydration sweep skipped: %s", exc)
    add_job_log(
        job_id,
        (
            f"Resolution complete in {summary['elapsed_seconds']:.1f}s: "
            f"{summary['resolved_auto']} resolved, {summary['needs_manual_review']} review, "
            f"{summary['no_match']} no_match, {summary['duplicate']} duplicate, {summary['error']} error"
        ),
        step="id_resolution_summary",
        data=summary,
    )
    return summary


@router.get(
    "/{author_id}/id-candidates",
    summary="Get identifier candidates for an author",
)
def author_id_candidates(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    row = db.execute("SELECT id, name, openalex_id, orcid FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    settings = _id_resolution_settings()
    a_name = str(row["name"] or "").strip() or None
    a_oaid = str(row["openalex_id"] or "").strip() or None
    a_orcid = str(row["orcid"] or "").strip() or None

    def _compute() -> dict:
        # Own connection — this may outlive the request when the deadline fires,
        # so it must NOT touch the request's `db` (committed + closed on return).
        work = open_db_connection()
        try:
            titles = _get_author_sample_titles(work, author_id, limit=5)
            resolution = resolve_author_identity(
                work,
                author_id=author_id,
                author_name=a_name,
                openalex_id=a_oaid,
                orcid=a_orcid,
                sample_titles=titles,
                use_semantic_scholar=settings["semantic_scholar_enabled"],
                use_orcid=settings["orcid_enabled"],
            )
            # `resolution` ALREADY ran the source-based (S2/ORCID) scholar lookup;
            # only pay for the EXTRA scrape pass when auto-scrape is enabled —
            # calling _resolve_scholar_candidates unconditionally re-did the whole
            # network fanout a second time (the doubled work behind the hang).
            scholar_candidates = list(resolution.scholar_candidates or [])
            if settings["scholar_scrape_auto_enabled"]:
                scraped = _resolve_scholar_candidates(
                    a_name or "", titles, openalex_id=a_oaid, orcid=a_orcid, mode="auto"
                )
                if scraped:
                    scholar_candidates = scraped
            return {
                "titles": titles,
                "openalex": resolution.openalex_candidates,
                "scholar": scholar_candidates,
                "resolved": {
                    "openalex_id": resolution.openalex_id,
                    "scholar_id": resolution.scholar_id,
                    "orcid": resolution.orcid,
                    "status": resolution.status,
                    "confidence": resolution.confidence,
                    "reason": resolution.reason,
                },
            }
        finally:
            work.close()

    timed_out = False
    empty = {"titles": [], "openalex": [], "scholar": [], "resolved": None}
    try:
        computed = _ID_CANDIDATE_EXECUTOR.submit(_compute).result(
            timeout=_ID_CANDIDATE_DEADLINE_S
        )
    except FuturesTimeout:
        # Don't cancel — the orphaned worker finishes on its own connection and
        # frees its slot. Return an honest empty result the UI can retry.
        logger.warning(
            "id-candidates lookup for author %s exceeded %.0fs — returning "
            "timed_out (upstream likely rate-limited)",
            author_id,
            _ID_CANDIDATE_DEADLINE_S,
        )
        timed_out = True
        computed = empty
    except Exception as exc:  # noqa: BLE001 — never 500 the diagnostics tab
        logger.warning("id-candidates lookup for author %s failed: %s", author_id, exc)
        computed = empty

    # Surface OpenAlex's daily-cap state so the UI can say "OpenAlex daily limit
    # reached" instead of a bare "no OpenAlex candidates" — the OpenAlex calls
    # fail fast when the quota is drained, so this is the honest reason the
    # OpenAlex column is empty. (Scholar/S2 has no daily pool and still runs.)
    from alma.core.http_sources import provider_remaining_credits

    oa_remaining = provider_remaining_credits("openalex")
    openalex_cap_reached = oa_remaining is not None and oa_remaining <= 0

    return {
        "author_id": author_id,
        "name": row["name"],
        "titles": computed["titles"],
        "openalex": computed["openalex"],
        "scholar": computed["scholar"],
        "resolved": computed["resolved"],
        "timed_out": timed_out,
        "openalex_cap_reached": openalex_cap_reached,
        "scholar_manual_search_enabled": settings["scholar_scrape_manual_enabled"],
        "scholar_auto_scrape_enabled": settings["scholar_scrape_auto_enabled"],
    }


@router.post(
    "/{author_id}/search-scholar",
    summary="Manually search Google Scholar candidates for an author",
)
def author_search_scholar_manual(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    settings = _id_resolution_settings()
    if not settings["scholar_scrape_manual_enabled"]:
        raise HTTPException(
            status_code=403,
            detail="Manual Google Scholar scraping is disabled in settings",
        )

    row = db.execute(
        "SELECT id, name, openalex_id, orcid FROM authors WHERE id = ?",
        (author_id,),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")

    titles = _get_author_sample_titles(db, author_id, limit=5)
    cands_sch = _resolve_scholar_candidates(
        row["name"],
        titles,
        openalex_id=(row["openalex_id"] or None),
        orcid=(row["orcid"] or None),
        mode="manual",
    )
    return {
        "author_id": author_id,
        "name": row["name"],
        "titles": titles,
        "candidates": cands_sch,
        "mode": "manual",
    }


@router.post(
    "/{author_id}/confirm-identifiers",
    summary="Manually confirm identifiers for an author",
)
def confirm_identifiers_for_author(
    author_id: str,
    payload: ConfirmIdentifiersRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    row = db.execute("SELECT id FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")

    # Build the column set up front (pure compute — no writes yet).
    clean = _validated_manual_identifiers(
        db,
        author_id,
        orcid=payload.orcid,
        openalex_id=payload.openalex_id,
        scholar_id=payload.scholar_id,
    )
    updates = [f"{column} = ?" for column in clean]  # keys are the helper's literals
    params: list[Any] = list(clean.values())

    status_value = (payload.status or "resolved_manual").strip().lower()
    if status_value not in _RESOLUTION_STATUSES:
        status_value = "resolved_manual"
    reason = (payload.reason or "Identifiers manually confirmed").strip()
    if status_value == "resolved_manual" and not clean and not any(
        db.execute(
            "SELECT openalex_id, orcid, scholar_id FROM authors WHERE id = ?", (author_id,)
        ).fetchone()
    ):
        raise HTTPException(status_code=400, detail="No identifiers supplied")

    # One atomic write unit: set the chosen identifiers (if any) and stamp
    # the resolution status together, serialized behind the writer gate.
    # `params + [author_id]` builds a fresh arg list each call so a lock
    # retry of the unit can't double-append the id (idempotent unit).
    def _persist() -> None:
        if updates:
            db.execute(
                f"UPDATE authors SET {', '.join(updates)} WHERE id = ?",
                tuple(params + [author_id]),
            )
        _set_resolution_status(db, author_id, status_value, reason)

    run_write_unit(db, _persist, label="confirm_identifiers_for_author")
    _log_author_action(
        db,
        "confirm_identifiers",
        f"Confirmed identifiers for author {author_id} ({status_value})",
        author_id=author_id,
        data={
            "openalex_id": payload.openalex_id,
            "scholar_id": payload.scholar_id,
            "orcid": payload.orcid,
            "status": status_value,
        },
    )
    return {"success": True, "author_id": author_id, "status": status_value}


@router.post(
    "/resolve-identifiers",
    summary="Resolve OpenAlex/Scholar IDs for authors",
)
def resolve_identifiers_bulk(
    req: ResolveIdentifiersRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    limit = max(1, min(int(req.limit or 200), 5000))

    scope = (req.scope or "corpus").strip().lower()
    if scope not in {"library", "followed", "corpus"}:
        scope = "corpus"

    scope_join_clauses: dict[str, str] = {
        "library": (
            "INNER JOIN publication_authors pa ON lower(pa.openalex_id) = lower(a.openalex_id) "
            f"INNER JOIN papers p ON p.id = pa.paper_id AND p.status = 'library' AND {standalone_paper_sql('p')}"
        ),
        "followed": "INNER JOIN followed_authors fa ON fa.author_id = a.id",
        "corpus": "",
    }
    scope_join = scope_join_clauses[scope]

    unresolved_filter = (
        "AND COALESCE(a.id_resolution_status, '') IN ('', 'unresolved', 'needs_manual_review', 'no_match', 'error')"
        if req.only_unresolved
        else ""
    )

    sql = f"""
        SELECT DISTINCT a.id AS id, a.name AS name
        FROM authors a
        {scope_join}
        WHERE 1=1
        {unresolved_filter}
        ORDER BY a.name
        LIMIT ?
    """
    rows = db.execute(sql, (limit,)).fetchall()

    authors_list = [(r["id"], r["name"]) for r in rows]
    total = len(authors_list)

    if total == 0:
        return {"status": "noop", "message": "No authors to resolve", "summary": {"total": 0}}

    # ── Synchronous mode ──
    if not req.background:
        job_id = f"resolve_ids_sync_{uuid.uuid4().hex[:8]}"
        summary = _resolve_identifiers_bulk_optimized(db, authors_list, job_id)
        return {"status": "completed", "summary": summary}

    # ── Background mode (Activity Panel) ──
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    operation_key = "authors.resolve_identifiers"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Author identifier resolution already running",
            total=total,
        )

    job_id = f"resolve_ids_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=utcnow().isoformat(),
        processed=0,
        total=total,
        message=f"Queued identifier resolution for {total} authors",
    )
    add_job_log(job_id, f"Queued identifier resolution for {total} authors", step="queued")

    def _bg():
        conn = open_db_connection()
        try:
            set_job_status(job_id, status="running", message="Resolving author identifiers")
            summary = _resolve_identifiers_bulk_optimized(conn, authors_list, job_id)
            set_job_status(
                job_id,
                status="completed",
                processed=total,
                total=total,
                message="Author identifier resolution completed",
                result=summary,
            )
        except Exception as exc:
            add_job_log(job_id, f"Resolve job failed: {exc}", level="ERROR", step="failed")
            set_job_status(job_id, status="failed", message=f"Author identifier resolution failed: {exc}")
        finally:
            conn.close()

    schedule_immediate(job_id, _bg)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message=f"Queued identifier resolution for {total} authors",
        extra={"total": total},
    )


@router.get(
    "/{author_id}/publications",
    response_model=list[dict],
    summary="Get author's publications",
    description="Retrieve all cached publications for a specific author.",
)
def get_author_publications(
    author_id: str,
    scope: str = Query(default="all", description="Publication scope: all | library | background"),
    order: str = Query(default="citations", description="Sort order: citations | recent"),
    limit: int = 100,
    offset: int = 0,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Get all publications for a specific author.

    Args:
        author_id: Google Scholar ID of the author
        limit: Maximum number of results (default: 100)
        offset: Number of results to skip (default: 0)

    Returns:
        List[dict]: List of publications

    Raises:
        HTTPException: If author is not found

    Example:
        ```bash
        curl http://localhost:8000/api/v1/authors/abc123xyz/publications?limit=10
        ```
    """
    try:
        publications = authors_app.list_author_publications(
            db,
            author_id,
            scope=scope,
            order=order,
            limit=limit,
            offset=offset,
        )
        if publications is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Author with ID {author_id} not found"
            )
        result = publications
        logger.info(f"Retrieved {len(result)} publications for author {author_id}")

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to retrieve publications for author {author_id}", e)


@router.get(
    "/{author_id}/openalex-works",
    summary="Fetch a page of the author's full OpenAlex bibliography",
    description=(
        "Lazy cursor-paginated window onto the author's complete OpenAlex works "
        "list. Does NOT touch the local DB — use /publications for the "
        "already-saved view and /library/import/search/save to pull a work "
        "into Library. Accepts either a local author UUID (resolved to its "
        "openalex_id) or a bare OpenAlex author id (A…)."
    ),
)
def list_author_openalex_works(
    author_id: str,
    cursor: str = Query(default="*", description="OpenAlex cursor ('*' for the first page)."),
    per_page: int = Query(default=50, ge=1, le=100),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.openalex.client import (
        _normalize_openalex_author_id,
        fetch_works_page_for_author,
    )

    openalex_id = ""
    # Try the local authors table first; fall back to treating the raw
    # identifier as an OpenAlex id for suggestion-opened dialogs where no
    # local row exists yet.
    author = authors_app.get_author(db, author_id)
    if author is not None:
        openalex_id = str(author.get("openalex_id") or "").strip()
    if not openalex_id:
        candidate = _normalize_openalex_author_id(author_id or "").strip()
        if candidate.upper().startswith("A"):
            openalex_id = candidate

    if not openalex_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Author has no OpenAlex ID on record.",
        )

    try:
        page = fetch_works_page_for_author(
            openalex_id,
            cursor=cursor,
            per_page=per_page,
        )
    except Exception as e:
        raise_internal("Failed to fetch OpenAlex works for author", e)

    # Annotate each result with whether we already have it locally. This
    # keeps the UI's Save button honest — papers already in the DB get a
    # "saved" pill instead of a Save action.
    already: dict[str, dict] = {}
    results = page.get("results") or []
    work_ids = [
        _normalize_openalex_author_id(str((r or {}).get("openalex_id") or (r or {}).get("id") or ""))
        for r in results
    ]
    # `canonical_lookup_doi` lowercases, URL-decodes and strips publisher
    # fragments — same form the SQL REPLACE below targets.
    dois = [d for d in (canonical_lookup_doi((r or {}).get("doi")) for r in results) if d]

    if work_ids or dois:
        try:
            params: list[object] = []
            clauses: list[str] = []
            if work_ids:
                placeholders = ",".join("?" * len([w for w in work_ids if w]))
                if placeholders:
                    clauses.append(f"lower(trim(openalex_id)) IN ({placeholders})")
                    params.extend([w.lower() for w in work_ids if w])
            if dois:
                placeholders = ",".join("?" * len(dois))
                clauses.append(f"lower(trim(REPLACE(REPLACE(doi, 'https://doi.org/', ''), 'http://doi.org/', ''))) IN ({placeholders})")
                params.extend(dois)
            if clauses:
                rows = db.execute(
                    f"SELECT id, openalex_id, doi, status, rating FROM papers WHERE {' OR '.join(clauses)}",
                    params,
                ).fetchall()
                for row in rows:
                    row_oaid = str(row["openalex_id"] or "").strip().lower()
                    row_doi = canonical_lookup_doi(row["doi"]) or ""
                    payload = {
                        "local_paper_id": row["id"],
                        "local_status": row["status"],
                        "local_rating": int(row["rating"] or 0),
                    }
                    if row_oaid:
                        already[row_oaid] = payload
                    if row_doi:
                        already[f"doi:{row_doi}"] = payload
        except Exception:
            already = {}

    # Inline the already-in-db lookup onto each result row.
    annotated = []
    for r, wid, doi in zip(
        results,
        work_ids,
        [canonical_lookup_doi((x or {}).get("doi")) or "" for x in results],
    ):
        hit = already.get((wid or "").lower()) or (already.get(f"doi:{doi}") if doi else None)
        r = dict(r)
        if hit:
            r["already_in_db"] = True
            r["local_paper_id"] = hit["local_paper_id"]
            r["local_status"] = hit["local_status"]
            r["local_rating"] = hit["local_rating"]
        else:
            r["already_in_db"] = False
        annotated.append(r)

    return {
        "results": annotated,
        "next_cursor": page.get("next_cursor"),
        "total": page.get("total"),
        "openalex_id": openalex_id,
    }


@router.get(
    "/{author_id}/detail",
    summary="Get lightweight author detail for the popup view",
    description="Profile + signal/score + top topics + monitor/backfill state. Excludes the heavier dossier lists (publications, collaborators, history).",
)
def get_author_detail(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        detail = authors_app.get_author_detail(db, author_id)
        if detail is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Author with ID {author_id} not found",
            )
        # The author payload already flows through the AuthorResponse projection
        # via get_author → dict; wrap it for the response envelope.
        detail["author"] = _author_response_from_data(detail["author"]).model_dump()
        return detail
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to retrieve detail for author {author_id}", e)


@router.post(
    "/{author_id}/history-backfill",
    summary="Queue followed-author historical corpus backfill",
)
def history_backfill_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        row = db.execute(
            "SELECT id, name, author_type FROM authors WHERE id = ?",
            (author_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Author not found")
        if str(row["author_type"] or "").strip().lower() != "followed":
            raise HTTPException(status_code=400, detail="Historical backfill is only available for followed authors")
        payload = schedule_followed_author_historical_backfill(author_id, trigger="user")
        if not payload:
            raise HTTPException(status_code=400, detail="Could not queue historical backfill")
        return payload
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to queue historical backfill for author {author_id}", e)


@router.post(
    "/{author_id}/repair",
    summary="Repair author identifiers and refreshability",
)
def repair_author(
    author_id: str,
    background: bool | None = Query(
        default=None,
        description="Run as a background Activity job when the scheduler is enabled",
    ),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    def _run_repair(conn: sqlite3.Connection) -> dict[str, Any]:
        db = conn
        row = db.execute(
            """
            SELECT id, name, openalex_id, scholar_id, orcid, author_type
            FROM authors
            WHERE id = ?
            """,
            (author_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Author not found")

        current_openalex = _norm_oaid(str(row["openalex_id"] or ""))
        current_scholar = str(row["scholar_id"] or "").strip() or None
        current_orcid = str(row["orcid"] or "").strip() or None
        current_name = str(row["name"] or "").strip() or None
        repaired_fields: list[str] = []
        resolution_notes: list[str] = []

        before_state = {
            "openalex_id": current_openalex,
            "scholar_id": current_scholar,
            "orcid": current_orcid,
            "name": current_name,
        }
        settings = _id_resolution_settings()
        resolution = resolve_author_identity(
            db,
            author_id=author_id,
            author_name=current_name,
            openalex_id=current_openalex,
            scholar_id=current_scholar,
            orcid=current_orcid,
            sample_titles=_get_author_sample_titles(db, author_id, limit=6),
            use_semantic_scholar=settings["semantic_scholar_enabled"],
            use_orcid=settings["orcid_enabled"],
            use_scholar_scrape_auto=settings["scholar_scrape_auto_enabled"],
        )

        resolved_openalex = _norm_oaid(str(resolution.openalex_id or ""))
        if resolved_openalex:
            duplicate = db.execute(
                "SELECT id, name FROM authors WHERE lower(openalex_id) = lower(?) AND id != ?",
                (resolved_openalex, author_id),
            ).fetchone()
            if duplicate:
                duplicate_id = duplicate["id"] if isinstance(duplicate, sqlite3.Row) else duplicate[0]
                duplicate_name = duplicate["name"] if isinstance(duplicate, sqlite3.Row) else duplicate[1]
                with write_section(db, label="repair_author duplicate"):
                    _set_resolution_status(
                        db,
                        author_id,
                        "needs_manual_review",
                        f"duplicate openalex_id {resolved_openalex} — already assigned to '{duplicate_name}' ({duplicate_id})",
                    )
                return {
                    "author_id": author_id,
                    "repaired_fields": [],
                    "openalex_id": current_openalex,
                    "scholar_id": current_scholar,
                    "refreshed": False,
                    "refresh_result": None,
                    "status": "needs_manual_review",
                    "resolution": {
                        "status": resolution.status,
                        "reason": summarize_author_resolution(resolution),
                        "confidence": resolution.confidence,
                    },
                }

        # Persist the resolution in its own gated unit BEFORE the refresh:
        # _refresh_author_cache_impl self-gates and rolls back any uncommitted
        # state on entry, so the resolution must already be committed (network
        # for the resolution happened above, outside this section).
        with write_section(db, label="repair_author apply resolution"):
            _apply_author_resolution_result(db, author_id, resolution)
        after_state = {
            "openalex_id": _norm_oaid(str(resolution.openalex_id or current_openalex or "")) or None,
            "scholar_id": str(resolution.scholar_id or current_scholar or "").strip() or None,
            "orcid": str(resolution.orcid or current_orcid or "").strip() or None,
            "name": str(resolution.author_name or current_name or "").strip() or None,
        }
        for field_name in ("openalex_id", "scholar_id", "orcid", "name"):
            if (before_state.get(field_name) or None) != (after_state.get(field_name) or None):
                repaired_fields.append(field_name)

        current_openalex = after_state["openalex_id"]
        current_scholar = after_state["scholar_id"]
        if resolution.reason:
            resolution_notes.append(summarize_author_resolution(resolution))

        refreshed = False
        refresh_result: dict | None = None
        if current_openalex and str(row["author_type"] or "") == "followed":
            try:
                refresh_result = _refresh_author_cache_impl(db, author_id, mode="incremental")
                refreshed = True
            except Exception as exc:
                resolution_notes.append(f"refresh_failed:{type(exc).__name__}")

        # Terminal status write — gated unit (the refresh above ran on a clean
        # connection and committed its own writes).
        with write_section(db, label="repair_author final status"):
            if repaired_fields:
                _set_resolution_status(
                    db,
                    author_id,
                    resolution.status if resolution.status in _RESOLUTION_STATUSES else "resolved_auto",
                    "; ".join(note for note in resolution_notes if note) or "Repaired author identifiers",
                )
            elif current_openalex:
                _set_resolution_status(
                    db,
                    author_id,
                    resolution.status if resolution.status in _RESOLUTION_STATUSES else "resolved_manual",
                    "; ".join(note for note in resolution_notes if note) or "Author already refreshable",
                )
            else:
                _set_resolution_status(
                    db,
                    author_id,
                    "needs_manual_review",
                    "; ".join(note for note in resolution_notes if note) or "Automatic repair could not resolve a refreshable bridge",
                )

        return {
            "author_id": author_id,
            "repaired_fields": repaired_fields,
            "openalex_id": current_openalex,
            "scholar_id": current_scholar,
            "refreshed": refreshed,
            "refresh_result": refresh_result,
            "status": "completed" if repaired_fields or refreshed else ("needs_manual_review" if not current_openalex else "noop"),
            "resolution": {
                "status": resolution.status,
                "reason": summarize_author_resolution(resolution),
                "confidence": resolution.confidence,
            },
        }

    try:
        if not background_mode_requested(background):
            return _run_repair(db)

        row = db.execute("SELECT name FROM authors WHERE id = ?", (author_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Author not found")

        from alma.api.scheduler import (
            activity_envelope,
            add_job_log,
            find_active_job,
            schedule_immediate,
            set_job_status,
        )

        author_name = str(row["name"] or author_id)
        operation_key = f"authors.repair:{normalize_author_id(author_id) or author_id}"
        existing = find_active_job(operation_key)
        if existing:
            return activity_envelope(
                str(existing.get("job_id") or ""),
                status="already_running",
                operation_key=operation_key,
                message=f"Author repair already running for {author_name}",
            )

        job_id = f"author_repair_{uuid.uuid4().hex[:10]}"
        queued_message = f"Queued author repair for {author_name}"
        set_job_status(
            job_id,
            status="queued",
            operation_key=operation_key,
            trigger_source="user",
            started_at=utcnow().isoformat(),
            message=queued_message,
        )
        add_job_log(job_id, queued_message, step="queued", data={"author_id": author_id})

        def _runner() -> None:
            conn = open_db_connection()
            try:
                result = _run_repair(conn)
                raw_status = str(result.get("status") or "").strip().lower()
                final_status = raw_status if raw_status in {"completed", "noop", "failed", "cancelled"} else (
                    "completed" if result.get("repaired_fields") or result.get("refreshed") else "noop"
                )
                final_message = (
                    "Author repair completed"
                    if final_status == "completed"
                    else "Author repair finished with no changes"
                )
                set_job_status(
                    job_id,
                    status=final_status,
                    finished_at=utcnow().isoformat(),
                    message=final_message,
                    result=result,
                    operation_key=operation_key,
                    trigger_source="user",
                )
            except HTTPException as exc:
                detail = str(exc.detail or "Author repair failed")
                add_job_log(job_id, detail, level="ERROR", step="failed")
                set_job_status(
                    job_id,
                    status="failed",
                    finished_at=utcnow().isoformat(),
                    message=detail,
                    error=detail,
                    operation_key=operation_key,
                    trigger_source="user",
                )
            except Exception as exc:
                add_job_log(job_id, f"Author repair failed: {exc}", level="ERROR", step="failed")
                set_job_status(
                    job_id,
                    status="failed",
                    finished_at=utcnow().isoformat(),
                    message=f"Author repair failed: {exc}",
                    error=str(exc),
                    operation_key=operation_key,
                    trigger_source="user",
                )
            finally:
                conn.close()

        schedule_immediate(job_id, _runner)
        return activity_envelope(
            job_id,
            status="queued",
            operation_key=operation_key,
            message=queued_message,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to repair author {author_id}", e)


@router.post(
    "/deep-refresh-all",
    summary="Deep refresh all refreshable authors",
    description=(
        "Full refresh pipeline for every selected author: hierarchical "
        "identity resolution → OpenAlex profile update (name, affiliation, "
        "institutions, citations, h-index, works_count, topics, "
        "cited_by_year, ORCID) → works + SPECTER2 vectors backfill → "
        "SPECTER2 centroid recompute. Same code the popup 'Refresh "
        "author' button runs — just iterated over the author pool. "
        "Imported-only placeholder authors without any upstream ID are "
        "skipped automatically."
    ),
)
def deep_refresh_all_authors(
    scope: str = Query(
        "followed",
        description="followed | needs_metadata | followed_plus_library | library | corpus",
    ),
    background: bool = Query(True, description="Run as background job and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Deep refresh every author matching ``scope``.

    Scope:
    - ``followed`` (default) — rows in ``followed_authors``.
    - ``needs_metadata`` — active authors with identity-resolution
      issues, followed authors missing OpenAlex, or OpenAlex-backed
      profiles missing ORCID/profile fields.
    - ``followed_plus_library`` — followed authors PLUS every co-author
      of any paper currently in the library. Surfaces the
      adjacent-author signal Discovery uses, without sweeping the long
      tail of orphaned placeholder rows.
    - ``library`` — every co-author of any saved library paper (drops
      followed-only authors who have no library paper).
    - ``corpus`` — every active author row. Available via API for
      power use; not exposed in the Settings UI.
    """
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    scope_value = (scope or "followed").strip().lower()
    if scope_value not in {"library", "followed", "needs_metadata", "followed_plus_library", "corpus"}:
        scope_value = "followed"

    if not background:
        try:
            return _deep_refresh_all_impl(db, scope=scope_value, limit=_FULL_SWEEP_BUDGET)
        except Exception as e:
            raise_internal("Deep refresh all failed", e)

    # One active job per (operation, scope) — a fast library-only sweep
    # shouldn't block a concurrent corpus-wide run (or vice versa).
    operation_key = f"authors.deep_refresh_all:{scope_value}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Deep refresh-all already running (scope={scope_value})",
        )

    job_id = f"authors_deep_refresh_all_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=utcnow().isoformat(),
        message=f"Queued deep refresh for all authors (scope={scope_value})",
    )
    add_job_log(
        job_id,
        f"Queued deep refresh for all authors (scope={scope_value})",
        step="queued",
    )

    def _runner() -> dict:
        conn = open_db_connection()
        try:
            return _deep_refresh_all_impl(conn, job_id=job_id, scope=scope_value, limit=_FULL_SWEEP_BUDGET)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message=f"Queued deep refresh for all authors (scope={scope_value})",
    )


@router.get(
    "/{author_id}/affiliations",
    summary="List affiliation evidence for one author",
    description="Read-only evidence rows used to pick an author's display affiliation.",
)
def get_author_affiliations(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.services.author_hydrate import list_author_affiliations

    payload = list_author_affiliations(db, author_id)
    if not payload.get("found", True):
        raise HTTPException(status_code=404, detail="Author not found")
    return payload


class PickAffiliationRequest(BaseModel):
    institution_name: str = Field(..., description="Institution to display for this author")
    institution_openalex_id: str | None = None
    institution_ror: str | None = None


@router.post(
    "/{author_id}/affiliation",
    summary="Lock an author's display affiliation to a chosen institution",
    description=(
        "Terminal resolution for the 'affiliation evidence disagrees across "
        "sources' needs-attention flag (the `pick_affiliation` action). Records "
        "the choice as an authoritative manual evidence row that outranks every "
        "auto source and survives future refreshes, so the display affiliation "
        "sticks and the conflict clears for good. Gated write + Activity row."
    ),
)
def pick_author_affiliation(
    author_id: str,
    req: PickAffiliationRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.application.author_affiliation import record_manual_affiliation

    name = (req.institution_name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="institution_name is required")
    if not db.execute("SELECT 1 FROM authors WHERE id = ?", (author_id,)).fetchone():
        raise HTTPException(status_code=404, detail="Author not found")

    # Network-free, single write unit (gate + BEGIN IMMEDIATE + lock retry).
    decision = run_write_unit(
        db,
        lambda: record_manual_affiliation(
            db,
            author_id,
            institution_name=name,
            institution_openalex_id=(req.institution_openalex_id or None),
            institution_ror=(req.institution_ror or None),
        ),
        label="pick_affiliation",
    )
    _log_author_action(
        db,
        "pick_affiliation",
        f"Set display affiliation to {name}",
        author_id=author_id,
        data={"institution_name": name, "conflict": decision.conflict},
    )
    return {
        "author_id": author_id,
        "affiliation": decision.selected_affiliation,
        "conflict": decision.conflict,
    }


@router.post(
    "/{author_id}/accept-unidentified",
    summary="Accept an author as unidentifiable (terminal — stops needs-attention flagging)",
    description=(
        "Terminal acknowledgment for an identity gap the resolver genuinely "
        "can't close (no_match / needs_manual_review / error / followed-without-"
        "id). Sets id_resolution_status='dismissed' so the author stops "
        "surfacing in needs-attention and the author-health counts — WITHOUT "
        "inventing an identifier. The user can still resolve it later by pasting "
        "an id (which flips it to resolved_manual). Gated write + Activity row."
    ),
)
def accept_author_unidentified(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    row = db.execute("SELECT name FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    now = utcnow().isoformat()

    def _persist():
        db.execute(
            "UPDATE authors SET id_resolution_status = 'dismissed', "
            "id_resolution_updated_at = ? WHERE id = ?",
            (now, author_id),
        )

    run_write_unit(db, _persist, label="accept_unidentified")
    _log_author_action(
        db,
        "accept_unidentified",
        f"Accepted {row['name'] or author_id} as unidentifiable",
        author_id=author_id,
    )
    return {"author_id": author_id, "id_resolution_status": "dismissed"}


@router.delete(
    "/{author_id}/accept-unidentified",
    summary="Undo accepting an author as unidentifiable",
    description="Reopens the identity gap: the author is `unresolved` and back on Needs attention.",
)
def reopen_author_identity(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    row = db.execute("SELECT name FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    now = utcnow().isoformat()

    def _persist():
        db.execute(
            "UPDATE authors SET id_resolution_status = 'unresolved', "
            "id_resolution_updated_at = ? WHERE id = ? AND id_resolution_status = 'dismissed'",
            (now, author_id),
        )

    run_write_unit(db, _persist, label="reopen_author_identity")
    _log_author_action(
        db,
        "reopen_identity",
        f"Reopened identity of {row['name'] or author_id}",
        author_id=author_id,
    )
    return {"author_id": author_id, "id_resolution_status": "unresolved"}


@router.post(
    "/{author_id}/identity-profile-refresh",
    summary="Refresh author identity/profile",
    description=(
        "Refreshes identity and profile metadata only. Does not paginate works, "
        "fetch S2 vectors, or recompute the centroid unless the resolved identity changes."
    ),
)
def identity_profile_refresh_author(
    author_id: str,
    background: bool = Query(True, description="Run as background job and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Cheap author repair path for needs-attention retry/profile refresh."""
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    if not background:
        try:
            return _refresh_author_identity_profile_impl(db, author_id)
        except HTTPException:
            raise
        except Exception as e:
            raise_internal(f"Identity/profile refresh failed for author {author_id}", e)

    row = db.execute("SELECT name FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    author_name = row["name"] or author_id
    operation_key = f"authors.identity_profile_refresh:{normalize_author_id(author_id)}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Identity/profile refresh already running for {author_name}",
        )

    job_id = f"author_identity_profile_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=utcnow().isoformat(),
        message=f"Refreshing author identity/profile: {author_name}",
    )
    add_job_log(job_id, f"Queued identity/profile refresh for {author_name}", step="queued")

    def _runner() -> dict:
        conn = open_db_connection()
        try:
            return _refresh_author_identity_profile_impl(conn, author_id, job_id=job_id)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message=f"Queued identity/profile refresh for {author_name}",
        extra={
            "success": True,
            "author_id": author_id,
            "count": None,
            "new_count": 0,
            "mode": "identity_profile",
        },
    )


@router.post(
    "/{author_id}/deep-refresh",
    summary="Deep refresh author (full re-fetch)",
    description="Full re-fetch of all publications ignoring last_fetched_at. Use sparingly.",
)
def deep_refresh_author(
    author_id: str,
    background: bool = Query(True, description="Run as background job and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Deep refresh: full re-fetch of all publications ignoring ``last_fetched_at``.

    This always uses the configured ``from_year`` from settings (not the
    last-fetched timestamp), ensuring a complete re-fetch of the author's publication history.
    """
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    if not background:
        try:
            return _refresh_author_cache_impl(db, author_id, mode="deep")
        except HTTPException:
            raise
        except Exception as e:
            raise_internal(f"Deep refresh failed for author {author_id}", e)

    row = db.execute("SELECT name FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    author_name = row["name"] or author_id
    operation_key = f"authors.deep_refresh:{normalize_author_id(author_id)}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Deep refresh already running for {author_name}",
        )

    job_id = f"author_deep_refresh_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=utcnow().isoformat(),
        message=f"Deep refreshing author: {author_name}",
    )
    add_job_log(job_id, f"Queued deep refresh for {author_name}", step="queued")

    def _runner() -> dict:
        conn = open_db_connection()
        try:
            return _refresh_author_cache_impl(conn, author_id, mode="deep", job_id=job_id)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message=f"Queued deep refresh for {author_name}",
    )

