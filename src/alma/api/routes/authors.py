"""Author management API endpoints."""

import json
import logging
import os
import sqlite3
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional
from datetime import datetime
from types import SimpleNamespace
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from difflib import SequenceMatcher

from alma.application import authors as authors_app
from alma.application.followed_authors import (
    apply_follow_state,
    ensure_followed_author_contract,
    resolve_canonical_author_id,
    schedule_followed_author_historical_backfill,
)
from alma.api.models import (
    AuthorCreate,
    AuthorFollowFromPaperRequest,
    AuthorFollowFromPaperResponse,
    AuthorResponse,
    AuthorSuggestionResponse,
    ErrorResponse,
    SavePublicationsRequest,
)
from alma.api.deps import get_db, get_current_user, normalize_author_id, open_db_connection
from alma.api.deps import _data_dir, _db_path  # internal helpers for path resolution
from alma.core.backend import fetch_publications_by_id, _settings as _fb_settings
from alma.core.utils import normalize_orcid
from alma.config import get_db_path, get_fetch_year
from alma.config import get_all_settings
from alma.api.models import PublicationResponse
from alma.plugins.config import load_plugin_config
from alma.plugins.registry import get_global_registry
from alma.plugins.slack import SlackPlugin
from alma.plugins.base import Publication
from alma.plugins.helpers import get_slack_plugin
from alma.openalex.client import upsert_papers as _upsert_pubs
from alma.openalex.client import resolve_openalex_candidates_from_scholar as _resolve_oa
from alma.openalex.client import _normalize_openalex_author_id as _norm_oaid
from alma.core.utils import derive_source_id, to_publication_dataclass
from alma.core.identifier_resolution import (
    resolve_scholar_candidates_from_sources,
    scholar_url_for_id,
)
from alma.core.resolution import (
    get_author_sample_titles as _shared_get_author_sample_titles,
    resolve_author_identity,
    resolve_paper_openalex_work,
    summarize_author_resolution,
)
from alma.core.operations import OperationOutcome, OperationRunner
from alma.core.redaction import redact_sensitive_text
from alma.api.helpers import background_mode_requested, raise_internal

logger = logging.getLogger(__name__)

# Backward-compatible patch target retained for identifier-resolution tests.
_resolve_oa_meta = _resolve_oa

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


_RESOLUTION_STATUSES = {
    "unresolved",
    "resolved_auto",
    "resolved_manual",
    "needs_manual_review",
    "no_match",
    "error",
}


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


def _sync_follow_state(db: sqlite3.Connection, author_id: str, *, followed: bool) -> None:
    """Keep followed_authors, authors.author_type, and feed_monitors in sync.

    Thin wrapper around the canonical ``apply_follow_state`` helper so every
    follow/unfollow flow funnels through one place and no surface sees
    drift between the three tables.
    """
    apply_follow_state(db, author_id, followed=followed)


def _id_resolution_settings() -> dict[str, bool]:
    """Identifier resolution toggles from settings.json (with safe defaults)."""
    cfg = get_all_settings()
    return {
        "semantic_scholar_enabled": bool(cfg.get("id_resolution_semantic_scholar_enabled", True)),
        "orcid_enabled": bool(cfg.get("id_resolution_orcid_enabled", True)),
        "scholar_scrape_auto_enabled": bool(cfg.get("id_resolution_scholar_scrape_auto_enabled", False)),
        "scholar_scrape_manual_enabled": bool(cfg.get("id_resolution_scholar_scrape_manual_enabled", True)),
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


def _ensure_author_resolution_columns(db: sqlite3.Connection) -> None:
    """Ensure author identifier resolution columns exist."""
    try:
        cols = [row[1] for row in db.execute("PRAGMA table_info(authors)").fetchall()]
        if "openalex_id" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN openalex_id TEXT")
        if "orcid" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN orcid TEXT")
        if "scholar_id" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN scholar_id TEXT")
        if "affiliation" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN affiliation TEXT")
        if "email_domain" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN email_domain TEXT")
        if "citedby" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN citedby INTEGER DEFAULT 0")
        if "h_index" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN h_index INTEGER DEFAULT 0")
        if "interests" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN interests TEXT")
        if "url_picture" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN url_picture TEXT")
        if "works_count" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN works_count INTEGER DEFAULT 0")
        if "last_fetched_at" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN last_fetched_at TEXT")
        if "cited_by_year" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN cited_by_year TEXT")
        if "institutions" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN institutions TEXT")
        if "added_at" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN added_at TEXT")
        if "author_type" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN author_type TEXT DEFAULT 'background'")
        if "id_resolution_status" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN id_resolution_status TEXT")
        if "id_resolution_reason" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN id_resolution_reason TEXT")
        if "id_resolution_updated_at" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN id_resolution_updated_at TEXT")
        # Phase D (2026-04-24) hierarchical resolver columns. These let the
        # UI surface "resolved via ORCID" vs "needs manual review" with a
        # calibrated confidence score instead of a boolean flag.
        if "id_resolution_method" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN id_resolution_method TEXT")
        if "id_resolution_confidence" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN id_resolution_confidence REAL")
        if "id_resolution_evidence" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN id_resolution_evidence TEXT")
        # Soft-removal lifecycle (2026-04-26) — mirrors papers.status
        # (D3): 'active' rows are visible to refresh / scope queries;
        # 'removed' rows stay in the table so Discovery can read them
        # as a negative signal but are filtered out of bulk refresh and
        # the canonical author list. Default 'active' so all pre-
        # existing rows keep their old behaviour.
        if "status" not in cols:
            db.execute("ALTER TABLE authors ADD COLUMN status TEXT DEFAULT 'active'")
            db.execute("UPDATE authors SET status = 'active' WHERE status IS NULL")
    except Exception:
        pass


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
    params.append(result.status)
    updates.append("id_resolution_reason = ?")
    params.append(summarize_author_resolution(result))
    updates.append("id_resolution_updated_at = ?")
    params.append(datetime.utcnow().isoformat())

    params.append(author_id)
    db.execute(f"UPDATE authors SET {', '.join(updates)} WHERE id = ?", tuple(params))


def _normalize_text(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _parse_interests(raw: Any) -> Optional[List[str]]:
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


def _resolve_scholar_candidates_from_local(author_name: str, sample_titles: list[str]) -> list[dict]:
    """Best-effort Scholar candidate resolution from name + sample titles."""
    try:
        from scholarly import scholarly as _sch
    except Exception:
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
    openalex_id: Optional[str] = None,
    orcid: Optional[str] = None,
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
    if mode == "manual":
        if settings["scholar_scrape_manual_enabled"]:
            from_local = _resolve_scholar_candidates_from_local(author_name, sample_titles)
    elif mode == "auto":
        if settings["scholar_scrape_auto_enabled"]:
            from_local = _resolve_scholar_candidates_from_local(author_name, sample_titles)

    return _merge_scholar_candidates(from_api, from_local)


def _pick_top_candidate(candidates: list[dict], min_score: float, min_margin: float) -> Optional[dict]:
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
    db.execute(
        """
        UPDATE authors
        SET id_resolution_status = ?, id_resolution_reason = ?, id_resolution_updated_at = ?
        WHERE id = ?
        """,
        (status_norm, reason[:1000], datetime.utcnow().isoformat(), author_id),
    )


def _collect_preprint_presence_for_openalex(openalex_id: str) -> dict:
    """Inspect an OpenAlex author's works and summarize arXiv/bioRxiv presence."""
    from alma.openalex.client import fetch_works_for_author

    oid = _norm_oaid(openalex_id)
    if not oid:
        return {"arxiv_count": 0, "biorxiv_count": 0}

    arxiv_count = 0
    biorxiv_count = 0
    try:
        works = fetch_works_for_author(oid, from_year=None)
    except Exception:
        works = []

    for w in works or []:
        doi = ((w or {}).get("doi") or "").lower()
        journal = ((w or {}).get("journal") or "").lower()
        url = ((w or {}).get("pub_url") or "").lower()
        if "arxiv" in journal or "arxiv.org" in url or doi.startswith("10.48550/arxiv."):
            arxiv_count += 1
        if "biorxiv" in journal or "biorxiv.org" in url or doi.startswith("10.1101/"):
            biorxiv_count += 1

    return {"arxiv_count": arxiv_count, "biorxiv_count": biorxiv_count}


def _auto_resolve_openalex_from_scholar(scholar_id: str) -> Optional[str]:
    """Best-effort auto-resolution of OpenAlex ID from Scholar ID."""
    if not (scholar_id or "").strip():
        return None
    try:
        cands = _resolve_oa(scholar_id.strip())
    except Exception:
        return None
    top = _pick_top_candidate(cands, min_score=3.0, min_margin=1.0)
    if not top:
        return None
    return _norm_oaid(str(top.get("openalex_id") or top.get("id") or ""))


def _auto_resolve_scholar_from_openalex(
    openalex_id: str,
    *,
    orcid: Optional[str] = None,
) -> Optional[dict]:
    """Best-effort Scholar ID resolution from OpenAlex via API bridges first."""
    from alma.openalex.client import fetch_author_profile, fetch_works_for_author

    oid = _norm_oaid(openalex_id)
    if not oid:
        return None
    try:
        profile = fetch_author_profile(oid) or {}
        name = (profile.get("name") or "").strip()
    except Exception:
        name = ""

    if not name:
        try:
            from alma.openalex.client import get_author_name_by_id
            name = (get_author_name_by_id(oid) or "").strip()
        except Exception:
            name = ""

    if not name:
        return None

    sample_titles: list[str] = []
    try:
        works = fetch_works_for_author(oid, from_year=None)
        for w in works[:8]:
            title = ((w or {}).get("title") or "").strip()
            if title:
                sample_titles.append(title)
    except Exception:
        pass

    effective_orcid = (orcid or "").strip() or None
    if not effective_orcid:
        try:
            effective_orcid = (profile.get("orcid") or "").strip() or None
        except Exception:
            effective_orcid = None

    cands = _resolve_scholar_candidates(
        name,
        sample_titles,
        openalex_id=oid,
        orcid=effective_orcid,
        mode="auto",
    )
    top = _pick_top_candidate(cands, min_score=5.5, min_margin=1.5)
    if not top:
        return None
    sid = (str(top.get("scholar_id") or "").strip() or None)
    if not sid:
        return None
    return {
        "scholar_id": sid,
        "source": (str(top.get("source") or "unknown").strip() or "unknown"),
        "score": float(top.get("score") or 0.0),
    }


def _refresh_author_cache_impl(
    db: sqlite3.Connection,
    author_id: str,
    *,
    mode: str,
    job_id: Optional[str] = None,
    profile_cache: Optional[Dict[str, Dict[str, Any]]] = None,
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

    _ensure_author_resolution_columns(db)

    if job_id and is_cancellation_requested(job_id):
        set_job_status(
            job_id,
            status="cancelled",
            finished_at=datetime.utcnow().isoformat(),
            message="Author refresh cancelled before execution",
        )
        add_job_log(job_id, "Cancelled before fetch start", step="cancelled")
        return {"success": False, "author_id": author_id, "mode": mode, "cancelled": True}

    requested_author_id = str(author_id or "").strip()
    cursor = db.execute(
        "SELECT name, openalex_id, scholar_id, orcid, last_fetched_at FROM authors WHERE id=?",
        (requested_author_id,),
    )
    row = cursor.fetchone()
    resolved_author_id = requested_author_id
    if not row:
        fallback_author_id = resolve_canonical_author_id(
            db,
            requested_author_id,
            create_if_missing=True,
            fallback_name=requested_author_id,
        )
        if fallback_author_id:
            cursor = db.execute(
                "SELECT name, openalex_id, scholar_id, orcid, last_fetched_at FROM authors WHERE id=?",
                (fallback_author_id,),
            )
            row = cursor.fetchone()
            if row:
                resolved_author_id = str(fallback_author_id or "").strip() or requested_author_id
                if job_id and resolved_author_id != requested_author_id:
                    add_job_log(
                        job_id,
                        f"Recovered author refresh target from {requested_author_id} to canonical author {resolved_author_id}",
                        step="refresh_recover_author",
                        data={"requested_author_id": requested_author_id, "resolved_author_id": resolved_author_id},
                    )
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")

    author_id = resolved_author_id

    author_name = row["name"]
    last_fetched = row["last_fetched_at"]

    # Phase D (2026-04-24) — hierarchical identity resolver with explicit
    # tier + confidence + preprint triangulation. Layers over the existing
    # `resolve_author_identity` cascade. The result is persisted with the
    # new id_resolution_method / id_resolution_confidence columns so the
    # Settings UI can surface "resolved via ORCID" vs "needs manual review".
    from alma.application.author_identity import (
        persist_identity_result,
        resolve_identity_hierarchical,
    )

    _id_settings = _id_resolution_settings()
    hierarchical = resolve_identity_hierarchical(
        db,
        author_id=author_id,
        author_name=str(author_name or "").strip() or None,
        openalex_id=str(row["openalex_id"] or "").strip() or None,
        scholar_id=str(row["scholar_id"] or "").strip() or None,
        orcid=str(row["orcid"] or "").strip() or None,
        use_semantic_scholar=_id_settings["semantic_scholar_enabled"],
        use_orcid=_id_settings["orcid_enabled"],
        use_preprints=_id_settings["semantic_scholar_enabled"],
    )
    persist_identity_result(db, author_id, hierarchical)
    db.commit()

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

    # Legacy resolver kept for backward-compat evidence display on the
    # dossier page. The hierarchical bundle is the source of truth for
    # the identity columns; the legacy result contributes `id_resolution_reason`.
    resolution = resolve_author_identity(
        db,
        author_id=author_id,
        author_name=str(author_name or "").strip() or None,
        openalex_id=hierarchical.openalex_id or str(row["openalex_id"] or "").strip() or None,
        scholar_id=hierarchical.scholar_id or str(row["scholar_id"] or "").strip() or None,
        orcid=hierarchical.orcid or str(row["orcid"] or "").strip() or None,
        use_semantic_scholar=_id_settings["semantic_scholar_enabled"],
        use_orcid=_id_settings["orcid_enabled"],
    )
    _apply_author_resolution_result(db, author_id, resolution)
    db.commit()
    openalex_id = hierarchical.openalex_id or resolution.openalex_id or (str(row["openalex_id"] or "").strip() or None)
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
    backfill_profile: Optional[dict] = None
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

    now = datetime.utcnow().isoformat()
    db.execute("UPDATE authors SET last_fetched_at = ? WHERE id = ?", (now, author_id))
    db.commit()

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
    from alma.application.author_profile import (
        apply_author_profile_update,
        refresh_author_centroid_safe,
    )

    if openalex_id:
        try:
            # Profile-source preference (cheapest first):
            #   1. `backfill_profile` — already paid for inside Phase 1
            #      of `refresh_author_works_and_vectors` (and that path
            #      itself prefers the pre-batched cache, so no double
            #      hit either way).
            #   2. `profile_cache` — bulk pre-flight via
            #      `batch_get_author_profiles`. Used when modern
            #      backfill didn't run (e.g. Scholar fallback path).
            #   3. fresh `fetch_author_profile` — final fallback.
            cache_hit = None
            if isinstance(profile_cache, dict) and profile_cache and openalex_id:
                cache_hit = profile_cache.get(_norm_oaid(openalex_id))
            if backfill_profile is not None:
                profile = backfill_profile
            elif cache_hit is not None:
                profile = cache_hit
            else:
                from alma.openalex.client import fetch_author_profile

                profile = fetch_author_profile(openalex_id)
            profile_summary = apply_author_profile_update(db, author_id, profile)
            if job_id:
                add_job_log(
                    job_id,
                    f"Profile refresh for {author_name}: updated {', '.join(profile_summary.get('updated') or ['nothing'])}",
                    step="profile_refresh",
                    data=profile_summary,
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

    centroid_refreshed = refresh_author_centroid_safe(db, openalex_id)
    if db.in_transaction:
        db.commit()
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


def _deep_refresh_all_impl(
    db: sqlite3.Connection,
    *,
    job_id: Optional[str] = None,
    scope: str = "corpus",
) -> dict:
    """Run full deep refresh for all refreshable authors.

    DRY contract (2026-04-24): the per-author iteration funnels through
    `_refresh_author_cache_impl` — the **exact** pipeline the popup's
    "Refresh author" button uses. That runs (1) hierarchical identity
    resolution + persistence of method/confidence/evidence, (2) legacy
    resolver for dossier evidence, (3) works + SPECTER2 vectors backfill
    via `refresh_author_works_and_vectors` (modern path) with Scholar
    fallback, (4) profile refresh via `apply_author_profile_update`
    (name, affiliation, institutions, citedby, h_index, works_count,
    interests, cited_by_year, orcid), (5) author centroid recompute.

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

    scope = (scope or "followed").strip().lower()
    if scope not in {"library", "followed", "needs_metadata", "followed_plus_library", "corpus"}:
        scope = "followed"

    scope_join_map: dict[str, str] = {
        # All authors of any paper currently in the user's library.
        "library": (
            "INNER JOIN publication_authors pa ON lower(pa.openalex_id) = lower(a.openalex_id) "
            "INNER JOIN papers p ON p.id = pa.paper_id AND p.status = 'library'"
        ),
        # Explicitly followed authors only.
        "followed": "INNER JOIN followed_authors fa ON fa.author_id = a.id",
        # Targeted metadata repair: rows where identity/profile state is
        # incomplete or failed, without sweeping every placeholder row.
        "needs_metadata": "",
        # Default scope used by the Settings UI: union of followed +
        # every author of every library paper. Captures the
        # "adjacent-author" signal (co-authors of papers I've kept)
        # that Discovery uses to surface related work, without sweeping
        # the long tail of placeholder rows that live only in the
        # corpus scope.
        "followed_plus_library": (
            "INNER JOIN ("
            "  SELECT fa.author_id AS id FROM followed_authors fa"
            "  UNION"
            "  SELECT a2.id FROM authors a2"
            "  INNER JOIN publication_authors pa2 ON lower(trim(pa2.openalex_id)) = lower(trim(a2.openalex_id))"
            "  INNER JOIN papers p2 ON p2.id = pa2.paper_id AND p2.status = 'library'"
            ") sub ON sub.id = a.id"
        ),
        # Every active author row. Available via API but no longer
        # surfaced in the Settings UI — see lifecycle decision
        # 2026-04-26 (soft-removed authors stay in the table for
        # Discovery's negative-signal reads but are filtered out of
        # bulk refresh / scope queries here).
        "corpus": "",
    }
    scope_join = scope_join_map[scope]
    needs_metadata_clause = """
        AND (
            COALESCE(a.id_resolution_status, '') IN ('error', 'no_match', 'needs_manual_review')
            OR (
                COALESCE(a.id_resolution_status, '') = 'unresolved'
                AND EXISTS (SELECT 1 FROM followed_authors fa WHERE fa.author_id = a.id)
            )
            OR (
                EXISTS (SELECT 1 FROM followed_authors fa WHERE fa.author_id = a.id)
                AND COALESCE(NULLIF(TRIM(a.openalex_id), ''), '') = ''
            )
            OR (
                COALESCE(NULLIF(TRIM(a.openalex_id), ''), '') != ''
                AND (
                    COALESCE(NULLIF(TRIM(a.orcid), ''), '') = ''
                    OR COALESCE(NULLIF(TRIM(a.affiliation), ''), '') = ''
                    OR COALESCE(NULLIF(TRIM(a.interests), ''), '') = ''
                    OR COALESCE(NULLIF(TRIM(a.institutions), ''), '') = ''
                    OR COALESCE(NULLIF(TRIM(a.cited_by_year), ''), '') = ''
                    OR COALESCE(a.works_count, 0) <= 0
                    OR COALESCE(NULLIF(TRIM(a.last_fetched_at), ''), '') = ''
                )
            )
        )
    """ if scope == "needs_metadata" else ""
    rows = db.execute(
        f"""
        SELECT DISTINCT a.id AS id, a.name AS name,
               a.openalex_id AS openalex_id, a.scholar_id AS scholar_id
        FROM authors a
        {scope_join}
        WHERE COALESCE(a.status, 'active') <> 'removed'
        {needs_metadata_clause}
        ORDER BY a.name
        """
    ).fetchall()
    total = len(rows)
    if total == 0:
        if job_id:
            add_job_log(job_id, f"No authors found for deep refresh (scope={scope})", step="preflight")
        return {"success": True, "total": 0, "refreshed": 0, "skipped": 0, "failed": 0, "failures": [], "scope": scope}

    refreshed = 0
    skipped = 0
    failed = 0
    failures: list[dict] = []
    processed = 0

    if job_id:
        add_job_log(
            job_id,
            f"Deep refresh all started for {total} authors (scope={scope})",
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
    # serialises every worker behind us.
    if db.in_transaction:
        db.commit()

    # Pre-classify rows. Only truly malformed rows (empty id) are
    # skipped up-front; import-only placeholders without upstream IDs
    # are *kept* so the inner identity-resolution cascade gets a
    # chance to find their OpenAlex ID by searching by name across the
    # papers we already have for them (`resolve_identity_hierarchical`
    # → `resolve_openalex_candidates_from_metadata`, which queries
    # OpenAlex works by title and inspects each work's `authorships`
    # to match the placeholder's display name).
    work_items: list[tuple[str, str, Optional[str]]] = []
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
    profile_cache: Dict[str, Dict[str, Any]] = {}
    cache_oids = [oid for _, _, oid in work_items if oid]
    if cache_oids:
        try:
            from alma.openalex.client import batch_get_author_profiles

            t_pre = datetime.utcnow()
            profile_cache = batch_get_author_profiles(cache_oids, batch_size=50, max_workers=4)
            if job_id:
                add_job_log(
                    job_id,
                    (
                        f"Pre-fetched {len(profile_cache)}/{len(cache_oids)} author profiles "
                        f"in batched pipe-filter calls "
                        f"({(datetime.utcnow() - t_pre).total_seconds():.1f}s)"
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

    def _run_one_author(author_id: str, author_name: str) -> tuple[str, str, str, Optional[Exception], bool]:
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
            # hierarchical identity resolve → legacy resolver → works
            # backfill (OpenAlex or Scholar fallback) → profile update →
            # centroid recompute. Runs on the worker's own connection;
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

    with ThreadPoolExecutor(
        max_workers=_DEEP_REFRESH_MAX_WORKERS,
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
                finished_at=datetime.utcnow().isoformat(),
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

    if db.in_transaction:
        db.commit()
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
    response_model=List[AuthorResponse],
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
        _ensure_author_resolution_columns(db)
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
        _ensure_author_resolution_columns(db)
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
    response_model=List[AuthorSuggestionResponse],
    summary="List collaborator and adjacent author suggestions",
)
def list_author_suggestions(
    limit: int = Query(default=5, ge=1, le=30),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        _ensure_author_resolution_columns(db)
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

    try:
        record_missing_author_remove(
            db, req.openalex_id, suggestion_bucket=req.suggestion_bucket
        )
        db.commit()
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

    try:
        record_followed_from_suggestion(
            db, req.openalex_id, req.suggestion_bucket
        )
        db.commit()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as e:
        raise_internal("Failed to log followed suggestion", e)


# ----------------------------------------------------------------------
# D12 Phase C — refresh the two network-backed suggestion buckets
# ----------------------------------------------------------------------

class RefreshAuthorNetworkRequest(BaseModel):
    """Trigger the two network author-suggestion refresh runners.

    `force=True` re-runs both even if their caches are fresh; by
    default stale/missing caches are the only ones refreshed
    (fresh caches are left alone — cheap no-op).
    """

    force: bool = Field(default=False, description="Refresh even when cache is fresh")


@router.post(
    "/suggestions/refresh-network",
    summary="Refresh OpenAlex + S2 author-suggestion caches (Activity-envelope)",
    description=(
        "Enqueues up to two Activity-envelope jobs, one per network "
        "source, that populate the `author_suggestion_cache` table. "
        "Sources whose cache is still fresh are skipped. Each job "
        "writes its own envelope; the response aggregates both."
    ),
)
def refresh_author_suggestion_network(
    req: RefreshAuthorNetworkRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )
    from alma.application import author_network

    # Route thread does ONLY cheap work: check if each source's cache
    # slot is fresh. Heavy seed selection lives inside the runners.
    jobs: list[dict] = []
    for source, runner_name in (
        (author_network.SOURCE_OPENALEX_RELATED, "refresh_openalex_related_network"),
        (author_network.SOURCE_S2_RELATED, "refresh_s2_related_network"),
    ):
        if not req.force and not author_network.is_cache_stale(db, source):
            jobs.append({
                "source": source,
                "status": "fresh",
                "job_id": None,
                "operation_key": None,
                "message": f"{source} cache is fresh; skipped",
            })
            continue

        operation_key = f"authors.refresh_network:{source}"
        existing = find_active_job(operation_key)
        if existing:
            jobs.append({
                "source": source,
                "status": "already_running",
                "job_id": str(existing.get("job_id") or ""),
                "operation_key": operation_key,
                "message": "Already running",
            })
            continue

        job_id = f"author_netrefresh_{uuid.uuid4().hex[:10]}"
        queued_msg = f"Queued {source} refresh"
        set_job_status(
            job_id,
            status="queued",
            operation_key=operation_key,
            trigger_source="user",
            started_at=datetime.utcnow().isoformat(),
            processed=0,
            total=0,
            message=queued_msg,
        )
        add_job_log(job_id, queued_msg, step="queued")

        def _make_runner(src: str, runner_fn_name: str, job_id_local: str, op_key_local: str):
            def _runner():
                from alma.api.deps import _db_path

                class _ShimCtx:
                    def log_step(self, step, *, message=None, processed=None, total=None, **_):
                        set_job_status(
                            job_id_local,
                            status="running",
                            message=message,
                            processed=processed,
                            total=total,
                        )

                try:
                    set_job_status(job_id_local, status="running", message=f"Refreshing {src}")
                    runner = getattr(author_network, runner_fn_name)
                    summary = runner(_db_path(), ctx=_ShimCtx())
                    seeds_total = int(summary.get("seeds") or 0)
                    set_job_status(
                        job_id_local,
                        status="completed",
                        processed=seeds_total,
                        total=seeds_total,
                        finished_at=datetime.utcnow().isoformat(),
                        message=(
                            f"{src} refresh completed · "
                            f"{int(summary.get('candidates') or 0)} candidates"
                        ),
                        result=summary,
                    )
                except Exception as exc:
                    add_job_log(
                        job_id_local, f"{src} refresh failed: {exc}",
                        level="ERROR", step="failed",
                    )
                    set_job_status(
                        job_id_local,
                        status="failed",
                        finished_at=datetime.utcnow().isoformat(),
                        message=f"{src} refresh failed: {exc}",
                        error=str(exc),
                    )
            return _runner

        schedule_immediate(
            job_id,
            _make_runner(source, runner_name, job_id, operation_key),
        )
        jobs.append({
            "source": source,
            "status": "queued",
            "job_id": job_id,
            "operation_key": operation_key,
            "message": queued_msg,
        })

    return {"jobs": jobs}


# ----------------------------------------------------------------------
# D12 Phase B — corpus author works + SPECTER2 backfill
# ----------------------------------------------------------------------

class BackfillAuthorWorksRequest(BaseModel):
    """Trigger a full works + vectors backfill for one author or the batch.

    When `author_openalex_id` is null the runner enumerates every
    resolved corpus author whose `author_centroids` row is missing or
    older than 14 days. Set `full_refetch=True` to bypass the
    `local >= declared` shortcut on per-author runs — useful when
    OpenAlex reshuffles counts and the shortcut would cache stale
    coverage.
    """

    author_openalex_id: Optional[str] = Field(
        default=None, description="Single author OpenAlex id, or null for the full batch"
    )
    full_refetch: bool = Field(
        default=False,
        description="Bypass the 'local paper count >= declared works_count' skip shortcut",
    )
    limit: Optional[int] = Field(
        default=None, ge=1, le=10000,
        description="Batch runs only: cap the number of authors processed this job",
    )


@router.post(
    "/backfill-works",
    summary="Backfill full works + SPECTER2 vectors for corpus authors (Activity-envelope)",
    description=(
        "Fetches every OpenAlex work for the target author(s), upserts papers + "
        "publication_authors rows, batch-fetches missing SPECTER2 vectors from "
        "Semantic Scholar, then recomputes the author_centroids entry."
    ),
)
def backfill_author_works(
    req: BackfillAuthorWorksRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        is_cancellation_requested,
        schedule_immediate,
        set_job_status,
    )

    target = str(req.author_openalex_id or "").strip()
    if target:
        # Single-author validation: author must exist in our corpus.
        row = db.execute(
            "SELECT 1 FROM authors WHERE lower(openalex_id) = ? LIMIT 1",
            (target.lower(),),
        ).fetchone()
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Author with OpenAlex id {target} not found in corpus",
            )
        operation_key = f"authors.backfill:{target.lower()}"
        queued_msg = f"Queued backfill for author {target}"
    else:
        operation_key = "authors.backfill:all"
        queued_msg = "Queued backfill for all resolved authors"

    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Author backfill already running for this target",
        )

    job_id = f"author_backfill_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        processed=0,
        total=0,
        message=queued_msg,
    )
    add_job_log(job_id, queued_msg, step="queued")

    def _runner():
        from alma.api.deps import _db_path
        from alma.application import author_backfill

        class _ShimCtx:
            """Forward log_step calls to set_job_status so the Activity row
            advances as phases progress (pattern from lessons.md
            `lenses.refresh_lens._LogCtx`)."""

            def log_step(self, step, *, message=None, processed=None, total=None, **_):
                set_job_status(
                    job_id,
                    status="running",
                    message=message,
                    processed=processed,
                    total=total,
                )

        db_path = _db_path()
        shim = _ShimCtx()
        try:
            set_job_status(job_id, status="running", message=queued_msg)
            if target:
                summary = author_backfill.refresh_author_works_and_vectors(
                    db_path, target, ctx=shim, full_refetch=req.full_refetch,
                )
                total = 1
                processed = 1
            else:
                summary = author_backfill.backfill_all_resolved_authors(
                    db_path,
                    ctx=shim,
                    limit=req.limit,
                    is_cancellation_requested=lambda: is_cancellation_requested(job_id),
                )
                total = int(summary.get("total") or 0)
                processed = int(summary.get("processed") or 0)
            set_job_status(
                job_id,
                status="completed",
                processed=processed,
                total=total,
                finished_at=datetime.utcnow().isoformat(),
                message="Author backfill completed",
                result=summary,
            )
        except Exception as exc:
            add_job_log(job_id, f"Backfill failed: {exc}", level="ERROR", step="failed")
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.utcnow().isoformat(),
                message=f"Author backfill failed: {exc}",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=queued_msg,
    )


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
        from alma.openalex.client import fetch_author_profile, upsert_work_sidecars

        _ensure_author_resolution_columns(db)
        ensure_followed_author_contract(db)

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

        authorship = _best_publication_author_match(
            db,
            paper_id=payload.paper_id,
            author_name=requested_author_name,
        )
        matched_via = "paper_authorship" if authorship else "resolution"
        if authorship is None:
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
        if not primary_id:
            raise HTTPException(
                status_code=422,
                detail="Could not resolve a stable author identity from this paper author",
            )

        status_value = resolution.status if resolution.status != "no_match" else "resolved_manual"
        reason_value = summarize_author_resolution(resolution) or "Author followed from paper"
        now_iso = datetime.utcnow().isoformat()
        existing = _find_existing_author_row(
            db,
            author_id=primary_id,
            openalex_id=resolved_openalex,
            scholar_id=resolved_scholar,
            orcid=resolved_orcid,
        )

        created = False
        author_id = primary_id
        if existing:
            author_id = str(existing["id"] or "").strip() or primary_id
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
            created = True
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
                    matched_name or requested_author_name or primary_id,
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
            author_id = primary_id

        if resolved_openalex:
            try:
                profile = fetch_author_profile(_norm_oaid(resolved_openalex))
                if profile:
                    db.execute(
                        """
                        UPDATE authors SET
                            affiliation = COALESCE(?, affiliation),
                            citedby = ?,
                            h_index = ?,
                            interests = ?,
                            works_count = ?,
                            orcid = COALESCE(?, orcid)
                        WHERE id = ?
                        """,
                        (
                            profile.get("affiliation"),
                            profile.get("citedby", 0),
                            profile.get("h_index", 0),
                            json.dumps(profile.get("interests")) if profile.get("interests") else None,
                            profile.get("works_count", 0),
                            normalize_orcid(profile.get("orcid")),
                            author_id,
                        ),
                    )
            except Exception as exc:
                logger.debug("OpenAlex profile enrichment failed during paper-author follow for %s: %s", author_id, exc)

        already_followed = db.execute(
            "SELECT 1 FROM followed_authors WHERE author_id = ? LIMIT 1",
            (author_id,),
        ).fetchone() is not None
        _sync_follow_state(db, author_id, followed=True)

        try:
            feedback_author_id = _norm_oaid(resolved_openalex) if resolved_openalex else author_id
            if feedback_author_id:
                from alma.application.gap_radar import clear_missing_author_feedback

                clear_missing_author_feedback(db, feedback_author_id)
        except Exception:
            pass

        db.commit()
        if not already_followed:
            try:
                schedule_followed_author_historical_backfill(author_id, trigger="paper_author_follow")
            except Exception as exc:
                logger.debug("Could not queue historical backfill for %s: %s", author_id, exc)

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
        from alma.openalex.client import fetch_author_profile, get_author_name_by_id
        _ensure_author_resolution_columns(db)

        scholar_id = (author.scholar_id or "").strip() or None
        openalex_id = normalize_author_id((author.openalex_id or "").strip()) or None
        orcid = (getattr(author, "orcid", None) or "").strip() or None
        provided_name = (getattr(author, "name", None) or "").strip() or None
        if not scholar_id and not openalex_id and not orcid:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Provide one of: scholar_id, openalex_id, or orcid")

        settings = _id_resolution_settings()
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

        resolved_openalex = resolution.openalex_id or (_norm_oaid(openalex_id) if openalex_id else None)
        resolved_scholar = resolution.scholar_id or scholar_id
        resolved_orcid = resolution.orcid or orcid
        primary_id = resolved_scholar or resolved_openalex or resolved_orcid
        if not primary_id:
            raise HTTPException(status_code=404, detail="Could not resolve a stable author identity")

        existing = db.execute("SELECT name FROM authors WHERE id = ?", (primary_id,)).fetchone()
        if existing:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Author with ID {primary_id} already exists")
        if resolved_openalex:
            existing_oa = db.execute(
                "SELECT id, name FROM authors WHERE lower(openalex_id) = lower(?)",
                (_norm_oaid(resolved_openalex),),
            ).fetchone()
            if existing_oa and (existing_oa["id"] if isinstance(existing_oa, sqlite3.Row) else existing_oa[0]) != primary_id:
                existing_id = existing_oa["id"] if isinstance(existing_oa, sqlite3.Row) else existing_oa[0]
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Author with OpenAlex ID {_norm_oaid(resolved_openalex)} already exists (id={existing_id})",
                )

        name = (
            provided_name
            or resolution.author_name
            or (get_author_name_by_id(resolved_openalex) if resolved_openalex else None)
            or primary_id
        )
        now_iso = datetime.utcnow().isoformat()
        status_value = resolution.status if resolution.status != "no_match" else "resolved_manual"
        reason_value = summarize_author_resolution(resolution) or "Author created"

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

        if resolved_openalex:
            try:
                profile = fetch_author_profile(_norm_oaid(resolved_openalex))
                if profile:
                    db.execute(
                        """
                        UPDATE authors SET
                            affiliation = COALESCE(?, affiliation),
                            citedby = ?,
                            h_index = ?,
                            interests = ?,
                            works_count = ?,
                            orcid = COALESCE(?, orcid)
                        WHERE id = ?
                        """,
                        (
                            profile.get("affiliation"),
                            profile.get("citedby", 0),
                            profile.get("h_index", 0),
                            json.dumps(profile.get("interests")) if profile.get("interests") else None,
                            profile.get("works_count", 0),
                            normalize_orcid(profile.get("orcid")),
                            primary_id,
                        ),
                    )
            except Exception as exc:
                logger.warning("OpenAlex profile enrichment failed during author creation for '%s': %s", primary_id, exc)

        _sync_follow_state(db, primary_id, followed=True)
        db.commit()
        try:
            schedule_followed_author_historical_backfill(primary_id, trigger="author_create")
        except Exception as exc:
            logger.debug("Could not queue historical backfill for %s: %s", primary_id, exc)
        logger.info(f"Added author: {name} ({primary_id})")

        runner = OperationRunner(db)

        def _op(_ctx):
            return OperationOutcome(
                status="completed",
                message=f"Author created: {name}",
                result={"author_id": primary_id, "name": name},
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
        raise_internal("Failed to add author", e)


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
    _ensure_author_resolution_columns(db)

    # Severity ordering: error > no_match > needs_manual_review >
    # unresolved/followed-but-no-openalex. Lower number = surface first.
    ranked = db.execute(
        """
        SELECT
            a.id AS author_id,
            a.name AS author_name,
            COALESCE(a.openalex_id, '') AS openalex_id,
            COALESCE(a.id_resolution_status, '') AS status,
            COALESCE(a.id_resolution_method, '') AS method,
            COALESCE(a.id_resolution_confidence, 0.0) AS confidence,
            COALESCE(a.id_resolution_reason, '') AS reason,
            COALESCE(a.id_resolution_updated_at, a.last_fetched_at, '') AS updated_at,
            CASE
                WHEN COALESCE(a.id_resolution_status, '') = 'error' THEN 0
                WHEN COALESCE(a.id_resolution_status, '') = 'no_match' THEN 1
                WHEN COALESCE(a.id_resolution_status, '') = 'needs_manual_review' THEN 2
                WHEN EXISTS (
                    SELECT 1 FROM followed_authors fa WHERE fa.author_id = a.id
                ) AND COALESCE(a.openalex_id, '') = '' THEN 3
                ELSE 9
            END AS severity
        FROM authors a
        WHERE COALESCE(a.id_resolution_status, '') IN ('error', 'no_match', 'needs_manual_review', 'unresolved')
           OR (
                EXISTS (SELECT 1 FROM followed_authors fa WHERE fa.author_id = a.id)
                AND COALESCE(a.openalex_id, '') = ''
              )
        ORDER BY severity ASC, updated_at DESC, a.name ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    # ── Split-profile detection ─────────────────────────────────────
    # Same human, multiple OpenAlex IDs followed/saved. Names are
    # NFKD-folded + lowercased + punctuation-stripped (same canonical
    # form the suggestion-rail dedup uses) so "Müller" / "MÜLLER" /
    # "Muller" collapse, but "Müller" / "Mueller" stay distinct.
    # We only flag clusters where the user has already followed ≥2
    # of the profiles — surfacing every author table cluster would
    # be too noisy on a fresh import.
    from alma.application.authors import _normalize_author_display_name

    cluster_rows = db.execute(
        """
        SELECT a.id, a.name, a.openalex_id, a.id_resolution_updated_at, a.last_fetched_at
        FROM authors a
        JOIN followed_authors fa ON fa.author_id = a.id
        WHERE COALESCE(a.openalex_id, '') <> ''
        """
    ).fetchall()

    clusters: dict[str, list[sqlite3.Row]] = {}
    for cr in cluster_rows:
        name_key = _normalize_author_display_name(str(cr["name"] or ""))
        if not name_key:
            continue
        clusters.setdefault(name_key, []).append(cr)
    split_profiles: list[dict] = []
    for name_key, members in clusters.items():
        if len(members) < 2:
            continue
        # Pick a primary: most recent activity, then alphabetical id.
        members_sorted = sorted(
            members,
            key=lambda r: (
                str(r["id_resolution_updated_at"] or r["last_fetched_at"] or ""),
                str(r["id"]),
            ),
            reverse=True,
        )
        primary = members_sorted[0]
        alts = [
            {
                "author_id": str(r["id"]),
                "openalex_id": str(r["openalex_id"]),
                "display_name": str(r["name"] or ""),
            }
            for r in members_sorted[1:]
        ]
        primary_name = str(primary["name"] or "").strip() or str(primary["id"])
        primary_oid = str(primary["openalex_id"] or "")
        split_profiles.append(
            {
                "author_id": str(primary["id"]),
                "author_name": primary_name,
                "openalex_id": primary_oid or None,
                "status": "split_profiles",
                "method": None,
                "confidence": 0.0,
                "reason_code": "split_profiles",
                "reason": f"Multiple OpenAlex profiles for the same name ({len(members)} found)",
                "reason_detail": (
                    f"Primary: {primary_oid}; alternates: "
                    + ", ".join(a["openalex_id"] for a in alts)
                ),
                "alt_profiles": alts,
                "suggested_action": {
                    "code": "review_profiles",
                    "label": "Review profiles",
                    "hint": (
                        "Same person split across multiple OpenAlex IDs. "
                        "Mark them as duplicates to merge follow + corpus, "
                        "or confirm they're different humans to dismiss."
                    ),
                },
                "updated_at": str(
                    primary["id_resolution_updated_at"]
                    or primary["last_fetched_at"]
                    or ""
                )
                or None,
            }
        )

    def _action_for(status: str, has_openalex: bool) -> dict[str, str]:
        """Map the row shape to a user-facing action."""
        if status == "error":
            return {"code": "retry_refresh", "label": "Retry refresh",
                    "hint": "The last refresh hit an exception. Try again."}
        if status == "no_match":
            return {"code": "manual_search", "label": "Search OpenAlex",
                    "hint": "Automatic search returned zero hits. Open the author card and paste an OpenAlex / ORCID manually."}
        if status == "needs_manual_review":
            return {"code": "review_candidates", "label": "Review candidates",
                    "hint": "Multiple OpenAlex candidates scored too close to each other. Pick the right one."}
        if not has_openalex:
            return {"code": "resolve_now", "label": "Resolve IDs",
                    "hint": "Followed author with no OpenAlex identifier. Run resolve to link one."}
        return {"code": "refresh", "label": "Refresh",
                "hint": "Re-run the full refresh pipeline."}

    def _reason_text(status: str, raw_reason: str) -> str:
        """Friendly user-facing summary."""
        if raw_reason:
            return raw_reason
        if status == "error":
            return "Last refresh raised an exception."
        if status == "no_match":
            return "OpenAlex found no author matching this name."
        if status == "needs_manual_review":
            return "Automatic candidates were too close to decide."
        return "Identifier not yet resolved."

    def _detail_text(
        status: str,
        method: str,
        confidence: float,
        has_oa: bool,
    ) -> Optional[str]:
        """Concrete second-line context: which resolver step ran, how
        confidently, whether an ID at least exists. The frontend renders
        this under the main reason so the user can see *what failed
        and how* before clicking the action button.

        Examples:
        - "Last attempted: openalex_search · confidence 0.42 — too low to auto-pick"
        - "ORCID resolved (A5012345678) but OpenAlex still missing"
        - "No resolver step has run yet"
        """
        bits: list[str] = []
        if method:
            bits.append(f"Last step: {method}")
        if confidence > 0.0:
            bits.append(f"confidence {confidence:.2f}")
        if status == "needs_manual_review" and confidence > 0.0:
            bits.append("too low to auto-pick")
        if has_oa and status in ("error", "no_match"):
            bits.append("OpenAlex ID is set; refresh failed against it")
        if not bits:
            return "No resolver step has run yet" if status == "unresolved" else None
        return " · ".join(bits)

    out: list[dict] = []
    for row in ranked:
        status = str(row["status"] or "")
        openalex_id = str(row["openalex_id"] or "").strip()
        has_oa = bool(openalex_id)
        method = str(row["method"] or "")
        confidence = float(row["confidence"] or 0.0)
        action = _action_for(status, has_oa)
        out.append(
            {
                "author_id": str(row["author_id"]),
                "author_name": str(row["author_name"] or row["author_id"]),
                "openalex_id": openalex_id or None,
                "status": status or "unresolved",
                "method": method or None,
                "confidence": confidence,
                "reason_code": action["code"],
                "reason": _reason_text(status, str(row["reason"] or "")),
                "reason_detail": _detail_text(status, method, confidence, has_oa),
                "suggested_action": action,
                "updated_at": str(row["updated_at"] or "") or None,
            }
        )

    # Merge conflicts — emitted when a previous merge_author_profiles
    # call had a hard-identifier conflict (orcid / scholar_id /
    # semantic_scholar_id) where both rows held a different non-null
    # value. The merge proceeded (primary's value won), but the alt's
    # value is preserved here so the user can review and decide which
    # is correct. Surfaces ABOVE split_profiles because conflicts
    # carry concrete evidence of a likely data-integrity issue.
    from alma.application.author_merge import list_unresolved_conflicts

    merge_conflicts: list[dict] = []
    for c in list_unresolved_conflicts(db):
        primary_name = str(c.get("primary_name") or c.get("primary_author_id") or "")
        field = str(c.get("field") or "")
        primary_val = str(c.get("primary_value") or "")
        alt_val = str(c.get("alt_value") or "")
        merge_conflicts.append(
            {
                "author_id": str(c.get("primary_author_id") or ""),
                "author_name": primary_name,
                "openalex_id": c.get("primary_openalex_id"),
                "status": "merge_conflict",
                "method": None,
                "confidence": 0.0,
                "reason_code": "merge_conflict",
                "reason": f"Merge kept conflicting {field}",
                "reason_detail": (
                    f"Primary: {primary_val} · merged-in alt "
                    f"({c.get('alt_openalex_id')}) had {alt_val}. "
                    "Pick which value to keep."
                ),
                "conflict_id": str(c.get("id") or ""),
                "conflict_field": field,
                "conflict_primary_value": primary_val,
                "conflict_alt_value": alt_val,
                "alt_openalex_id": c.get("alt_openalex_id"),
                "suggested_action": {
                    "code": "resolve_conflict",
                    "label": "Resolve",
                    "hint": (
                        "Two profiles got merged but disagreed on a "
                        "hard identifier. Confirm which value should "
                        "stick — or dismiss if both are wrong."
                    ),
                },
                "updated_at": str(c.get("created_at") or "") or None,
            }
        )

    # Split-profile clusters surface above the existing rows (severity
    # bucket "split" sits above the unresolved bucket but below true
    # errors/no_match — they are actionable but not blocking like a
    # missing identifier on a followed author).
    from alma.application.author_affiliation import list_affiliation_conflicts

    affiliation_conflicts: list[dict] = []
    for c in list_affiliation_conflicts(db, limit=limit):
        first = c.get("first") or {}
        second = c.get("second") or {}
        first_name = str(first.get("institution_name") or "")
        second_name = str(second.get("institution_name") or "")
        affiliation_conflicts.append(
            {
                "author_id": str(c.get("author_id") or ""),
                "author_name": str(c.get("author_name") or ""),
                "openalex_id": c.get("openalex_id"),
                "status": "affiliation_conflict",
                "method": None,
                "confidence": 0.0,
                "reason_code": "affiliation_conflict",
                "reason": "Affiliation evidence disagrees across sources",
                "reason_detail": (
                    f"Top evidence points to {first_name}; "
                    f"near-tie evidence points to {second_name}."
                ),
                "selected_affiliation": c.get("selected_affiliation"),
                "suggested_action": {
                    "code": "pick_affiliation",
                    "label": "Review affiliation",
                    "hint": (
                        "Open the evidence list and choose which "
                        "institution should be displayed for this author."
                    ),
                },
                "updated_at": str(first.get("observed_at") or second.get("observed_at") or "") or None,
            }
        )

    out = merge_conflicts + affiliation_conflicts + split_profiles + out
    return {"total": len(out), "items": out}


class SetIdentifiersRequest(BaseModel):
    """Manual paste of authoritative identifiers for an author who
    failed automatic resolution. Any subset is accepted; absent fields
    are left unchanged. ORCID + OpenAlex + Scholar IDs are normalized
    server-side (URL prefix stripped, lowercased)."""

    orcid: Optional[str] = Field(default=None, description="ORCID iD (with or without https prefix)")
    openalex_id: Optional[str] = Field(default=None, description="OpenAlex author ID (A1234… or full URL)")
    scholar_id: Optional[str] = Field(default=None, description="Google Scholar user id")


@router.get(
    "/enrichment-status",
    summary="Author metadata hydration ledger",
    description="Read-only summary of author profile / affiliation hydration ledger rows.",
)
def get_author_enrichment_status(
    limit: int = Query(100, ge=1, le=500),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.services.author_hydrate import (
        build_author_enrichment_status,
        list_author_enrichment_status_items,
    )

    payload = build_author_enrichment_status(db)
    payload["items"] = list_author_enrichment_status_items(db, limit=limit)
    return payload


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
    _ensure_author_resolution_columns(db)
    row = db.execute("SELECT id FROM authors WHERE id = ?", (author_id,)).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Author not found")

    updates: list[str] = []
    params: list[object] = []

    if body.orcid is not None:
        updates.append("orcid = ?")
        params.append(normalize_orcid(body.orcid))

    if body.openalex_id is not None:
        oid = body.openalex_id.strip()
        # _normalize_oaid handles both bare A123… and full URL forms.
        oid_norm = _norm_oaid(oid) if oid else ""
        updates.append("openalex_id = ?")
        params.append(oid_norm or None)

    if body.scholar_id is not None:
        updates.append("scholar_id = ?")
        params.append(body.scholar_id.strip() or None)

    if not updates:
        raise HTTPException(status_code=400, detail="No identifiers supplied")

    now = datetime.utcnow().isoformat()
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
    db.execute(
        f"UPDATE authors SET {', '.join(updates)} WHERE id = ?",
        params,
    )
    db.commit()

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
    from the needs-attention payload directly."""

    alt_author_ids: List[str] = Field(default_factory=list, min_length=1)


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

    try:
        return merge_author_profiles(db, author_id, body.alt_author_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise_internal(f"Failed to merge profiles into {author_id}", exc)


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

    now = datetime.utcnow().isoformat()
    new_status = ""
    if body.choice == "primary":
        new_status = "resolved_keep_primary"
    elif body.choice == "alt":
        # Overwrite the primary author's column with the alt's value.
        # Field is allowlisted — only hard-identifier columns get into
        # author_merge_conflicts in the first place.
        field = str(row["field"])
        if field not in {"orcid", "scholar_id", "semantic_scholar_id"}:
            raise HTTPException(status_code=400, detail=f"Unknown conflict field: {field}")
        new_value = (
            normalize_orcid(row["alt_value"]) if field == "orcid" else row["alt_value"]
        )
        db.execute(
            f"UPDATE authors SET {field} = ? WHERE id = ?",  # noqa: S608 — allowlist above
            (new_value, row["primary_author_id"]),
        )
        new_status = "resolved_use_alt"
    else:
        new_status = "dismissed"

    db.execute(
        "UPDATE author_merge_conflicts SET status = ?, resolved_at = ? WHERE id = ?",
        (new_status, now, conflict_id),
    )
    db.commit()
    return {
        "conflict_id": conflict_id,
        "status": new_status,
        "primary_author_id": row["primary_author_id"],
    }


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
    from alma.application.author_merge import discover_aliases_via_orcid

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
    summary["primary_author_id"] = author_id
    summary["primary_display_name"] = str(row["name"] or "")
    return summary


@router.get(
    "/{author_id}",
    response_model=AuthorResponse,
    summary="Get author details",
    description="Retrieve detailed information about a specific author.",
)
def get_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Get detailed information about a specific author.

    Args:
        author_id: Google Scholar ID of the author

    Returns:
        AuthorResponse: Author information with publication count

    Raises:
        HTTPException: If author is not found

    Example:
        ```bash
        curl http://localhost:8000/api/v1/authors/abc123xyz
        ```
    """
    try:
        _ensure_author_resolution_columns(db)
        d = authors_app.get_author(db, author_id)
        if d is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Author with ID {author_id} not found",
            )
        return _author_response_from_data(d)

    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to retrieve author {author_id}", e)


@router.delete(
    "/{author_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete an author",
    description="Remove an author and all their cached publications from the system.",
)
def delete_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Delete an author and all their publications.

    Args:
        author_id: Google Scholar ID of the author to delete

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


class ResolveOpenAlexRequest(BaseModel):
    scholar_id: str


class AuthorTypeUpdateRequest(BaseModel):
    author_type: str


@router.patch(
    "/{author_id}/type",
    response_model=AuthorResponse,
    summary="Update author type",
    description="Set author classification: followed or background.",
)
def update_author_type(
    author_id: str,
    body: AuthorTypeUpdateRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
):
    author_type = (body.author_type or "").strip().lower()
    if author_type not in {"followed", "background"}:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="author_type must be 'followed' or 'background'",
        )

    _ensure_author_resolution_columns(db)
    cursor = db.execute("SELECT id FROM authors WHERE id = ?", (author_id,))
    existing = cursor.fetchone()
    if existing is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Author with ID {author_id} not found",
        )
    db.execute(
        "UPDATE authors SET author_type = ? WHERE id = ?",
        (author_type, author_id),
    )
    _sync_follow_state(db, author_id, followed=(author_type == "followed"))
    db.commit()
    if author_type == "followed":
        try:
            schedule_followed_author_historical_backfill(author_id, trigger="author_type_follow")
        except Exception as exc:
            logger.debug("Could not queue historical backfill for %s: %s", author_id, exc)
    d = authors_app.get_author(db, author_id)
    if d is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Author with ID {author_id} not found",
        )
    return _author_response_from_data(d)


@router.post(
    "/resolve-openalex",
    summary="Resolve OpenAlex candidates from a Scholar ID",
    description="Fetch name+affiliation+one work from Scholar, search OpenAlex works and return candidate authors for confirmation.",
)
def resolve_openalex_from_scholar(req: ResolveOpenAlexRequest, user: dict = Depends(get_current_user)):
    try:
        cands = _resolve_oa(req.scholar_id)
        # Normalize response shape for UI
        for c in cands:
            c["openalex_id"] = _norm_oaid(c.get("openalex_id") or c.get("id") or "")
        return {"candidates": cands}
    except Exception as e:
        raise_internal(f"Resolve OpenAlex failed for scholar {req.scholar_id}", e)


class ConfirmOpenAlexRequest(BaseModel):
    openalex_id: str


class ConfirmIdentifiersRequest(BaseModel):
    openalex_id: Optional[str] = None
    scholar_id: Optional[str] = None
    status: str = "resolved_manual"
    reason: Optional[str] = None


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


def _score_authorship_candidates(
    author_name: str,
    authorship_rows: list,
) -> list[dict]:
    """Score OpenAlex authorship candidates from publication_authors data.

    Each row must have: openalex_id, display_name, orcid, institution, pub_count.
    Returns sorted list of candidate dicts with 'score' key.
    """
    name_norm = _normalize_text(author_name)
    if not name_norm:
        return []
    name_parts = name_norm.split()
    name_last = name_parts[-1] if name_parts else ""
    name_first = name_parts[0] if name_parts else ""

    candidates = []
    for row in authorship_rows:
        cand_name = (row["display_name"] if isinstance(row, dict) else row[1]) or ""
        cand_norm = _normalize_text(cand_name)
        if not cand_norm:
            continue
        cand_parts = cand_norm.split()
        cand_last = cand_parts[-1] if cand_parts else ""
        cand_first = cand_parts[0] if cand_parts else ""

        # Name scoring
        if cand_norm == name_norm:
            name_score = 5.0
        elif cand_last == name_last and cand_first == name_first:
            name_score = 4.0
        elif cand_last == name_last and name_first[:1] and cand_first[:1] == name_first[:1]:
            name_score = 3.0
        elif cand_last == name_last:
            name_score = 2.0
        else:
            # Last name doesn't match — skip
            continue

        pub_count = int(row["pub_count"] if isinstance(row, dict) else row[4]) if not isinstance(row, dict) else int(row.get("pub_count", 1))
        # Publication count bonus (more co-authored pubs = higher confidence)
        pub_score = min(3.0, pub_count * 1.0)

        score = name_score + pub_score
        candidates.append({
            "openalex_id": (row["openalex_id"] if isinstance(row, dict) else row[0]) or "",
            "display_name": cand_name,
            "orcid": (row["orcid"] if isinstance(row, dict) else row[2]) or "",
            "institution": (row["institution"] if isinstance(row, dict) else row[3]) or "",
            "score": round(score, 2),
            "pub_count": pub_count,
        })

    candidates.sort(key=lambda x: x["score"], reverse=True)
    return candidates


def _resolve_identifiers_for_author(
    db: sqlite3.Connection,
    author_id: str,
    author_name: str,
) -> dict:
    """Resolve identifiers for a single author via the shared resolution layer."""
    resolution = resolve_author_identity(
        db,
        author_id=author_id,
        author_name=author_name,
        sample_titles=_get_author_sample_titles(db, author_id, limit=4),
        use_semantic_scholar=_id_resolution_settings()["semantic_scholar_enabled"],
        use_orcid=_id_resolution_settings()["orcid_enabled"],
    )
    _apply_author_resolution_result(db, author_id, resolution)
    return {
        "author_id": author_id,
        "status": resolution.status,
        "reason": summarize_author_resolution(resolution),
        "updates": resolution.updates,
        "openalex_candidates": resolution.openalex_candidates[:3],
        "scholar_candidates": resolution.scholar_candidates[:3],
    }


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
        # Release any pending writes from the previous iteration before we
        # block on OpenAlex / Semantic Scholar / ORCID so short user writes
        # don't queue behind an in-flight network call.
        if db.in_transaction:
            db.commit()

        try:
            resolution = resolve_author_identity(
                db,
                author_id=author_id,
                author_name=author_name,
                sample_titles=_get_author_sample_titles(db, author_id, limit=4),
                use_semantic_scholar=_id_resolution_settings()["semantic_scholar_enabled"],
                use_orcid=_id_resolution_settings()["orcid_enabled"],
            )

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

            # Flush this author's writes immediately so the next iteration's
            # remote call doesn't start under an open write transaction.
            if db.in_transaction:
                db.commit()
        except Exception as exc:
            if db.in_transaction:
                db.rollback()
            summary["error"] += 1
            try:
                _set_resolution_status(db, author_id, "error", f"resolver error: {type(exc).__name__}: {exc}")
                db.commit()
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

    if db.in_transaction:
        db.commit()
    summary["elapsed_seconds"] = round(time.monotonic() - t0, 1)
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


@router.post(
    "/{author_id}/confirm-openalex",
    summary="Confirm OpenAlex author for an existing Scholar author",
    description="Attach OpenAlex ID and ORCID to the existing author row.",
)
def confirm_openalex_for_author(
    author_id: str,
    payload: ConfirmOpenAlexRequest,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        # Ensure author exists
        row = db.execute("SELECT name FROM authors WHERE id=?", (author_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Author not found")
        # Fetch author details to retrieve ORCID
        from alma.openalex.client import _get_author_details as _get_oa_det
        # Normalize OpenAlex id to bare key (A...)
        oid = _norm_oaid(payload.openalex_id)
        det = _get_oa_det(oid) or {}
        orcid = (det.get("orcid") or "").strip() or None
        # Ensure columns exist
        try:
            db.execute("ALTER TABLE authors ADD COLUMN openalex_id TEXT")
        except Exception:
            pass
        try:
            db.execute("ALTER TABLE authors ADD COLUMN orcid TEXT")
        except Exception:
            pass
        # Update
        db.execute(
            "UPDATE authors SET openalex_id=?, orcid=COALESCE(?, orcid) WHERE id=?",
            (oid, normalize_orcid(orcid), author_id),
        )
        _set_resolution_status(
            db,
            author_id,
            "resolved_manual",
            f"OpenAlex manually confirmed ({oid})",
        )
        db.commit()
        return {"success": True, "author_id": author_id, "openalex_id": oid, "orcid": orcid}
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Confirm OpenAlex failed for author {author_id}", e)


@router.get(
    "/{author_id}/id-candidates",
    summary="Get identifier candidates for an author",
)
def author_id_candidates(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    _ensure_author_resolution_columns(db)
    row = db.execute("SELECT id, name, openalex_id, orcid FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    titles = _get_author_sample_titles(db, author_id, limit=5)
    settings = _id_resolution_settings()
    resolution = resolve_author_identity(
        db,
        author_id=author_id,
        author_name=str(row["name"] or "").strip() or None,
        openalex_id=str(row["openalex_id"] or "").strip() or None,
        orcid=str(row["orcid"] or "").strip() or None,
        sample_titles=titles,
        use_semantic_scholar=settings["semantic_scholar_enabled"],
        use_orcid=settings["orcid_enabled"],
    )
    scholar_candidates = _resolve_scholar_candidates(
        str(row["name"] or "").strip(),
        titles,
        openalex_id=str(row["openalex_id"] or "").strip() or None,
        orcid=str(row["orcid"] or "").strip() or None,
        mode="auto",
    ) or resolution.scholar_candidates
    return {
        "author_id": author_id,
        "name": row["name"],
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
    _ensure_author_resolution_columns(db)
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
    _ensure_author_resolution_columns(db)
    row = db.execute("SELECT id FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")

    updates: list[str] = []
    params: list[Any] = []
    if payload.openalex_id is not None:
        updates.append("openalex_id = ?")
        params.append(_norm_oaid(payload.openalex_id))
    if payload.scholar_id is not None:
        updates.append("scholar_id = ?")
        params.append((payload.scholar_id or "").strip())
    if updates:
        params.append(author_id)
        db.execute(f"UPDATE authors SET {', '.join(updates)} WHERE id = ?", tuple(params))

    status_value = (payload.status or "resolved_manual").strip().lower()
    if status_value not in _RESOLUTION_STATUSES:
        status_value = "resolved_manual"
    reason = (payload.reason or "Identifiers manually confirmed").strip()
    _set_resolution_status(db, author_id, status_value, reason)
    db.commit()
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
    _ensure_author_resolution_columns(db)
    limit = max(1, min(int(req.limit or 200), 5000))

    scope = (req.scope or "corpus").strip().lower()
    if scope not in {"library", "followed", "corpus"}:
        scope = "corpus"

    scope_join_clauses: dict[str, str] = {
        "library": (
            "INNER JOIN publication_authors pa ON lower(pa.openalex_id) = lower(a.openalex_id) "
            "INNER JOIN papers p ON p.id = pa.paper_id AND p.status = 'library'"
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
        started_at=datetime.utcnow().isoformat(),
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
    response_model=List[dict],
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
        _ensure_author_resolution_columns(db)
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
    dois = [str((r or {}).get("doi") or "").strip().lower() for r in results]
    # Strip https://doi.org/ prefix for comparison.
    dois = [d.replace("https://doi.org/", "").replace("http://doi.org/", "") for d in dois if d]

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
                    row_doi = (
                        str(row["doi"] or "")
                        .strip()
                        .lower()
                        .replace("https://doi.org/", "")
                        .replace("http://doi.org/", "")
                    )
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
    for r, wid, doi in zip(results, work_ids, [str((x or {}).get("doi") or "").strip().lower().replace("https://doi.org/", "").replace("http://doi.org/", "") for x in results]):
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
        _ensure_author_resolution_columns(db)
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


@router.get(
    "/{author_id}/dossier",
    summary="Get author dossier",
    description="Return history, corpus split, topics, venues, collaborators, and recommended next actions for one author.",
)
def get_author_dossier(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    try:
        _ensure_author_resolution_columns(db)
        dossier = authors_app.get_author_dossier(db, author_id)
        if dossier is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Author with ID {author_id} not found",
            )
        return dossier
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Failed to retrieve dossier for author {author_id}", e)


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
        _ensure_author_resolution_columns(db)
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
        _ensure_author_resolution_columns(db)
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
                _set_resolution_status(
                    db,
                    author_id,
                    "needs_manual_review",
                    f"duplicate openalex_id {resolved_openalex} — already assigned to '{duplicate_name}' ({duplicate_id})",
                )
                db.commit()
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

        db.commit()
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

        from alma.api.scheduler import activity_envelope, add_job_log, find_active_job, schedule_immediate, set_job_status

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
            started_at=datetime.utcnow().isoformat(),
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
                    finished_at=datetime.utcnow().isoformat(),
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
                    finished_at=datetime.utcnow().isoformat(),
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
                    finished_at=datetime.utcnow().isoformat(),
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
    "/{author_id}/refresh-cache",
    summary="Refresh author cache (incremental)",
    description="Fetch latest publications since last fetch. Does NOT send notifications.",
)
def refresh_author_cache(
    author_id: str,
    background: bool = Query(True, description="Run as background job and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Refresh cached publications for a specific author without sending notifications.

    Uses ``last_fetched_at`` for incremental fetches when available, falling
    back to the configured ``from_year`` from settings.
    """
    from alma.api.scheduler import activity_envelope, add_job_log, find_active_job, schedule_immediate, set_job_status

    if not background:
        try:
            return _refresh_author_cache_impl(db, author_id, mode="incremental")
        except HTTPException:
            raise
        except Exception as e:
            raise_internal(f"Refresh failed for author {author_id}", e)

    row = db.execute("SELECT name FROM authors WHERE id = ?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    author_name = row["name"] or author_id
    operation_key = f"authors.refresh_cache:{normalize_author_id(author_id)}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message=f"Refresh already running for {author_name}",
        )

    job_id = f"author_refresh_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message=f"Refreshing author cache: {author_name}",
    )
    add_job_log(job_id, f"Queued incremental refresh for {author_name}", step="queued")

    def _runner() -> dict:
        conn = open_db_connection()
        try:
            return _refresh_author_cache_impl(conn, author_id, mode="incremental", job_id=job_id)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message=f"Queued incremental refresh for {author_name}",
        extra={
            "success": True,
            "author_id": author_id,
            "count": None,
            "new_count": None,
            "mode": "incremental",
        },
    )



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
    from alma.api.scheduler import activity_envelope, add_job_log, find_active_job, schedule_immediate, set_job_status

    scope_value = (scope or "followed").strip().lower()
    if scope_value not in {"library", "followed", "needs_metadata", "followed_plus_library", "corpus"}:
        scope_value = "followed"

    if not background:
        try:
            return _deep_refresh_all_impl(db, scope=scope_value)
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
        started_at=datetime.utcnow().isoformat(),
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
            return _deep_refresh_all_impl(conn, job_id=job_id, scope=scope_value)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message=f"Queued deep refresh for all authors (scope={scope_value})",
    )


@router.post(
    "/rehydrate-metadata",
    summary="Hydrate author profile and affiliation metadata",
    description=(
        "Queues an Activity-backed author metadata job. The job uses a "
        "per-author/source/purpose ledger and fills profile fields plus "
        "structured affiliation evidence from OpenAlex, ORCID, Semantic "
        "Scholar, and Crossref when identifiers are available."
    ),
)
def rehydrate_author_metadata(
    limit: Optional[int] = Query(
        None,
        ge=1,
        le=100_000,
        description="Maximum authors to prepare; omitted means all eligible authors",
    ),
    force: bool = Query(False, description="Ignore current ledger state and retry eligible authors"),
    background: bool = Query(True, description="Run in background and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.services.author_hydrate import run_author_metadata_rehydration

    if not background:
        try:
            return run_author_metadata_rehydration(limit=limit, force=force)
        except Exception as e:
            raise_internal("Author metadata hydration failed", e)

    from alma.api.scheduler import activity_envelope, add_job_log, find_active_job, schedule_immediate, set_job_status

    operation_key = "authors.rehydrate_metadata"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Author metadata hydration already running",
        )

    job_id = f"author_metadata_rehydrate_{uuid.uuid4().hex[:10]}"
    total_hint = limit if limit is not None else 0
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        processed=0,
        total=total_hint,
        message=(
            "Author metadata hydration queued"
            if limit is not None
            else "Author metadata hydration queued for all eligible authors"
        ),
    )
    add_job_log(
        job_id,
        "Queued author metadata hydration",
        step="queued",
        data={"limit": limit, "force": force},
    )

    def _runner() -> dict:
        from alma.api.scheduler import add_job_log as _add_log
        from alma.api.scheduler import is_cancellation_requested, set_job_status as _set_status

        return run_author_metadata_rehydration(
            job_id,
            limit=limit,
            force=force,
            set_job_status=_set_status,
            add_job_log=_add_log,
            is_cancellation_requested=is_cancellation_requested,
        )

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message="Queued author metadata hydration",
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


@router.post(
    "/dedup-by-orcid",
    summary="Dedup followed authors by ORCID (auto-merge + alias record)",
    description=(
        "Manual sweep that walks every followed author with an "
        "OpenAlex ID, calls /authors?filter=orcid:X to discover every "
        "split profile sharing the same ORCID, then for each alias: "
        "(a) auto-merges if another currently-followed author already "
        "holds that openalex_id (richer-profile-wins), or (b) records "
        "it in `author_alt_identifiers` so the suggestion rail filters "
        "it out. Activity-enveloped — runs in the background and the "
        "Activity panel shows per-author progress."
    ),
)
def dedup_authors_by_orcid_endpoint(
    background: bool = Query(True, description="Run as background job and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )
    from alma.application.author_merge import dedup_followed_authors_by_orcid

    if not background:
        try:
            return dedup_followed_authors_by_orcid(db)
        except Exception as e:
            raise_internal("Author ORCID dedup sweep failed", e)

    operation_key = "authors.dedup_by_orcid"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Author ORCID dedup sweep already running",
        )

    job_id = f"authors_dedup_orcid_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message="Queued author ORCID dedup sweep",
    )
    add_job_log(job_id, "Queued author ORCID dedup sweep", step="queued")

    def _runner() -> dict:
        conn = open_db_connection()
        try:
            return dedup_followed_authors_by_orcid(conn, job_id=job_id)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message="Queued author ORCID dedup sweep",
    )


@router.post(
    "/garbage-collect-orphans",
    summary="Garbage-collect orphan author rows",
    description=(
        "Soft-removes (status='removed') every author who is (a) not "
        "followed and (b) has no publication_authors row pointing to a "
        "paper in a live state (anything other than 'removed' / "
        "'dismissed'). Mirrors the lifecycle pattern used for papers "
        "(D3): the row stays in the table so Discovery can read it as "
        "a negative signal, but it's filtered out of bulk refresh and "
        "the canonical author list. "
        "Pass dry_run=true to preview without committing."
    ),
)
def garbage_collect_orphan_authors_endpoint(
    dry_run: bool = Query(False, description="Preview without writing"),
    background: bool = Query(True, description="Run as background job and track in Activity"),
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Sweep helper. Eager triggers (paper-remove, unfollow) already
    cover the steady-state cases — this endpoint is for catching up
    with historical drift or for users who want to preview what's
    eligible before pulling the trigger."""
    from alma.api.scheduler import (
        activity_envelope,
        add_job_log,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )
    from alma.application.author_lifecycle import garbage_collect_orphan_authors

    if not background:
        try:
            return garbage_collect_orphan_authors(db, dry_run=dry_run)
        except Exception as e:
            raise_internal("Author GC sweep failed", e)

    operation_key = f"authors.garbage_collect_orphans:{'dry' if dry_run else 'live'}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Author GC sweep already running",
        )

    job_id = f"authors_gc_orphans_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.utcnow().isoformat(),
        message=f"Queued author GC sweep ({'dry-run' if dry_run else 'live'})",
    )
    add_job_log(
        job_id,
        f"Queued author GC sweep (dry_run={dry_run})",
        step="queued",
    )

    def _runner() -> dict:
        conn = open_db_connection()
        try:
            return garbage_collect_orphan_authors(conn, dry_run=dry_run, job_id=job_id)
        finally:
            conn.close()

    schedule_immediate(job_id, _runner)
    return _immediate_job_response(
        job_id,
        operation_key=operation_key,
        queued_message=f"Queued author GC sweep ({'dry-run' if dry_run else 'live'})",
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

    Unlike the incremental ``refresh-cache`` endpoint, this always uses the
    configured ``from_year`` from settings (not the last-fetched timestamp),
    ensuring a complete re-fetch of the author's publication history.
    """
    from alma.api.scheduler import activity_envelope, add_job_log, find_active_job, schedule_immediate, set_job_status

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
        started_at=datetime.utcnow().isoformat(),
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


@router.post(
    "/{author_id}/empty-cache",
    summary="Empty author cache",
    description="Delete all cached publications for the specified author without removing the author.",
)
def empty_author_cache(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Delete all cached publications for an author.

    This does not touch the author entry; it only removes rows from papers.
    """
    try:
        # Ensure the author exists
        row = db.execute("SELECT name FROM authors WHERE id=?", (author_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Author not found")

        # Count and delete papers linked via publication_authors
        oa_row = db.execute(
            "SELECT openalex_id FROM authors WHERE id = ?", (author_id,)
        ).fetchone()
        oa_id = ((oa_row["openalex_id"] if oa_row else None) or "").strip()
        if oa_id:
            before = db.execute(
                """SELECT COUNT(DISTINCT pa.paper_id) AS c
                   FROM publication_authors pa
                   WHERE pa.openalex_id = ?""",
                (oa_id,),
            ).fetchone()["c"]
            # Delete papers only linked to this author, then clean junction rows
            db.execute(
                """DELETE FROM papers WHERE id IN (
                       SELECT pa.paper_id FROM publication_authors pa
                       WHERE pa.openalex_id = ?
                       AND NOT EXISTS (
                           SELECT 1 FROM publication_authors pa2
                           JOIN authors a2 ON a2.openalex_id = pa2.openalex_id
                           WHERE pa2.paper_id = pa.paper_id AND a2.id != ?
                       )
                   )""",
                (oa_id, author_id),
            )
            db.execute(
                "DELETE FROM publication_authors WHERE openalex_id = ?",
                (oa_id,),
            )
        else:
            before = 0
        after = 0
        logger.info("Emptied cache for %s: deleted %d", author_id, before)
        return {"success": True, "author_id": author_id, "deleted": int(before), "remaining": int(after)}
    except HTTPException:
        raise
    except Exception as e:
        raise_internal(f"Empty cache failed for author {author_id}", e)


def _existing_source_ids_for_author(db: sqlite3.Connection, author_id: str) -> set[str]:
    """Return the source-id set of papers already linked to this author.

    Identifier policy: prefer DOI, else URL, else title. Normalizes
    doi.org URL forms so a stored `https://doi.org/...` matches a bare DOI
    returned by the remote fetch.
    """
    try:
        oa_row = db.execute(
            "SELECT openalex_id FROM authors WHERE id = ?", (author_id,)
        ).fetchone()
        oa_id_val = ((oa_row["openalex_id"] if oa_row else None) or "").strip()
        if not oa_id_val:
            return set()
        rows = db.execute(
            """SELECT COALESCE(NULLIF(p.doi, ''), NULLIF(p.source_id, ''), '') AS sid,
                      p.title, p.url
               FROM papers p
               JOIN publication_authors pa ON p.id = pa.paper_id
               WHERE pa.openalex_id = ?""",
            (oa_id_val,),
        ).fetchall()
    except Exception:
        return set()

    existing: set[str] = set()
    for r in rows:
        sid = (r["sid"] or "").strip()
        lower = sid.lower()
        if lower.startswith("https://doi.org/"):
            sid = sid[len("https://doi.org/"):]
        elif lower.startswith("http://doi.org/"):
            sid = sid[len("http://doi.org/"):]
        if sid:
            existing.add(sid)
        else:
            existing.add((r["title"] or "").strip())
    return existing


def _publication_to_dict(author_id: str, p: dict) -> dict:
    """Project a remote-fetch publication dict into the PublicationResponse JSON shape."""
    year_raw = p.get("year")
    try:
        year_int = int(year_raw) if year_raw is not None else None
    except Exception:
        year_int = None
    citations = p.get("num_citations") if p.get("num_citations") is not None else p.get("citations", 0)
    return {
        "author_id": author_id,
        "title": p.get("title") or "",
        "authors": p.get("authors") or "",
        "year": year_int,
        "abstract": p.get("abstract") or p.get("summary"),
        "url": p.get("pub_url") or p.get("url"),
        "citations": int(citations or 0),
        "journal": p.get("journal"),
        "doi": p.get("doi"),
    }


@router.post(
    "/{author_id}/fetch-preview",
    summary="Fetch preview for author (Activity-backed)",
    description="Queue a background job that fetches latest publications for an author "
                "and stores the preview as the job's result. Returns an Activity envelope; "
                "poll `/activity/{job_id}/logs` or read `operation_status.result` on completion.",
)
def fetch_preview_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Schedule a background preview fetch for the given author."""
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    row = db.execute("SELECT name FROM authors WHERE id=?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")

    existing_source_ids = _existing_source_ids_for_author(db, author_id)
    from_year = get_fetch_year()

    operation_key = f"authors.fetch_preview:{author_id}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Preview fetch already running for this author",
        )

    job_id = f"author_preview_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.now().isoformat(),
        message=f"Fetching preview for author {author_id}",
    )

    def _runner():
        try:
            pubs = fetch_publications_by_id(
                author_id,
                output_folder=_data_dir(),
                args=SimpleNamespace(update_cache=False, test_fetching=True),
                from_year=from_year,
            ) or []
            result = [
                _publication_to_dict(author_id, p)
                for p in pubs
                if derive_source_id(p) not in existing_source_ids
            ]
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.now().isoformat(),
                processed=len(result),
                total=len(pubs),
                message=f"Preview ready ({len(result)} new / {len(pubs)} fetched)",
                result={
                    "author_id": author_id,
                    "fetched": len(pubs),
                    "new": len(result),
                    "items": result,
                },
            )
        except Exception as exc:  # pragma: no cover - runner crash path
            logger.error("Preview runner failed for author %s: %s", author_id, exc)
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.now().isoformat(),
                message=f"Preview failed for author {author_id}",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Queued preview fetch for author {author_id}",
    )


@router.post(
    "/{author_id}/preview/save",
    summary="Save preview publications to DB",
    description="Persist selected preview items to the publications database.",
)
def save_preview_publications(
    author_id: str,
    req: SavePublicationsRequest,
    user: dict = Depends(get_current_user),
):
    """Upsert selected preview publications for an author into publications DB."""
    try:
        items = req.items or []
        works = []
        for it in items:
            if it.author_id != author_id:
                continue
            works.append({
                "title": it.title,
                "authors": it.authors or "",
                "abstract": it.abstract or "",
                "year": it.year,
                "pub_url": it.url or "",
                "doi": getattr(it, 'doi', None) or "",
                "num_citations": it.citations or 0,
                "journal": it.journal or "",
            })
        # Persist using the publications DB path resolved from API deps
        from pathlib import Path
        db_path = Path(_db_path())
        # Note: author_id association handled via publication_authors (from authorships)
        count = _upsert_pubs(works, db_path=db_path)
        return {"success": True, "author_id": author_id, "saved": count}
    except Exception as e:
        raise_internal(f"Failed saving preview publications for author {author_id}", e)


@router.post(
    "/{author_id}/fetch-and-send",
    summary="Fetch and send for author (Activity-backed)",
    description="Queue a background job that fetches latest publications for an author "
                "and sends them via the configured Slack plugin. Returns an Activity envelope.",
)
def fetch_and_send_author(
    author_id: str,
    db: sqlite3.Connection = Depends(get_db),
    user: dict = Depends(get_current_user),
):
    """Schedule a background fetch-and-send for the given author."""
    from alma.api.scheduler import (
        activity_envelope,
        find_active_job,
        schedule_immediate,
        set_job_status,
    )

    row = db.execute("SELECT name FROM authors WHERE id=?", (author_id,)).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Author not found")
    author_name = row["name"]

    # Validate Slack plugin synchronously so the caller gets 400 immediately
    # when the integration is not configured.
    try:
        get_slack_plugin(required=True)
    except RuntimeError as exc:
        logger.warning(
            "Fetch & send unavailable for %s: %s",
            author_id,
            redact_sensitive_text(str(exc)),
        )
        raise HTTPException(status_code=400, detail="Slack plugin is not configured") from exc

    from_year = get_fetch_year()
    operation_key = f"authors.fetch_and_send:{author_id}"
    existing = find_active_job(operation_key)
    if existing:
        return activity_envelope(
            str(existing.get("job_id") or ""),
            status="already_running",
            operation_key=operation_key,
            message="Fetch-and-send already running for this author",
        )

    job_id = f"author_fetch_send_{uuid.uuid4().hex[:10]}"
    set_job_status(
        job_id,
        status="queued",
        operation_key=operation_key,
        trigger_source="user",
        started_at=datetime.now().isoformat(),
        message=f"Fetching and sending for {author_name}",
    )

    def _runner():
        try:
            pubs = fetch_publications_by_id(
                author_id,
                output_folder=_data_dir(),
                args=SimpleNamespace(update_cache=False, test_fetching=False),
                from_year=from_year,
            ) or []
            # Re-resolve the plugin inside the worker so config changes between
            # queueing and execution are respected, and so the worker doesn't
            # reuse a plugin instance created on the request thread.
            plugin, config = get_slack_plugin(required=True)
            publications = [to_publication_dataclass(p) for p in pubs]
            message = plugin.format_publications(publications)
            target = config.get("default_channel") or config.get("channel", "")
            ok = plugin.send_message(message, target)
            if not ok:
                raise RuntimeError("Plugin returned failure from send_message")
            logger.info(
                "Fetch & send completed for %s: %d pubs",
                author_id,
                len(publications),
            )
            set_job_status(
                job_id,
                status="completed",
                finished_at=datetime.now().isoformat(),
                processed=len(publications),
                total=len(publications),
                message=f"Sent {len(publications)} publications for {author_name}",
                result={
                    "success": True,
                    "author_id": author_id,
                    "sent_count": len(publications),
                },
            )
        except Exception as exc:  # pragma: no cover - runner crash path
            logger.error(
                "Fetch & send runner failed for author %s: %s", author_id, exc
            )
            set_job_status(
                job_id,
                status="failed",
                finished_at=datetime.now().isoformat(),
                message=f"Fetch & send failed for {author_name}",
                error=str(exc),
            )

    schedule_immediate(job_id, _runner)
    return activity_envelope(
        job_id,
        status="queued",
        operation_key=operation_key,
        message=f"Queued fetch-and-send for {author_name}",
    )
