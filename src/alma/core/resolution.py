"""Shared author and paper resolution backbone.

This module centralizes the identity-matching logic used by:
- author creation and repair
- followed-author historical backfill
- OpenAlex-backed author refresh
- library/import enrichment

The goal is to avoid route-local heuristics and make upstream resolution
deterministic, inspectable, and reusable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
import logging
import re
import sqlite3
from typing import Any, Optional

from alma.core.identifier_resolution import (
    normalize_orcid,
    resolve_scholar_candidates_from_sources,
)
from alma.core.utils import normalize_doi, normalize_text as _normalize_text
from alma.discovery.semantic_scholar import search_papers as search_semantic_papers
from alma.openalex.client import (
    _WORKS_SELECT_FIELDS,
    _normalize_openalex_author_id,
    _normalize_work,
    batch_fetch_works_by_dois,
    batch_fetch_works_by_openalex_ids,
    batch_get_author_details,
    find_author_by_orcid,
    find_author_id_by_name,
    get_author_name_by_id,
    resolve_openalex_candidates_from_metadata,
    resolve_openalex_candidates_from_scholar,
)
from alma.openalex.http import get_client

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ResolutionEvidence:
    source: str
    detail: str
    score: float = 0.0
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AuthorResolutionResult:
    author_name: str | None
    openalex_id: str | None
    scholar_id: str | None
    orcid: str | None
    status: str
    confidence: float
    reason: str
    evidence: list[ResolutionEvidence] = field(default_factory=list)
    openalex_candidates: list[dict[str, Any]] = field(default_factory=list)
    scholar_candidates: list[dict[str, Any]] = field(default_factory=list)
    openalex_profile: dict[str, Any] | None = None

    @property
    def updates(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if self.openalex_id:
            out["openalex_id"] = self.openalex_id
        if self.scholar_id:
            out["scholar_id"] = self.scholar_id
        if self.orcid:
            out["orcid"] = self.orcid
        return out


@dataclass(slots=True)
class PaperResolutionResult:
    work: dict[str, Any] | None
    source: str | None
    reason: str | None
    confidence: float
    evidence: list[ResolutionEvidence] = field(default_factory=list)
    semantic_candidate: dict[str, Any] | None = None


def _pick_top_candidate(candidates: list[dict[str, Any]], min_score: float, min_margin: float) -> dict[str, Any] | None:
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


def get_author_sample_titles(
    db: sqlite3.Connection,
    author_id: str,
    *,
    author_name: str | None = None,
    openalex_id: str | None = None,
    limit: int = 4,
) -> list[str]:
    """Return representative publication titles for an author from local data."""
    author_key = str(author_id or "").strip()
    if not author_key and not (author_name or "").strip() and not (openalex_id or "").strip():
        return []

    resolved_name = (author_name or "").strip()
    resolved_openalex = _normalize_openalex_author_id(str(openalex_id or "").strip())
    if author_key and (not resolved_name or not resolved_openalex):
        row = db.execute(
            "SELECT name, openalex_id FROM authors WHERE id = ?",
            (author_key,),
        ).fetchone()
        if row:
            if not resolved_name:
                resolved_name = str(row["name"] or "").strip()
            if not resolved_openalex:
                resolved_openalex = _normalize_openalex_author_id(str(row["openalex_id"] or "").strip())

    rows: list[sqlite3.Row | tuple] = []
    if resolved_openalex:
        try:
            rows = db.execute(
                """
                SELECT p.title
                FROM papers p
                JOIN publication_authors pa ON p.id = pa.paper_id
                WHERE lower(pa.openalex_id) = lower(trim(?))
                  AND COALESCE(trim(p.title), '') <> ''
                ORDER BY COALESCE(p.cited_by_count, 0) DESC,
                         COALESCE(p.publication_date, '') DESC,
                         COALESCE(p.year, 0) DESC
                LIMIT ?
                """,
                (resolved_openalex, max(1, int(limit or 4))),
            ).fetchall()
        except Exception:
            rows = []

    if not rows and resolved_name:
        try:
            rows = db.execute(
                """
                SELECT p.title
                FROM papers p
                JOIN publication_authors pa ON p.id = pa.paper_id
                WHERE lower(trim(pa.display_name)) = lower(trim(?))
                  AND COALESCE(trim(p.title), '') <> ''
                ORDER BY COALESCE(p.cited_by_count, 0) DESC,
                         COALESCE(p.publication_date, '') DESC,
                         COALESCE(p.year, 0) DESC
                LIMIT ?
                """,
                (resolved_name, max(1, int(limit or 4))),
            ).fetchall()
        except Exception:
            rows = []

    return [str(row["title"] if isinstance(row, sqlite3.Row) else row[0] or "").strip() for row in rows if str(row["title"] if isinstance(row, sqlite3.Row) else row[0] or "").strip()]


def _person_name_tokens(name: str) -> list[str]:
    return [token for token in _normalize_text(name).split() if token]


def _author_name_alignment(author_name: str, candidate_name: str) -> float:
    """Return a conservative 0..1 alignment score for person-name matching."""
    source_tokens = _person_name_tokens(author_name)
    candidate_tokens = _person_name_tokens(candidate_name)
    if not source_tokens or not candidate_tokens:
        return 0.0
    if source_tokens == candidate_tokens:
        return 1.0

    source_first = source_tokens[0]
    candidate_first = candidate_tokens[0]
    source_last = source_tokens[-1]
    candidate_last = candidate_tokens[-1]
    if source_last != candidate_last:
        return 0.0

    seq_ratio = SequenceMatcher(None, " ".join(source_tokens), " ".join(candidate_tokens)).ratio()
    if source_first == candidate_first:
        return max(0.9, seq_ratio)
    if source_first[:1] and source_first[:1] == candidate_first[:1]:
        if len(source_first) == 1 or len(candidate_first) == 1:
            return max(0.84, min(0.92, seq_ratio))
        shorter = min(len(source_first), len(candidate_first))
        if shorter >= 3 and (source_first.startswith(candidate_first) or candidate_first.startswith(source_first)):
            return max(0.86, min(0.94, seq_ratio))
        return max(0.62, min(0.72, seq_ratio))
    return 0.0


def _candidate_sources(candidate: dict[str, Any]) -> set[str]:
    return {
        source.strip()
        for source in str(candidate.get("source") or "").split(",")
        if source.strip()
    }


def _candidate_name_support(candidate: dict[str, Any], author_name: str) -> float:
    if not author_name:
        return 1.0
    candidate_name = str(candidate.get("display_name") or "").strip()
    if not candidate_name:
        return 0.0
    return _author_name_alignment(author_name, candidate_name)


def _should_accept_openalex_candidate(candidate: dict[str, Any], author_name: str) -> bool:
    if not candidate:
        return False
    sources = _candidate_sources(candidate)
    if "provided_openalex" in sources or "orcid_openalex" in sources:
        return True

    name_support = float(candidate.get("name_alignment") or _candidate_name_support(candidate, author_name))
    score = float(candidate.get("score") or 0.0)
    if "local_authorship" in sources:
        return name_support >= 0.76 or (name_support >= 0.68 and score >= 8.0)
    if "scholar_openalex_bridge" in sources:
        return name_support >= 0.78
    return name_support >= 0.86 and score >= 5.5


def score_authorship_candidates(author_name: str, authorship_rows: list[Any]) -> list[dict[str, Any]]:
    """Score candidate OpenAlex author identities from local authorship rows."""
    name_norm = _normalize_text(author_name)
    if not name_norm:
        return []

    candidates: list[dict[str, Any]] = []
    for row in authorship_rows:
        cand_name = (row["display_name"] if isinstance(row, sqlite3.Row | dict) else row[1]) or ""
        cand_name = str(cand_name).strip()
        if not cand_name:
            continue
        alignment = _author_name_alignment(author_name, cand_name)
        if alignment <= 0.0:
            continue
        if alignment >= 0.98:
            name_score = 5.0
        elif alignment >= 0.9:
            name_score = 4.25
        elif alignment >= 0.82:
            name_score = 3.5
        elif alignment >= 0.74:
            name_score = 2.75
        else:
            continue

        if isinstance(row, (sqlite3.Row, dict)):
            pub_count = int(row["pub_count"] or 1)
            openalex_id = str(row["openalex_id"] or "").strip()
            orcid = str(row["orcid"] or "").strip()
            institution = str(row["institution"] or "").strip()
        else:
            pub_count = int(row[4] or 1)
            openalex_id = str(row[0] or "").strip()
            orcid = str(row[2] or "").strip()
            institution = str(row[3] or "").strip()

        score = round(name_score + min(3.0, float(pub_count)), 2)
        candidates.append(
            {
                "openalex_id": _normalize_openalex_author_id(openalex_id),
                "display_name": cand_name,
                "orcid": normalize_orcid(orcid) or orcid,
                "institution": institution,
                "score": score,
                "pub_count": pub_count,
                "name_alignment": round(alignment, 3),
            }
        )

    candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    return candidates


def _merge_openalex_candidates(*groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    for group in groups:
        for candidate in group or []:
            openalex_id = _normalize_openalex_author_id(str(candidate.get("openalex_id") or candidate.get("id") or ""))
            if not openalex_id:
                continue

            score = float(candidate.get("score") or 0.0)
            source = str(candidate.get("source") or "").strip() or "unknown"
            current = merged.get(openalex_id)
            if current is None:
                item = dict(candidate)
                item["openalex_id"] = openalex_id
                item["_sources"] = {source}
                merged[openalex_id] = item
                continue

            current["_sources"].add(source)
            current["score"] = round(max(float(current.get("score") or 0.0), score) + 1.25, 3)
            for field in ("display_name", "orcid", "institution", "pub_count"):
                if not current.get(field) and candidate.get(field):
                    current[field] = candidate.get(field)

    out: list[dict[str, Any]] = []
    for item in merged.values():
        item["source"] = ",".join(sorted(item.pop("_sources")))
        out.append(item)
    out.sort(key=lambda candidate: float(candidate.get("score") or 0.0), reverse=True)
    return out


def _local_authorship_candidates(
    db: sqlite3.Connection,
    *,
    author_id: str | None,
    author_name: str,
    known_openalex_id: str | None,
) -> list[dict[str, Any]]:
    """Find candidate OpenAlex identities from local authorship evidence."""
    rows: list[Any] = []
    normalized_openalex = _normalize_openalex_author_id(str(known_openalex_id or ""))

    if normalized_openalex:
        try:
            rows = db.execute(
                """
                SELECT pa.openalex_id, pa.display_name, pa.orcid, pa.institution, COUNT(*) AS pub_count
                FROM publication_authors pa
                WHERE lower(pa.openalex_id) = lower(trim(?))
                  AND COALESCE(TRIM(pa.openalex_id), '') <> ''
                GROUP BY pa.openalex_id, pa.display_name, pa.orcid, pa.institution
                ORDER BY pub_count DESC
                LIMIT 12
                """,
                (normalized_openalex,),
            ).fetchall()
        except Exception:
            rows = []

    if not rows and author_name:
        try:
            rows = db.execute(
                """
                SELECT pa.openalex_id, pa.display_name, pa.orcid, pa.institution, COUNT(*) AS pub_count
                FROM publication_authors pa
                WHERE lower(trim(pa.display_name)) = lower(trim(?))
                  AND COALESCE(TRIM(pa.openalex_id), '') <> ''
                GROUP BY pa.openalex_id, pa.display_name, pa.orcid, pa.institution
                ORDER BY pub_count DESC
                LIMIT 12
                """,
                (author_name.strip(),),
            ).fetchall()
        except Exception:
            rows = []

    if not rows and author_name:
        name_tokens = _person_name_tokens(author_name)
        name_last = name_tokens[-1] if name_tokens else ""
        if name_last:
            try:
                rows = db.execute(
                    """
                    SELECT pa.openalex_id, pa.display_name, pa.orcid, pa.institution, COUNT(*) AS pub_count
                    FROM publication_authors pa
                    WHERE lower(trim(pa.display_name)) LIKE ?
                      AND COALESCE(TRIM(pa.openalex_id), '') <> ''
                    GROUP BY pa.openalex_id, pa.display_name, pa.orcid, pa.institution
                    ORDER BY pub_count DESC
                    LIMIT 40
                    """,
                    (f"%{name_last}%",),
                ).fetchall()
            except Exception:
                rows = []

    candidates = score_authorship_candidates(author_name, rows)
    for candidate in candidates:
        candidate["source"] = "local_authorship"
    return candidates


def _hydrate_openalex_author_candidates(candidates: list[dict[str, Any]], author_name: str | None = None) -> list[dict[str, Any]]:
    """Add OpenAlex profile details to the top author candidates in one batch."""
    candidate_ids = [
        _normalize_openalex_author_id(str(candidate.get("openalex_id") or ""))
        for candidate in candidates[:8]
    ]
    details_by_id = batch_get_author_details(candidate_ids) if candidate_ids else {}
    out: list[dict[str, Any]] = []
    for candidate in candidates:
        openalex_id = _normalize_openalex_author_id(str(candidate.get("openalex_id") or ""))
        detail = details_by_id.get(openalex_id) or {}
        enriched = dict(candidate)
        if detail:
            enriched["display_name"] = str(detail.get("display_name") or candidate.get("display_name") or "").strip()
            enriched["orcid"] = normalize_orcid(str(detail.get("orcid") or candidate.get("orcid") or "")) or str(candidate.get("orcid") or "").strip()
            enriched["institution"] = str(detail.get("institution") or candidate.get("institution") or "").strip()
            enriched["works_count"] = int(detail.get("works_count") or 0)
            enriched["cited_by_count"] = int(detail.get("cited_by_count") or 0)
            enriched["topics"] = detail.get("topics") or []
            if detail.get("display_name"):
                ratio = SequenceMatcher(
                    None,
                    _normalize_text(str(candidate.get("display_name") or "")),
                    _normalize_text(str(detail.get("display_name") or "")),
                ).ratio()
                enriched["score"] = round(float(enriched.get("score") or 0.0) + (0.75 if ratio >= 0.9 else 0.25), 3)
        if author_name:
            name_alignment = _candidate_name_support(enriched, author_name)
            enriched["name_alignment"] = round(
                max(float(enriched.get("name_alignment") or 0.0), name_alignment),
                3,
            )
            enriched["score"] = round(
                float(enriched.get("score") or 0.0) + (0.9 if name_alignment >= 0.95 else 0.45 if name_alignment >= 0.82 else 0.0),
                3,
            )
        out.append(enriched)

    out.sort(key=lambda candidate: float(candidate.get("score") or 0.0), reverse=True)
    return out


def resolve_author_identity(
    db: sqlite3.Connection,
    *,
    author_id: str | None = None,
    author_name: str | None = None,
    openalex_id: str | None = None,
    scholar_id: str | None = None,
    orcid: str | None = None,
    sample_titles: list[str] | None = None,
    use_semantic_scholar: bool = True,
    use_orcid: bool = True,
    use_scholar_scrape_auto: bool = False,
) -> AuthorResolutionResult:
    """Resolve a robust author identity bundle from local and upstream sources."""
    resolved_name = str(author_name or "").strip()
    resolved_openalex_id = _normalize_openalex_author_id(str(openalex_id or ""))
    resolved_scholar_id = str(scholar_id or "").strip() or None
    resolved_orcid = normalize_orcid(orcid)
    evidence: list[ResolutionEvidence] = []

    if author_id and (not resolved_name or not resolved_openalex_id or not sample_titles):
        row = db.execute(
            "SELECT name, openalex_id, orcid, scholar_id FROM authors WHERE id = ?",
            (str(author_id).strip(),),
        ).fetchone()
        if row:
            if not resolved_name:
                resolved_name = str(row["name"] or "").strip()
            if not resolved_openalex_id:
                resolved_openalex_id = _normalize_openalex_author_id(str(row["openalex_id"] or ""))
            if not resolved_orcid:
                resolved_orcid = normalize_orcid(str(row["orcid"] or ""))
            if not resolved_scholar_id:
                resolved_scholar_id = str(row["scholar_id"] or "").strip() or None

    titles = [str(title or "").strip() for title in (sample_titles or []) if str(title or "").strip()]
    if not titles and author_id:
        titles = get_author_sample_titles(
            db,
            str(author_id).strip(),
            author_name=resolved_name,
            openalex_id=resolved_openalex_id,
            limit=4,
        )

    openalex_candidates: list[dict[str, Any]] = []

    if resolved_openalex_id:
        openalex_candidates.append(
            {
                "openalex_id": resolved_openalex_id,
                "display_name": resolved_name,
                "score": 12.0,
                "source": "provided_openalex",
            }
        )
        evidence.append(
            ResolutionEvidence(
                source="provided_openalex",
                detail=f"Caller supplied OpenAlex author id {resolved_openalex_id}",
                score=12.0,
            )
        )

    if resolved_orcid:
        candidate = find_author_by_orcid(resolved_orcid)
        if candidate:
            openalex_candidates.append(
                {
                    "openalex_id": _normalize_openalex_author_id(str(candidate.get("id") or "")),
                    "display_name": str(candidate.get("display_name") or "").strip(),
                    "score": 11.5,
                    "source": "orcid_openalex",
                    "orcid": resolved_orcid,
                }
            )
            evidence.append(
                ResolutionEvidence(
                    source="orcid_openalex",
                    detail=f"ORCID {resolved_orcid} resolved directly to OpenAlex",
                    score=11.5,
                )
            )

    if resolved_scholar_id:
        scholar_to_openalex = resolve_openalex_candidates_from_scholar(resolved_scholar_id)
        for candidate in scholar_to_openalex:
            normalized_id = _normalize_openalex_author_id(str(candidate.get("openalex_id") or candidate.get("id") or ""))
            openalex_candidates.append(
                {
                    "openalex_id": normalized_id,
                    "display_name": str(candidate.get("display_name") or "").strip(),
                    "orcid": normalize_orcid(str(candidate.get("orcid") or "")),
                    "institution": str(candidate.get("institution") or "").strip(),
                    "score": float(candidate.get("score") or 0.0) + 2.5,
                    "source": "scholar_openalex_bridge",
                }
            )

    if resolved_name:
        openalex_candidates.extend(
            _local_authorship_candidates(
                db,
                author_id=author_id,
                author_name=resolved_name,
                known_openalex_id=resolved_openalex_id,
            )
        )
        if titles:
            metadata_candidates = resolve_openalex_candidates_from_metadata(resolved_name, titles)
            for candidate in metadata_candidates:
                candidate = dict(candidate)
                candidate["source"] = "openalex_metadata"
                openalex_candidates.append(candidate)
        else:
            fallback_by_name = find_author_id_by_name(resolved_name)
            if fallback_by_name:
                openalex_candidates.append(
                    {
                        "openalex_id": _normalize_openalex_author_id(fallback_by_name),
                        "display_name": resolved_name,
                        "score": 4.5,
                        "source": "openalex_name_search",
                    }
                )

    openalex_candidates = _hydrate_openalex_author_candidates(
        _merge_openalex_candidates(openalex_candidates),
        resolved_name or None,
    )
    top_openalex = _pick_top_candidate(openalex_candidates, min_score=5.0, min_margin=1.25)
    if top_openalex and _should_accept_openalex_candidate(top_openalex, resolved_name):
        resolved_openalex_id = _normalize_openalex_author_id(str(top_openalex.get("openalex_id") or ""))
        if not resolved_name:
            resolved_name = str(top_openalex.get("display_name") or "").strip()
        if not resolved_orcid:
            resolved_orcid = normalize_orcid(str(top_openalex.get("orcid") or ""))
        evidence.append(
            ResolutionEvidence(
                source=str(top_openalex.get("source") or "openalex"),
                detail=f"Selected OpenAlex author {resolved_openalex_id}",
                score=float(top_openalex.get("score") or 0.0),
                payload={"display_name": str(top_openalex.get("display_name") or "").strip()},
            )
        )
    elif openalex_candidates:
        top_rejected = openalex_candidates[0]
        evidence.append(
            ResolutionEvidence(
                source="openalex_ambiguous",
                detail="OpenAlex candidates found but identity confidence was too low",
                score=float(top_rejected.get("score") or 0.0),
                payload={
                    "top_openalex_id": str(top_rejected.get("openalex_id") or "").strip(),
                    "top_display_name": str(top_rejected.get("display_name") or "").strip(),
                    "name_alignment": float(top_rejected.get("name_alignment") or 0.0),
                },
            )
        )

    scholar_candidates = resolve_scholar_candidates_from_sources(
        resolved_name,
        openalex_id=resolved_openalex_id or None,
        orcid=resolved_orcid or None,
        sample_titles=titles,
        use_semantic_scholar=use_semantic_scholar,
        use_orcid=use_orcid,
    ) if resolved_name else []
    if use_scholar_scrape_auto:
        # Scrape fallback remains outside this module on purpose.
        logger.debug("Scholar scrape fallback requested but not handled in core resolver")

    top_scholar = _pick_top_candidate(scholar_candidates, min_score=5.5, min_margin=1.0)
    if top_scholar:
        resolved_scholar_id = str(top_scholar.get("scholar_id") or "").strip() or resolved_scholar_id
        evidence.append(
            ResolutionEvidence(
                source=str(top_scholar.get("source") or "semantic_scholar"),
                detail=f"Resolved Scholar id {resolved_scholar_id}",
                score=float(top_scholar.get("score") or 0.0),
            )
        )

    if resolved_openalex_id and not resolved_name:
        try:
            resolved_name = get_author_name_by_id(resolved_openalex_id) or resolved_name
        except Exception:
            pass

    openalex_profile = None
    if top_openalex:
        openalex_profile = dict(top_openalex)

    confidence = 0.0
    if top_openalex:
        confidence = max(confidence, min(1.0, float(top_openalex.get("score") or 0.0) / 12.0))
    if top_scholar:
        confidence = max(confidence, min(1.0, float(top_scholar.get("score") or 0.0) / 10.0))
    if resolved_orcid and resolved_openalex_id:
        confidence = max(confidence, 0.95)

    if resolved_openalex_id or resolved_scholar_id:
        status = "resolved_auto"
    elif openalex_candidates or scholar_candidates:
        status = "needs_manual_review"
    else:
        status = "no_match"

    reason_parts = [e.detail for e in evidence[:4]]
    if not reason_parts:
        if resolved_name:
            reason_parts.append(f"No confident identity match found for {resolved_name}")
        else:
            reason_parts.append("No confident identity match found")

    return AuthorResolutionResult(
        author_name=resolved_name or None,
        openalex_id=resolved_openalex_id or None,
        scholar_id=resolved_scholar_id or None,
        orcid=resolved_orcid or None,
        status=status,
        confidence=round(confidence, 3),
        reason="; ".join(reason_parts)[:1000],
        evidence=evidence,
        openalex_candidates=openalex_candidates[:5],
        scholar_candidates=scholar_candidates[:5],
        openalex_profile=openalex_profile,
    )


def summarize_author_resolution(result: AuthorResolutionResult) -> str:
    parts = [result.reason]
    if result.confidence:
        parts.append(f"confidence={result.confidence:.2f}")
    if result.openalex_id:
        parts.append(f"openalex={result.openalex_id}")
    if result.scholar_id:
        parts.append(f"scholar={result.scholar_id}")
    if result.orcid:
        parts.append(f"orcid={result.orcid}")
    return "; ".join([part for part in parts if part])[:1000]


def _title_variants(title: str) -> list[str]:
    base = (title or "").strip()
    if not base:
        return []
    variants = [base]
    no_brackets = re.sub(r"[\(\[\{].*?[\)\]\}]", " ", base)
    no_brackets = re.sub(r"\s+", " ", no_brackets).strip(" .,:;")
    if no_brackets and no_brackets not in variants:
        variants.append(no_brackets)
    for sep in (":", " - ", " – ", " — "):
        if sep in no_brackets:
            head = no_brackets.split(sep, 1)[0].strip(" .,:;")
            if len(head) >= 20 and head not in variants:
                variants.append(head)
    normalized = _normalize_text(base)
    if normalized and normalized not in variants:
        variants.append(normalized)
    return variants[:5]


def _doi_variants(doi_raw: str) -> list[str]:
    raw = (doi_raw or "").strip()
    if not raw:
        return []
    values = [
        raw,
        normalize_doi(raw) or "",
        re.sub(r"^https?://(dx\.)?doi\.org/", "", raw, flags=re.IGNORECASE),
        re.sub(r"^doi:\\s*", "", raw, flags=re.IGNORECASE),
    ]
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = (value or "").strip()
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        out.append(cleaned)
    return out


def _extract_arxiv_id(text: str) -> str | None:
    raw = (text or "").strip()
    if not raw:
        return None
    patterns = [
        r"(?:arxiv[:/\\s]+)(\\d{4}\\.\\d{4,5}(?:v\\d+)?)",
        r"(?:arxiv\\.org/(?:abs|pdf)/)(\\d{4}\\.\\d{4,5}(?:v\\d+)?)",
        r"\\b(\\d{4}\\.\\d{4,5}(?:v\\d+)?)\\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, raw, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _extract_biorxiv_doi(text: str) -> str | None:
    raw = (text or "").strip()
    if not raw:
        return None
    match = re.search(r"(10\\.1101/[^\\s/\\\"'<>]+)", raw, flags=re.IGNORECASE)
    if match:
        return normalize_doi(match.group(1))
    return None


def _preprint_hints(pub: dict[str, Any]) -> dict[str, Any]:
    combined = " ".join(
        [
            str(pub.get("doi") or "").strip(),
            str(pub.get("url") or "").strip(),
            str(pub.get("title") or "").strip(),
            str(pub.get("journal") or "").strip(),
        ]
    )
    arxiv_id = _extract_arxiv_id(combined)
    biorxiv_doi = _extract_biorxiv_doi(combined)
    synthetic_dois: list[str] = []
    if arxiv_id:
        synthetic_dois.append(f"10.48550/arXiv.{arxiv_id}")
    if biorxiv_doi:
        synthetic_dois.append(biorxiv_doi)
    return {
        "looks_preprint": bool(arxiv_id or biorxiv_doi or "arxiv" in combined.lower() or "biorxiv" in combined.lower()),
        "synthetic_dois": synthetic_dois,
    }


def _author_tokens(authors_raw: str) -> set[str]:
    tokens: set[str] = set()
    for name in [part.strip() for part in (authors_raw or "").split(",") if part.strip()][:8]:
        pieces = [piece for piece in re.split(r"\\s+", name) if piece]
        if not pieces:
            continue
        tokens.add(pieces[-1].lower())
        if len(pieces) > 1:
            tokens.add(pieces[0].lower())
    return tokens


def _score_openalex_work_candidate(candidate: dict[str, Any], pub: dict[str, Any]) -> float:
    candidate_title = str(candidate.get("display_name") or "").strip()
    input_title = str(pub.get("title") or "").strip()
    if not candidate_title or not input_title:
        return 0.0

    title_a = _normalize_text(input_title)
    title_b = _normalize_text(candidate_title)
    if not title_a or not title_b:
        return 0.0

    title_score = SequenceMatcher(None, title_a, title_b).ratio() * 6.0
    if title_a == title_b:
        title_score += 2.0
    elif title_a in title_b or title_b in title_a:
        title_score += 1.0

    year_score = 0.0
    try:
        src_year = int(pub.get("year")) if pub.get("year") is not None else None
        candidate_year = int(candidate.get("publication_year")) if candidate.get("publication_year") is not None else None
        if src_year is not None and candidate_year is not None:
            if src_year == candidate_year:
                year_score = 1.5
            elif abs(src_year - candidate_year) <= 1:
                year_score = 0.7
    except Exception:
        pass

    doi_score = 0.0
    src_doi = normalize_doi(pub.get("doi") or "")
    candidate_doi = normalize_doi(candidate.get("doi") or "")
    if src_doi and candidate_doi and src_doi == candidate_doi:
        doi_score = 3.0

    author_score = 0.0
    src_tokens = _author_tokens(str(pub.get("authors") or ""))
    candidate_tokens: set[str] = set()
    for authorship in (candidate.get("authorships") or [])[:10]:
        author_name = str(((authorship or {}).get("author") or {}).get("display_name") or "").strip()
        for token in re.split(r"\\s+", author_name):
            if token:
                candidate_tokens.add(token.lower())
    if src_tokens and candidate_tokens:
        author_score = min(2.0, 0.8 * len(src_tokens.intersection(candidate_tokens)))

    return round(title_score + year_score + doi_score + author_score, 3)


def _search_openalex_work_candidates(title_query: str, *, per_page: int = 10) -> list[dict[str, Any]]:
    if not (title_query or "").strip():
        return []
    try:
        response = get_client().get(
            "/works",
            params={
                "search": title_query.strip(),
                "per-page": max(1, min(int(per_page or 10), 25)),
                "select": _WORKS_SELECT_FIELDS,
            },
            timeout=20,
        )
        response.raise_for_status()
        return (response.json() or {}).get("results") or []
    except Exception as exc:
        logger.debug("OpenAlex paper search failed for '%s': %s", title_query[:80], exc)
        return []


def _score_semantic_candidate(candidate: dict[str, Any], pub: dict[str, Any]) -> float:
    title = str(candidate.get("title") or "").strip()
    input_title = str(pub.get("title") or "").strip()
    if not title or not input_title:
        return 0.0
    title_score = SequenceMatcher(None, _normalize_text(input_title), _normalize_text(title)).ratio() * 5.0
    doi_score = 2.5 if normalize_doi(candidate.get("doi") or "") and normalize_doi(candidate.get("doi") or "") in {normalize_doi(pub.get("doi") or ""), normalize_doi(pub.get("url") or "")} else 0.0
    author_score = min(1.5, 0.75 * len(_author_tokens(str(pub.get("authors") or "")).intersection(_author_tokens(str(candidate.get("authors") or "")))))
    year_score = 0.0
    try:
        if pub.get("year") is not None and candidate.get("year") is not None:
            src_year = int(pub.get("year"))
            cand_year = int(candidate.get("year"))
            if src_year == cand_year:
                year_score = 1.0
            elif abs(src_year - cand_year) <= 1:
                year_score = 0.4
    except Exception:
        pass
    return round(title_score + doi_score + author_score + year_score, 3)


def resolve_paper_openalex_work(
    pub: dict[str, Any],
    *,
    title_search_cache: dict[str, dict[str, Any] | None] | None = None,
    doi_prefetch: dict[str, dict[str, Any]] | None = None,
) -> PaperResolutionResult:
    """Resolve a paper to a normalized OpenAlex work using multiple bridges."""
    evidence: list[ResolutionEvidence] = []
    title = str(pub.get("title") or "").strip()
    title_key = _normalize_text(title)
    hints = _preprint_hints(pub)

    openalex_id = str(pub.get("openalex_id") or "").strip()
    if openalex_id:
        work = batch_fetch_works_by_openalex_ids([openalex_id]).get(_normalize_openalex_author_id(openalex_id))  # type: ignore[arg-type]
        if not work:
            normalized_work_id = str(openalex_id).rstrip("/").split("/")[-1]
            work = batch_fetch_works_by_openalex_ids([normalized_work_id]).get(normalized_work_id)
        if work:
            evidence.append(ResolutionEvidence("provided_openalex", "Paper already had an OpenAlex id", 12.0))
            return PaperResolutionResult(
                work=_normalize_work(work),
                source="openalex_id",
                reason=None,
                confidence=1.0,
                evidence=evidence,
            )

    if doi_prefetch:
        for doi_try in _doi_variants(str(pub.get("doi") or "")) + list(hints.get("synthetic_dois") or []):
            normalized = (normalize_doi(doi_try) or doi_try).strip().lower()
            if normalized and normalized in doi_prefetch:
                evidence.append(ResolutionEvidence("doi_prefetch", f"Prefetched DOI hit for {normalized}", 11.0))
                return PaperResolutionResult(
                    work=_normalize_work(doi_prefetch[normalized]),
                    source="doi_prefetch",
                    reason=None,
                    confidence=0.98,
                    evidence=evidence,
                )

    doi_candidates = _doi_variants(str(pub.get("doi") or "")) + list(hints.get("synthetic_dois") or [])
    if doi_candidates:
        by_doi = batch_fetch_works_by_dois(doi_candidates, batch_size=min(len(doi_candidates), 10), max_workers=1)
        for doi_try in doi_candidates:
            normalized = (normalize_doi(doi_try) or doi_try).strip().lower()
            work = by_doi.get(normalized)
            if work:
                evidence.append(ResolutionEvidence("openalex_doi", f"Resolved paper by DOI {normalized}", 11.5))
                return PaperResolutionResult(
                    work=_normalize_work(work),
                    source="doi",
                    reason=None,
                    confidence=0.99,
                    evidence=evidence,
                )

    if title_search_cache is not None and title_key and title_key in title_search_cache:
        cached = title_search_cache[title_key]
        if cached is not None:
            evidence.append(ResolutionEvidence("title_cache", f"Resolved from cached title match for '{title[:80]}'", 9.0))
            return PaperResolutionResult(
                work=cached,
                source="title_cached",
                reason=None,
                confidence=0.9,
                evidence=evidence,
            )
        return PaperResolutionResult(
            work=None,
            source=None,
            reason="not_found_cached",
            confidence=0.0,
            evidence=evidence,
        )

    openalex_candidates: dict[str, dict[str, Any]] = {}
    for query in _title_variants(title):
        for candidate in _search_openalex_work_candidates(query, per_page=10):
            candidate_id = str(candidate.get("id") or "").strip()
            if candidate_id:
                openalex_candidates[candidate_id] = candidate

    if openalex_candidates:
        ranked = sorted(
            (
                (
                    candidate_id,
                    _score_openalex_work_candidate(candidate, pub),
                    candidate,
                )
                for candidate_id, candidate in openalex_candidates.items()
            ),
            key=lambda item: item[1],
            reverse=True,
        )
        top_id, top_score, top_candidate = ranked[0]
        second_score = ranked[1][1] if len(ranked) > 1 else -1.0
        min_score = 3.6 if hints.get("looks_preprint") else 4.5
        min_margin = 0.4 if hints.get("looks_preprint") else 0.75
        if top_score >= min_score and (len(ranked) == 1 or (top_score - second_score) >= min_margin):
            normalized = _normalize_work(top_candidate)
            if title_search_cache is not None and title_key:
                title_search_cache[title_key] = normalized
            evidence.append(ResolutionEvidence("openalex_title_scored", f"Scored OpenAlex title candidates for '{title[:80]}'", top_score))
            return PaperResolutionResult(
                work=normalized,
                source="title_scored",
                reason=None,
                confidence=min(0.95, round(top_score / 8.0, 3)),
                evidence=evidence,
            )
        evidence.append(
            ResolutionEvidence(
                "openalex_title_ambiguous",
                f"OpenAlex title search for '{title[:80]}' was ambiguous",
                top_score,
            )
        )

    semantic_candidates = search_semantic_papers(title, limit=8) if title else []
    for candidate in semantic_candidates:
        candidate["score"] = _score_semantic_candidate(candidate, pub)
    semantic_candidates.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    top_semantic = _pick_top_candidate(semantic_candidates, min_score=4.2, min_margin=0.6)
    if top_semantic:
        semantic_dois = _doi_variants(str(top_semantic.get("doi") or ""))
        if semantic_dois:
            bridged = batch_fetch_works_by_dois(semantic_dois, batch_size=min(len(semantic_dois), 10), max_workers=1)
            for doi_try in semantic_dois:
                normalized = (normalize_doi(doi_try) or doi_try).strip().lower()
                work = bridged.get(normalized)
                if work:
                    normalized_work = _normalize_work(work)
                    if title_search_cache is not None and title_key:
                        title_search_cache[title_key] = normalized_work
                    evidence.append(
                        ResolutionEvidence(
                            "semantic_to_openalex",
                            f"Semantic Scholar bridged this paper back to OpenAlex via DOI {normalized}",
                            float(top_semantic.get("score") or 0.0),
                        )
                    )
                    return PaperResolutionResult(
                        work=normalized_work,
                        source="semantic_doi_bridge",
                        reason=None,
                        confidence=min(0.9, round(float(top_semantic.get("score") or 0.0) / 7.5, 3)),
                        evidence=evidence,
                        semantic_candidate=top_semantic,
                    )
        evidence.append(
            ResolutionEvidence(
                "semantic_only_match",
                f"Semantic Scholar found a strong candidate for '{title[:80]}' but no OpenAlex bridge was available",
                float(top_semantic.get("score") or 0.0),
            )
        )
        if title_search_cache is not None and title_key:
            title_search_cache[title_key] = None
        return PaperResolutionResult(
            work=None,
            source=None,
            reason="semantic_match_without_openalex_bridge",
            confidence=min(0.75, round(float(top_semantic.get("score") or 0.0) / 7.5, 3)),
            evidence=evidence,
            semantic_candidate=top_semantic,
        )

    if title_search_cache is not None and title_key:
        title_search_cache[title_key] = None
    return PaperResolutionResult(
        work=None,
        source=None,
        reason="not_found",
        confidence=0.0,
        evidence=evidence,
    )
