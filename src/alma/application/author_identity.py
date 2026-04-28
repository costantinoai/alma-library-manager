"""Hierarchical author-identity resolver.

Single responsibility: given one partial author record (any combination
of ``author_name`` / ``openalex_id`` / ``scholar_id`` / ``orcid`` /
``sample_titles``), return a scored identity bundle that an upstream
caller can persist on the ``authors`` row.

This module layers a tiered, auditable cascade on top of the existing
``core.resolution.resolve_author_identity`` backbone. The older resolver
produced a ``status`` + ``confidence`` but didn't expose which tier
fired or preserve cross-source evidence from preprint venues (arXiv /
bioRxiv / psyRxiv). This one does both, so the Settings UI can surface
"resolved via ORCID" / "resolved via preprint coauthor overlap" /
"needs manual review" with calibrated confidences.

Tiers, in priority order:

    1.00  ``orcid_direct``          ORCID + OpenAlex match via ``find_author_by_orcid``
    0.95  ``openalex_provided``     Caller supplied a valid OpenAlex author id
    0.90  ``scholar_bridge``        Google Scholar → OpenAlex via bridge
    0.85  ``semantic_scholar``      S2 paper match surfaces preprint externalIds
    0.80  ``title_overlap``         Name match + ≥ 3 titles in S2/OA hit
    0.70  ``coauthor_overlap``      Name match + ≥ 3 coauthors overlap
    0.60  ``name_affiliation``      Name match + affiliation token overlap
   <0.60  ``needs_manual_review``   No tier crossed — surface for human review

Accepted threshold is 0.70. Below that we persist the top candidate
with ``id_resolution_status = 'needs_manual_review'`` and flag in the UI.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from alma.core.resolution import (
    AuthorResolutionResult,
    get_author_sample_titles,
    resolve_author_identity,
)
from alma.core.utils import normalize_title_key

logger = logging.getLogger(__name__)

ACCEPTED_CONFIDENCE = 0.70


@dataclass(slots=True)
class IdentityEvidence:
    """A single piece of identity evidence contributed by one source."""

    source: str
    detail: str
    confidence: float
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class HierarchicalIdentity:
    """Scored identity bundle ready for persistence."""

    author_name: Optional[str] = None
    openalex_id: Optional[str] = None
    scholar_id: Optional[str] = None
    orcid: Optional[str] = None
    semantic_scholar_id: Optional[str] = None
    preprint_ids: dict[str, str] = field(default_factory=dict)  # {"arxiv": "...", "biorxiv": "..."}
    method: str = "none"
    confidence: float = 0.0
    status: str = "no_match"  # resolved_auto / needs_manual_review / no_match
    accepted: bool = False
    evidence: list[IdentityEvidence] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "author_name": self.author_name,
            "openalex_id": self.openalex_id,
            "scholar_id": self.scholar_id,
            "orcid": self.orcid,
            "semantic_scholar_id": self.semantic_scholar_id,
            "preprint_ids": self.preprint_ids,
            "method": self.method,
            "confidence": round(self.confidence, 3),
            "status": self.status,
            "accepted": self.accepted,
            "evidence": [
                {
                    "source": e.source,
                    "detail": e.detail,
                    "confidence": round(e.confidence, 3),
                    "payload": e.payload,
                }
                for e in self.evidence
            ],
        }


# -- Preprint triangulation -----------------------------------------------------


_PREPRINT_EXTERNAL_IDS = {
    "ArXiv": "arxiv",
    "BioRxiv": "biorxiv",
    "PsyRxiv": "psyrxiv",
    "PsyArXiv": "psyrxiv",
    "MedRxiv": "medrxiv",
    "ChemRxiv": "chemrxiv",
}


def _preprint_hints_from_titles(
    author_name: str,
    sample_titles: list[str],
    *,
    max_papers: int = 6,
) -> tuple[dict[str, str], list[IdentityEvidence]]:
    """Query Semantic Scholar for sample titles, extract preprint external IDs.

    S2 stores arXiv / bioRxiv / psyRxiv / medRxiv / chemRxiv identifiers
    in ``externalIds`` when the record originated from a preprint server.
    This gives us a second axis of author confirmation: if every title
    in ``sample_titles`` that resolves on S2 lists the same author as a
    preprint contributor, we gain preprint-level triangulation evidence.
    """
    evidence: list[IdentityEvidence] = []
    preprint_ids: dict[str, str] = {}
    if not sample_titles:
        return preprint_ids, evidence

    try:
        from alma.discovery.semantic_scholar import search_papers
    except Exception as exc:
        logger.debug("S2 preprint search unavailable: %s", exc)
        return preprint_ids, evidence

    normalized_author = (author_name or "").strip().lower()
    titles_to_try = list(sample_titles[:max_papers])
    matches = 0

    for title in titles_to_try:
        if not title:
            continue
        try:
            papers = search_papers(title, limit=3)
        except Exception as exc:
            logger.debug("S2 search_papers failed for %r: %s", title[:40], exc)
            continue

        best = None
        best_key = normalize_title_key(title)
        for paper in papers:
            paper_title = str(paper.get("title") or "").strip()
            if normalize_title_key(paper_title) == best_key:
                best = paper
                break
        if not best and papers:
            best = papers[0]
        if not best:
            continue

        # Extract preprint external IDs.
        ext_ids = best.get("externalIds") or best.get("payload", {}).get("externalIds") or {}
        if not isinstance(ext_ids, dict):
            continue
        for s2_name, our_name in _PREPRINT_EXTERNAL_IDS.items():
            raw = str(ext_ids.get(s2_name) or "").strip()
            if raw and our_name not in preprint_ids:
                preprint_ids[our_name] = raw

        # Confirm the S2 paper lists our author.
        authors_csv = str(best.get("authors") or "").lower()
        if normalized_author and normalized_author in authors_csv:
            matches += 1

    if preprint_ids or matches:
        evidence.append(
            IdentityEvidence(
                source="semantic_scholar_preprint",
                detail=(
                    f"S2 confirmed {matches}/{len(titles_to_try)} sample titles; "
                    f"preprint IDs: {', '.join(preprint_ids.keys()) or 'none'}"
                ),
                confidence=min(0.85, 0.4 + 0.15 * matches),
                payload={"preprint_ids": preprint_ids, "sample_hits": matches},
            )
        )
    return preprint_ids, evidence


# -- Tier cascade --------------------------------------------------------------


def _tier_from_core_result(result: AuthorResolutionResult) -> tuple[str, float]:
    """Map the core resolver's evidence trail to an explicit tier + confidence.

    The core resolver already produced a 0-1 confidence. We re-label it
    with an explicit tier name so the UI can render "resolved via ORCID"
    vs "resolved via title overlap" accurately. If two sources
    contributed, the higher-confidence tier wins.
    """
    sources = {str(e.source or "") for e in result.evidence}
    confidence = float(result.confidence or 0.0)

    if "orcid_openalex" in sources or (result.orcid and result.openalex_id and confidence >= 0.90):
        return "orcid_direct", max(confidence, 1.00 if confidence >= 0.9 else confidence)
    if "provided_openalex" in sources:
        return "openalex_provided", max(confidence, 0.95)
    if "scholar_openalex_bridge" in sources:
        return "scholar_bridge", max(confidence, 0.90)
    if "openalex_metadata" in sources:
        return "title_overlap", max(confidence, 0.80)
    if "local_authorship" in sources:
        return "coauthor_overlap", max(confidence, 0.70)
    if "openalex_name_search" in sources:
        return "name_affiliation", max(confidence, 0.60)
    return "none", confidence


def resolve_identity_hierarchical(
    db: sqlite3.Connection,
    *,
    author_id: Optional[str] = None,
    author_name: Optional[str] = None,
    openalex_id: Optional[str] = None,
    scholar_id: Optional[str] = None,
    orcid: Optional[str] = None,
    sample_titles: Optional[list[str]] = None,
    use_semantic_scholar: bool = True,
    use_orcid: bool = True,
    use_preprints: bool = True,
    accepted_confidence: float = ACCEPTED_CONFIDENCE,
) -> HierarchicalIdentity:
    """Run the full tiered cascade and return a scored identity bundle.

    Callers that already have an ``author_id`` in the local ``authors``
    table can pass it — the helper will fill in ``author_name`` and
    ``sample_titles`` from the database when the caller doesn't supply
    them.

    This delegates the heavy candidate enumeration to the existing
    ``resolve_author_identity`` in ``core.resolution``, then layers
    preprint triangulation and tier re-labelling on top. Returns a
    fully-formed :class:`HierarchicalIdentity` ready for
    :func:`persist_identity_result`.
    """
    resolved_titles = list(sample_titles or [])
    if author_id and not resolved_titles:
        try:
            resolved_titles = get_author_sample_titles(
                db,
                str(author_id).strip(),
                author_name=author_name,
                openalex_id=openalex_id,
                limit=6,
            )
        except Exception as exc:
            logger.debug("sample-title enrichment failed for %s: %s", author_id, exc)
            resolved_titles = []

    try:
        core_result = resolve_author_identity(
            db,
            author_id=author_id,
            author_name=author_name,
            openalex_id=openalex_id,
            scholar_id=scholar_id,
            orcid=orcid,
            sample_titles=resolved_titles,
            use_semantic_scholar=use_semantic_scholar,
            use_orcid=use_orcid,
        )
    except Exception as exc:
        logger.warning(
            "Core identity resolver failed for %s / %s: %s",
            author_name or author_id or "<unknown>",
            openalex_id,
            exc,
        )
        core_result = AuthorResolutionResult(
            author_name=author_name,
            openalex_id=openalex_id,
            scholar_id=scholar_id,
            orcid=orcid,
            status="no_match",
            confidence=0.0,
            reason=f"Core resolver error: {exc}",
        )

    method, confidence = _tier_from_core_result(core_result)

    bundle = HierarchicalIdentity(
        author_name=core_result.author_name,
        openalex_id=core_result.openalex_id,
        scholar_id=core_result.scholar_id,
        orcid=core_result.orcid,
        method=method,
        confidence=confidence,
        evidence=[
            IdentityEvidence(
                source=str(e.source),
                detail=str(e.detail),
                confidence=min(1.0, float(e.score or 0.0) / 12.0),
                payload=dict(e.payload or {}),
            )
            for e in core_result.evidence
        ],
    )

    # Preprint triangulation — independent signal that can upgrade the
    # tier when the core resolver landed something weak but S2 confirms
    # the same author on matching preprints.
    if use_preprints and bundle.author_name:
        try:
            preprint_ids, preprint_evidence = _preprint_hints_from_titles(
                bundle.author_name,
                resolved_titles,
            )
            bundle.preprint_ids = preprint_ids
            bundle.evidence.extend(preprint_evidence)
            if preprint_evidence:
                top = max(e.confidence for e in preprint_evidence)
                bundle.confidence = max(bundle.confidence, top)
                if bundle.method in {"none", "name_affiliation"} and top >= 0.70:
                    bundle.method = "semantic_scholar"
        except Exception as exc:
            logger.debug("preprint triangulation failed: %s", exc)

    # Semantic Scholar authorId (not just preprint IDs) — fetch if we
    # already have an OpenAlex id so the bundle is complete for downstream
    # consumers that key on S2.
    if use_semantic_scholar and bundle.openalex_id and bundle.semantic_scholar_id is None:
        try:
            bundle.semantic_scholar_id = _lookup_s2_author_id(bundle.author_name or "", resolved_titles)
        except Exception as exc:
            logger.debug("S2 authorId lookup failed: %s", exc)

    if bundle.confidence >= accepted_confidence and (bundle.openalex_id or bundle.scholar_id):
        bundle.accepted = True
        bundle.status = "resolved_auto"
    elif bundle.openalex_id or bundle.scholar_id or bundle.confidence > 0:
        bundle.status = "needs_manual_review"
    else:
        bundle.status = "no_match"

    return bundle


def _lookup_s2_author_id(author_name: str, sample_titles: list[str]) -> Optional[str]:
    """Best-effort S2 authorId lookup from a paper-title round-trip."""
    if not author_name or not sample_titles:
        return None
    try:
        from alma.discovery.semantic_scholar import search_papers
    except Exception:
        return None

    needle = author_name.strip().lower()
    for title in sample_titles[:3]:
        try:
            papers = search_papers(title, limit=3)
        except Exception:
            continue
        for paper in papers:
            for a in paper.get("authors") or []:
                if str(a.get("name") or "").strip().lower() == needle:
                    author_id = str(a.get("authorId") or "").strip()
                    if author_id:
                        return author_id
    return None


# -- Persistence ---------------------------------------------------------------


def persist_identity_result(
    db: sqlite3.Connection,
    author_id: str,
    result: HierarchicalIdentity,
    *,
    now: Optional[str] = None,
) -> None:
    """Write the tier + confidence + evidence onto the ``authors`` row.

    Assumes :func:`_ensure_identity_resolution_columns` already ran at
    startup so the three columns exist. Evidence is serialised as JSON
    so the UI can re-hydrate it without shipping a second join.
    """
    timestamp = now or datetime.utcnow().isoformat()
    evidence_blob = json.dumps(result.to_dict(), ensure_ascii=False)

    updates: list[str] = []
    params: list[Any] = []

    if result.openalex_id:
        updates.append("openalex_id = COALESCE(NULLIF(openalex_id, ''), ?)")
        params.append(result.openalex_id)
    if result.scholar_id:
        updates.append("scholar_id = COALESCE(NULLIF(scholar_id, ''), ?)")
        params.append(result.scholar_id)
    if result.orcid:
        from alma.core.utils import normalize_orcid

        normalized_orcid = normalize_orcid(result.orcid)
        if normalized_orcid:
            updates.append("orcid = COALESCE(NULLIF(orcid, ''), ?)")
            params.append(normalized_orcid)

    updates.extend(
        [
            "id_resolution_status = ?",
            "id_resolution_method = ?",
            "id_resolution_confidence = ?",
            "id_resolution_evidence = ?",
            "id_resolution_updated_at = ?",
        ]
    )
    params.extend(
        [
            result.status,
            result.method,
            float(result.confidence or 0.0),
            evidence_blob,
            timestamp,
        ]
    )

    sql = f"UPDATE authors SET {', '.join(updates)} WHERE id = ?"
    params.append(author_id)
    try:
        db.execute(sql, params)
        if db.in_transaction:
            db.commit()
    except sqlite3.OperationalError as exc:
        logger.warning("persist_identity_result failed for %s: %s", author_id, exc)


def ensure_identity_resolution_columns(db: sqlite3.Connection) -> None:
    """One-shot migration: add the three identity columns if missing."""
    try:
        cols = {r[1] for r in db.execute("PRAGMA table_info(authors)").fetchall()}
    except sqlite3.OperationalError:
        return
    additions = {
        "id_resolution_method": "TEXT",
        "id_resolution_confidence": "REAL",
        "id_resolution_evidence": "TEXT",
    }
    for col, typ in additions.items():
        if col not in cols:
            try:
                db.execute(f"ALTER TABLE authors ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError as exc:
                logger.debug("add column %s failed: %s", col, exc)
    if db.in_transaction:
        db.commit()
