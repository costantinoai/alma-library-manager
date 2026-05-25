"""Canonical data-health assessment — the single source of truth for
"what's wrong with the corpus" (task 24, Pillar 1).

Health used to be recomputed independently in ~5 surfaces (Operational
Status, Insights Diagnostics, papers/authors needs-attention, Overview),
which drifted apart. This module consolidates the detection into ONE
assessor that every surface reads (via the materialised-view layer), so
there is one code path per number = consistency by construction.

It is **pure-read derivation** — no new tables. It reuses the existing
building blocks rather than reinventing them:
- ``corpus_rehydrate.build_enrichment_status`` — per-field missing counts.
- ``embedding_chain._count_s2_fetch_candidates`` / ``_count_local_specter2_candidates``
  — counts that mirror the actual repair SELECTs (so a shown count equals
  the work a repair would do).
- the active-embedding-model coverage formula used by ``graphs.py``.

Each emitted **dimension** carries not just the metric but the
human-facing *problem* (``explanation``/``impact``) and the *fix*
(``actions``) so the Health cards, needs-attention rows, and operational
states all render the same guidance.

Always read this through ``mv.get(conn, "health:corpus")`` — never call
``assess_corpus`` on the request path (the missing-field aggregate scans
``papers`` with EXISTS subqueries; cheap for a backgrounded MV build,
not for every GET).
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any

from alma.application import materialized_views as mv

# embeddings_ready flips true at this coverage % (user decision 2026-05-25).
EMBEDDINGS_READY_THRESHOLD = 80.0

HEALTH_CORPUS_VIEW_KEY = "health:corpus"

Severity = str  # "ok" | "info" | "warning" | "critical"


# --------------------------------------------------------------------------
# Severity helpers
# --------------------------------------------------------------------------


def _severity_from_fraction(
    count: int, total: int, *, warn: float = 0.05, crit: float = 0.25
) -> Severity:
    """Severity for a "fewer is better" count (e.g. missing abstracts)."""
    if count <= 0:
        return "ok"
    if total <= 0:
        return "info"
    frac = count / total
    if frac >= crit:
        return "critical"
    if frac >= warn:
        return "warning"
    return "info"


def _coverage_severity(pct: float) -> Severity:
    """Severity for a "more is better" coverage percentage."""
    if pct >= EMBEDDINGS_READY_THRESHOLD:
        return "ok"
    if pct >= 50.0:
        return "warning"
    return "critical"


# --------------------------------------------------------------------------
# Fix actions — keyed by repair_task. The ``operation_key`` values line up
# with the maintenance registry (task 24 Phase 2); the ``target`` is the
# existing manual endpoint so the action is usable before the registry lands.
# --------------------------------------------------------------------------

_REPAIR_ACTIONS: dict[str, list[dict[str, str]]] = {
    "corpus_metadata": [
        {
            "label": "Rehydrate metadata",
            "kind": "run_now",
            "operation_key": "maintenance.corpus_metadata",
            "target": "/api/v1/publications/rehydrate-metadata",
        }
    ],
    "s2_vector": [
        {
            "label": "Fetch missing S2 vectors",
            "kind": "run_now",
            "operation_key": "maintenance.s2_vector",
            "target": "/api/v1/ai/backfill-s2-vectors",
        }
    ],
    "embedding": [
        {
            "label": "Compute embeddings locally",
            "kind": "run_now",
            "operation_key": "maintenance.embedding",
            "target": "/api/v1/ai/compute-embeddings",
        }
    ],
    "title_resolution": [
        {
            "label": "Resolve missing identity",
            "kind": "run_now",
            "operation_key": "maintenance.title_resolution",
            "target": "/api/v1/ai/title-resolution-sweep",
        }
    ],
}


def _dimension(
    *,
    key: str,
    entity: str,
    label: str,
    count: int,
    total: int,
    severity: Severity,
    explanation: str,
    impact: str = "",
    repair_task: str | None = None,
    coverage_pct: float | None = None,
    scope: str = "corpus",
    extra_actions: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Build one uniform dimension record (the shape every surface reads)."""
    actions = list(_REPAIR_ACTIONS.get(repair_task or "", []))
    if extra_actions:
        actions = actions + extra_actions
    return {
        "key": key,
        "entity": entity,
        "label": label,
        "count": int(count),
        "total": int(total),
        "coverage_pct": coverage_pct,
        "severity": severity,
        "explanation": explanation,
        "impact": impact,
        "repair_task": repair_task,
        "actions": actions,
        "scope": scope,
    }


# --------------------------------------------------------------------------
# Missing-field metadata — drives the per-field paper dimensions.
# (key suffix, label, why-missing clause, impact, repair_task)
# --------------------------------------------------------------------------

_MISSING_FIELD_META: dict[str, tuple[str, str, str, str]] = {
    "abstract": (
        "Missing abstract",
        "have an OpenAlex id but no abstract",
        "Abstracts power embeddings and ranking — without one a paper is hard to embed or recommend.",
        "corpus_metadata",
    ),
    "references": (
        "Missing references",
        "have no stored reference list",
        "References build the citation graph that Discovery and the graph views rely on.",
        "corpus_metadata",
    ),
    "topics": (
        "Missing topics",
        "have no topics",
        "Topics drive topic-overlap scoring and the topic map.",
        "corpus_metadata",
    ),
    "authorships": (
        "Missing authors",
        "have no author rows",
        "Author links feed author tracking, suggestions, and dedup.",
        "corpus_metadata",
    ),
    "doi": (
        "Missing DOI",
        "have an OpenAlex id but no DOI",
        "A DOI is the most reliable cross-source key for vectors and dedup.",
        "corpus_metadata",
    ),
    "publication_date": (
        "Missing publication date",
        "have no publication date",
        "Dates drive recency in Feed and Discovery.",
        "corpus_metadata",
    ),
    "url": (
        "Missing URL",
        "have no landing-page URL",
        "A URL lets you open the paper at the source.",
        "corpus_metadata",
    ),
}


# --------------------------------------------------------------------------
# Sub-signals (cheap reads reused / replicated from existing code)
# --------------------------------------------------------------------------


def _embedding_coverage(conn: sqlite3.Connection) -> dict[str, Any]:
    """Active-model embedding coverage — mirrors ``graphs.py`` exactly."""
    try:
        from alma.discovery.similarity import get_active_embedding_model

        model = get_active_embedding_model(conn)
        emb = conn.execute(
            "SELECT COUNT(*) AS c FROM publication_embeddings WHERE model = ?",
            (model,),
        ).fetchone()
        emb_count = int((emb["c"] if emb else 0) or 0)
    except Exception:
        model = ""
        emb_count = 0
    try:
        pub = conn.execute("SELECT COUNT(*) AS c FROM papers").fetchone()
        pub_count = int((pub["c"] if pub else 0) or 0)
    except Exception:
        pub_count = 0
    pct = round((emb_count / pub_count * 100.0), 1) if pub_count > 0 else 0.0
    return {
        "active_model": model,
        "embeddings_count": emb_count,
        "papers_count": pub_count,
        "coverage_pct": pct,
        "ready": pct >= EMBEDDINGS_READY_THRESHOLD,
    }


def _count_canonical_orphans(conn: sqlite3.Connection) -> int:
    """Papers whose ``canonical_paper_id`` points at a non-existent row."""
    try:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c FROM papers p
            WHERE COALESCE(NULLIF(TRIM(p.canonical_paper_id), ''), '') != ''
              AND NOT EXISTS (
                  SELECT 1 FROM papers c WHERE c.id = p.canonical_paper_id
              )
            """
        ).fetchone()
        return int((row["c"] if row else 0) or 0)
    except sqlite3.OperationalError:
        return 0


# --------------------------------------------------------------------------
# The canonical assessor
# --------------------------------------------------------------------------


def assess_corpus(conn: sqlite3.Connection) -> dict[str, Any]:
    """Canonical corpus-health snapshot — the MV ``health:corpus`` build_fn.

    Returns ``{generated_at, totals, dimensions: [<uniform records>]}``.
    Reuses ``build_enrichment_status`` + the candidate counts + coverage so
    every number reconciles with the surfaces that compute them today.
    """
    from alma.services.corpus_rehydrate import build_enrichment_status
    from alma.services.embedding_chain import (
        _count_local_specter2_candidates,
        _count_s2_fetch_candidates,
    )

    enr = build_enrichment_status(conn)
    papers_total = int(enr.get("papers_total") or 0)
    missing = enr.get("missing") or {}
    coverage = _embedding_coverage(conn)
    s2_missing = _count_s2_fetch_candidates(conn)
    local_computable = _count_local_specter2_candidates(conn)
    orphans = _count_canonical_orphans(conn)
    without_oa = int(enr.get("without_openalex_id") or 0)
    retryable_waiting = int(enr.get("retryable_waiting") or 0)

    dims: list[dict[str, Any]] = []

    # --- Identity ----------------------------------------------------------
    dims.append(
        _dimension(
            key="identity.unresolved",
            entity="identity",
            label="Unresolved identity",
            count=without_oa,
            total=papers_total,
            severity=_severity_from_fraction(without_oa, papers_total),
            explanation=(
                f"{without_oa} papers aren't resolved to an OpenAlex id, so they "
                "lack rich metadata, citations, and topics."
            ),
            impact="OpenAlex resolution unlocks abstracts, references, topics, and vectors.",
            repair_task="title_resolution",
        )
    )
    if orphans:
        dims.append(
            _dimension(
                key="identity.canonical_orphans",
                entity="identity",
                label="Orphaned merge pointers",
                count=orphans,
                total=papers_total,
                severity="warning" if orphans else "ok",
                explanation=(
                    f"{orphans} papers point to a canonical (merged) paper that no "
                    "longer exists — they'll be hidden from normal views."
                ),
                impact="Orphaned merge pointers make papers silently disappear.",
                repair_task=None,
            )
        )

    # --- Per-field metadata gaps ------------------------------------------
    for field, meta in _MISSING_FIELD_META.items():
        count = int(missing.get(field) or 0)
        label, why, impact, repair = meta
        dims.append(
            _dimension(
                key=f"papers.missing_{field}",
                entity="paper",
                label=label,
                count=count,
                total=papers_total,
                severity=_severity_from_fraction(count, papers_total),
                explanation=f"{count} papers {why}.",
                impact=impact,
                repair_task=repair,
            )
        )

    # --- Embeddings --------------------------------------------------------
    dims.append(
        _dimension(
            key="embeddings.coverage",
            entity="embedding",
            label="Embedding coverage",
            count=coverage["embeddings_count"],
            total=coverage["papers_count"],
            coverage_pct=coverage["coverage_pct"],
            severity=_coverage_severity(coverage["coverage_pct"]),
            explanation=(
                f"{coverage['coverage_pct']}% of papers have a vector for the active "
                f"model. Ready at ≥{int(EMBEDDINGS_READY_THRESHOLD)}%."
            ),
            impact="Discovery similarity and the paper map depend on embedding coverage.",
            repair_task=None,
        )
    )
    dims.append(
        _dimension(
            key="embeddings.s2_vector_missing",
            entity="embedding",
            label="Fetchable S2 vectors",
            count=s2_missing,
            total=papers_total,
            severity=_severity_from_fraction(s2_missing, papers_total),
            explanation=(
                f"{s2_missing} papers have a DOI/S2 id and could fetch a precomputed "
                "SPECTER2 vector from Semantic Scholar."
            ),
            impact="Fetched vectors are higher quality than local fallbacks and need no GPU.",
            repair_task="s2_vector",
        )
    )
    dims.append(
        _dimension(
            key="embeddings.local_computable",
            entity="embedding",
            label="Locally computable embeddings",
            count=local_computable,
            total=papers_total,
            severity=_severity_from_fraction(local_computable, papers_total),
            explanation=(
                f"{local_computable} papers have a title + abstract and can be embedded "
                "locally with SPECTER2."
            ),
            impact="Covers papers Semantic Scholar can't supply a vector for.",
            repair_task="embedding",
        )
    )

    # --- Ledger health (informational; the retry clock working as intended)
    if retryable_waiting:
        dims.append(
            _dimension(
                key="ledger.retry_waiting",
                entity="ops",
                label="Waiting to retry",
                count=retryable_waiting,
                total=papers_total,
                severity="info",
                explanation=(
                    f"{retryable_waiting} papers hit a transient error and are cooling "
                    "down before an automatic retry — no action needed."
                ),
                impact="These resolve themselves once their retry clock elapses.",
                repair_task=None,
            )
        )

    by_severity = {s: 0 for s in ("ok", "info", "warning", "critical")}
    for d in dims:
        by_severity[d["severity"]] = by_severity.get(d["severity"], 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "papers_total": papers_total,
            "with_openalex_id": int(enr.get("with_openalex_id") or 0),
            "without_openalex_id": without_oa,
            "eligible_now": int(enr.get("eligible_now") or 0),
            "embedding_coverage_pct": coverage["coverage_pct"],
            "embeddings_ready": coverage["ready"],
            "dimensions_by_severity": by_severity,
        },
        "dimensions": dims,
    }


# --------------------------------------------------------------------------
# Materialised-view registration (cheap <1s reads via mv.get)
# --------------------------------------------------------------------------

# Fingerprint changes exactly when the assessment should: row counts +
# latest-mutation timestamps over papers and the two paper ledgers, plus the
# active embedding-model setting. All subqueries are NULL-safe (a missing
# settings row yields '' rather than an error that would defeat caching).
_HEALTH_CORPUS_FINGERPRINT_SQL = """
    SELECT
      (SELECT COUNT(*) FROM papers WHERE COALESCE(canonical_paper_id,'')=''),
      (SELECT COALESCE(MAX(updated_at),'') FROM papers),
      (SELECT COUNT(*) FROM paper_enrichment_status),
      (SELECT COALESCE(MAX(updated_at),'') FROM paper_enrichment_status),
      (SELECT COUNT(*) FROM publication_embeddings),
      (SELECT COUNT(*) FROM publication_embedding_fetch_status),
      (SELECT COALESCE(MAX(value),'') FROM discovery_settings WHERE key LIKE '%embedding_model%')
"""


mv.register(
    mv.View(
        key=HEALTH_CORPUS_VIEW_KEY,
        fingerprint_sql=_HEALTH_CORPUS_FINGERPRINT_SQL,
        build_fn=assess_corpus,
        operation_key="materialize.health.corpus",
    )
)
