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

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable, Tuple

from alma.application import materialized_views as mv
from alma.core.sql_helpers import canonical_paper_filter

logger = logging.getLogger(__name__)

# embeddings_ready flips true at this coverage % (user decision 2026-05-25).
EMBEDDINGS_READY_THRESHOLD = 80.0

# Dimension measurement states (H-2). The UI renders these distinctly so a real
# failure can never masquerade as a healthy "0". ``measured`` is the normal path.
DIM_MEASURED = "measured"
DIM_ERROR = "error"  # the assessor raised — count is unknown, NOT zero


def _safe_assess(label: str, fn: Callable[[], Any]) -> Tuple[Any, bool]:
    """Run a health assessor; on failure log LOUDLY and signal it (H-2).

    Returns ``(value, ok)``. ``ok=False`` means the assessor raised — the caller
    MUST render a typed ``error`` state, never a healthy zero. A missing table /
    malformed migration / SQL regression must look broken, not green (the
    project's no-silent-failure rule). The traceback goes to the log with the
    assessor label so the failure is actionable.
    """
    try:
        return fn(), True
    except Exception as exc:  # noqa: BLE001 — deliberately broad: ANY failure must be loud, not silent
        logger.error("health assessor %r failed: %s", label, exc, exc_info=True)
        return None, False

HEALTH_CORPUS_VIEW_KEY = "health:corpus"

Severity = str  # "ok" | "info" | "warning" | "critical"


# --------------------------------------------------------------------------
# Papers needs-attention predicates — the single source of truth for the
# "this Library paper has a concrete gap" thresholds. The Library landing
# card (``library.py`` ``get_library_workflow_summary``) builds its per-row
# flag columns, its ``issue_count``, AND its ``WHERE``/count clauses from
# these, so the surfaced rows and the headline count can never drift apart
# (they used to repeat the same predicate three times by hand).
#
# Column references are unqualified so a fragment drops straight into any
# ``... FROM papers`` query; the caller adds the membership scope (e.g.
# ``status = 'library'``). Each key is the ``attention_reasons`` ``code``
# the frontend switches on — do not rename without updating the UI.
# Insertion order defines flag-column / issue_count order.
# --------------------------------------------------------------------------

PAPER_ATTENTION_PREDICATES: dict[str, str] = {
    "no_identifier": (
        "(openalex_id IS NULL OR TRIM(openalex_id) = '') "
        "AND (doi IS NULL OR TRIM(doi) = '')"
    ),
    "no_abstract": "abstract IS NULL OR LENGTH(TRIM(abstract)) < 40",
    "no_authors": "authors IS NULL OR LENGTH(TRIM(authors)) < 3",
    "enrichment_stuck": (
        "openalex_resolution_status IN "
        "('pending_enrichment', 'not_openalex_resolved', 'failed')"
    ),
}


def paper_attention_flag_columns_sql() -> str:
    """``CASE WHEN <pred> THEN 1 ELSE 0 END AS flag_<code>`` for every predicate.

    Drop into a SELECT list; the resulting ``flag_<code>`` columns are what
    the Library summary's ``_attention_reasons`` reads by name.
    """
    return ",\n".join(
        f"CASE WHEN {pred} THEN 1 ELSE 0 END AS flag_{code}"
        for code, pred in PAPER_ATTENTION_PREDICATES.items()
    )


def paper_attention_issue_count_sql(alias: str = "issue_count") -> str:
    """Sum of the predicate flags, aliased (drives the needs-attention ordering)."""
    terms = "\n    + ".join(
        f"CASE WHEN {pred} THEN 1 ELSE 0 END"
        for pred in PAPER_ATTENTION_PREDICATES.values()
    )
    return f"({terms}) AS {alias}"


def paper_attention_where_sql() -> str:
    """OR of every predicate — a row qualifies if *any* single gap is present."""
    return "\n     OR ".join(
        f"({pred})" for pred in PAPER_ATTENTION_PREDICATES.values()
    )


# --------------------------------------------------------------------------
# Authors needs-attention ladder — single source of truth for which author
# rows the identity resolver couldn't finish and in what severity order.
# Shared so the ``/authors/needs-attention`` row query AND ``assess_authors``
# below select / rank the same buckets (the ladder lived only in the endpoint
# before, so author health had no canonical counterpart). Column refs use the
# ``a.`` alias both callers give the ``authors`` table; status strings are the
# resolver state-machine enum.
# --------------------------------------------------------------------------

# Resolution statuses that qualify a row for attention. 'unresolved' qualifies
# (it lands in the WHERE) but carries no dedicated severity rank → falls to 9.
AUTHOR_ATTENTION_STATUSES: tuple[str, ...] = (
    "error",
    "no_match",
    "needs_manual_review",
    "unresolved",
)

# Status → severity rank (lower = surface first). Mirrors the endpoint's CASE.
_AUTHOR_STATUS_RANK: tuple[tuple[str, int], ...] = (
    ("error", 0),
    ("no_match", 1),
    ("needs_manual_review", 2),
)

# A *followed* author with no OpenAlex id — the resolver never bridged them.
# Ranks just below the explicit failure statuses (severity 3 in the endpoint).
# ``dismissed`` is the user's terminal "can't be identified — stop flagging"
# acknowledgment: excluded here so an accepted author never re-surfaces as a
# fixable gap (the exact-status counts already skip it, being a new status).
_AUTHOR_FOLLOWED_UNRESOLVED_SQL = (
    "EXISTS (SELECT 1 FROM followed_authors fa WHERE fa.author_id = a.id) "
    "AND COALESCE(a.openalex_id, '') = '' "
    "AND COALESCE(a.id_resolution_status, '') != 'dismissed'"
)


def author_attention_severity_case_sql(alias: str = "severity") -> str:
    """The ``CASE … END AS severity`` that ranks attention rows (lower first)."""
    whens = "\n".join(
        f"                WHEN COALESCE(a.id_resolution_status, '') = '{status}' THEN {rank}"
        for status, rank in _AUTHOR_STATUS_RANK
    )
    return (
        "CASE\n"
        f"{whens}\n"
        f"                WHEN {_AUTHOR_FOLLOWED_UNRESOLVED_SQL} THEN 3\n"
        "                ELSE 9\n"
        f"            END AS {alias}"
    )


def author_attention_where_sql() -> str:
    """The ``WHERE`` predicate selecting every author row that needs attention."""
    status_list = ", ".join(f"'{s}'" for s in AUTHOR_ATTENTION_STATUSES)
    return (
        f"COALESCE(a.id_resolution_status, '') IN ({status_list})\n"
        f"           OR ({_AUTHOR_FOLLOWED_UNRESOLVED_SQL})"
    )


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

# ``operation_key`` is the maintenance-registry task key (== repair_task), so a
# dimension's action maps directly to POST /api/v1/health/operations/{key}/run.
# ``target`` is the bounded run-now endpoint for that task.
_REPAIR_ACTIONS: dict[str, list[dict[str, str]]] = {
    "corpus_metadata": [
        {
            "label": "Rehydrate metadata",
            "kind": "run_now",
            "operation_key": "corpus_metadata",
            "target": "/api/v1/health/operations/corpus_metadata/run",
        }
    ],
    "s2_vector": [
        {
            "label": "Fetch missing S2 vectors",
            "kind": "run_now",
            "operation_key": "s2_vector",
            "target": "/api/v1/health/operations/s2_vector/run",
        }
    ],
    "embedding": [
        {
            "label": "Compute embeddings locally",
            "kind": "run_now",
            "operation_key": "embedding",
            "target": "/api/v1/health/operations/embedding/run",
        }
    ],
    "title_resolution": [
        {
            "label": "Resolve missing identity",
            "kind": "run_now",
            "operation_key": "title_resolution",
            "target": "/api/v1/health/operations/title_resolution/run",
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
    exhausted: int | None = None,
    state: str = DIM_MEASURED,
) -> dict[str, Any]:
    """Build one uniform dimension record (the shape every surface reads).

    ``exhausted`` (when given) is the subset of this gap that no repair op can
    fix — tried and terminal (e.g. Semantic Scholar has no vector for them). The
    UI splits it out so the user isn't surprised that Run-now skips them.

    ``state`` (H-2): ``measured`` normally, ``error`` when the assessor failed —
    then ``count`` is ``None`` (unknown), NOT ``0``, so the UI shows "couldn't
    measure" instead of a misleading healthy zero.
    """
    actions = list(_REPAIR_ACTIONS.get(repair_task or "", []))
    if extra_actions:
        actions = actions + extra_actions
    return {
        "key": key,
        "entity": entity,
        "label": label,
        "count": int(count) if count is not None else None,
        "total": int(total) if total is not None else None,
        "coverage_pct": coverage_pct,
        "severity": severity,
        "explanation": explanation,
        "impact": impact,
        "repair_task": repair_task,
        "actions": actions,
        "scope": scope,
        "exhausted": int(exhausted) if exhausted is not None else None,
        "state": state,
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


def embedding_coverage(
    conn: sqlite3.Connection, model: str | None = None
) -> dict[str, Any]:
    """Canonical embedding-coverage definition: active-model vectors / CANONICAL
    papers.

    This is the single source of truth for the headline coverage % — both the
    Health snapshot (``assess_corpus``) and Settings' ``/ai/status`` call it so
    the two surfaces can never report a different number. ``model`` defaults to
    ``get_active_embedding_model(conn)`` (the ``discovery_settings.embedding_model``
    setting); callers that track a provider-specific model (``/ai/status``) pass
    it explicitly so their headline matches their own per-model breakdown.
    (``graphs.py`` still keeps an equivalent inline copy — folding it in is a
    separate DRY follow-up.)

    H-1: numerator AND denominator are restricted to CANONICAL papers (via
    ``canonical_paper_filter``) — the same universe the ``embeddings.coverage``
    drilldown uses — so the headline % reconciles with the affected-items list
    (covered + missing = canonical total). Counting merged-away alias rows here
    (as it did before) inflated both counts with papers the drilldown could never
    show, and could even push the numerator above the denominator.
    """
    # H-2: a SQL/schema failure must NOT silently read as 0% healthy coverage.
    # Log loudly and flag ``error`` so callers render a "couldn't measure" state
    # (coverage_pct=None, ready=False) — never a misleading zero.
    error: str | None = None
    try:
        if model is None:
            from alma.discovery.similarity import get_active_embedding_model

            model = get_active_embedding_model(conn)
        emb = conn.execute(
            f"""
            SELECT COUNT(*) AS c
            FROM publication_embeddings pe
            JOIN papers p ON p.id = pe.paper_id
            WHERE pe.model = ? AND {canonical_paper_filter('p')}
            """,
            (model,),
        ).fetchone()
        emb_count = int((emb["c"] if emb else 0) or 0)
    except Exception as exc:  # noqa: BLE001
        logger.error("embedding_coverage: embedding count failed: %s", exc, exc_info=True)
        model = ""
        emb_count = 0
        error = "embedding count failed"
    try:
        pub = conn.execute(
            f"SELECT COUNT(*) AS c FROM papers p WHERE {canonical_paper_filter('p')}"
        ).fetchone()
        pub_count = int((pub["c"] if pub else 0) or 0)
    except Exception as exc:  # noqa: BLE001
        logger.error("embedding_coverage: paper count failed: %s", exc, exc_info=True)
        pub_count = 0
        error = "paper count failed"
    pct = round((emb_count / pub_count * 100.0), 1) if pub_count > 0 else 0.0
    return {
        "active_model": model,
        "embeddings_count": emb_count,
        "papers_count": pub_count,
        "coverage_pct": None if error else pct,
        "ready": error is None and pct >= EMBEDDINGS_READY_THRESHOLD,
        "error": error,
    }


def _count_canonical_orphans(conn: sqlite3.Connection) -> int:
    """Papers whose ``canonical_paper_id`` points at a non-existent row.

    No internal swallow — the caller runs this through ``_safe_assess`` (H-2), so
    a failure is logged loudly and surfaced as an ``error`` state, not a silent 0.
    """
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
        _count_s2_fetch_terminal,
    )

    # H-2: every assessor runs through _safe_assess, so a failure becomes a typed
    # ``error`` dimension (loud in the log, count=None in the UI) — never a
    # healthy-looking zero. The per-field metadata dims all derive from ``enr``,
    # so they share its state.
    enr, enr_ok = _safe_assess("enrichment_status", lambda: build_enrichment_status(conn))
    enr = enr or {}
    enr_state = DIM_MEASURED if enr_ok else DIM_ERROR
    papers_total = int(enr.get("papers_total") or 0)
    missing = enr.get("missing") or {}
    coverage = embedding_coverage(conn)  # carries its own ``error`` flag
    coverage_state = DIM_ERROR if coverage.get("error") else DIM_MEASURED
    s2_missing, s2_ok = _safe_assess("s2_fetch_candidates", lambda: _count_s2_fetch_candidates(conn))
    s2_terminal, _ = _safe_assess("s2_fetch_terminal", lambda: _count_s2_fetch_terminal(conn))
    local_computable, local_ok = _safe_assess(
        "local_specter2_candidates", lambda: _count_local_specter2_candidates(conn)
    )
    orphans, orphans_ok = _safe_assess("canonical_orphans", lambda: _count_canonical_orphans(conn))
    without_oa = int(enr.get("without_openalex_id") or 0)
    retryable_waiting = int(enr.get("retryable_waiting") or 0)

    dims: list[dict[str, Any]] = []

    # --- Identity ----------------------------------------------------------
    dims.append(
        _dimension(
            key="identity.unresolved",
            entity="identity",
            label="Unresolved identity",
            count=without_oa if enr_ok else None,
            total=papers_total,
            state=enr_state,
            severity=_severity_from_fraction(without_oa, papers_total) if enr_ok else "warning",
            explanation=(
                f"{without_oa} papers aren't resolved to an OpenAlex id, so they "
                "lack rich metadata, citations, and topics. Resolve missing identity "
                "(Semantic Scholar title search) is the fix — including papers a previous "
                "search left stuck as 'unmatched'; until they resolve they can't be "
                "enriched or embedded."
            ),
            impact="OpenAlex resolution unlocks abstracts, references, topics, and vectors.",
            repair_task="title_resolution",
        )
    )
    if orphans_ok and orphans:  # on assessor failure: skip (already loud in the log), don't claim 0
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
                count=count if enr_ok else None,
                total=papers_total,
                state=enr_state,
                severity=_severity_from_fraction(count, papers_total) if enr_ok else "warning",
                explanation=(f"{count} papers {why}." if enr_ok else "Couldn't measure — see logs."),
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
            count=coverage["embeddings_count"] if coverage_state == DIM_MEASURED else None,
            total=coverage["papers_count"] if coverage_state == DIM_MEASURED else None,
            coverage_pct=coverage["coverage_pct"],
            state=coverage_state,
            severity=(
                _coverage_severity(coverage["coverage_pct"])
                if coverage_state == DIM_MEASURED
                else "warning"
            ),
            explanation=(
                f"{coverage['coverage_pct']}% of papers have a vector for the active "
                f"model. Ready at ≥{int(EMBEDDINGS_READY_THRESHOLD)}%."
                if coverage_state == DIM_MEASURED
                else "Couldn't measure embedding coverage — see logs."
            ),
            impact="Discovery similarity and the paper map depend on embedding coverage.",
            # Coverage itself isn't a single runner; it improves by fetching S2
            # vectors and computing local ones, so it carries both as actions.
            repair_task=None,
            extra_actions=_REPAIR_ACTIONS["s2_vector"] + _REPAIR_ACTIONS["embedding"],
        )
    )
    dims.append(
        _dimension(
            key="embeddings.s2_vector_missing",
            entity="embedding",
            label="Fetchable S2 vectors",
            count=s2_missing if s2_ok else None,
            total=papers_total,
            state=DIM_MEASURED if s2_ok else DIM_ERROR,
            severity=_severity_from_fraction(s2_missing, papers_total) if s2_ok else "warning",
            explanation=(
                f"{s2_missing} papers have a DOI/S2 id and could fetch a precomputed "
                "SPECTER2 vector from Semantic Scholar. Papers that Semantic Scholar "
                "has no vector for fall through to local compute below."
                if s2_ok
                else "Couldn't measure — see logs."
            ),
            impact="Fetched vectors are higher quality than local fallbacks and need no GPU.",
            repair_task="s2_vector",
            # Tried + terminal at S2 (no vector / unmatched): not re-fetched — only
            # local compute can help. Split out so they don't read as actionable here.
            exhausted=s2_terminal,
        )
    )
    dims.append(
        _dimension(
            key="embeddings.local_computable",
            entity="embedding",
            label="Locally computable embeddings",
            count=local_computable if local_ok else None,
            total=papers_total,
            state=DIM_MEASURED if local_ok else DIM_ERROR,
            severity=_severity_from_fraction(local_computable, papers_total) if local_ok else "warning",
            explanation=(
                f"{local_computable} papers have a title + abstract and can be embedded "
                "locally with SPECTER2 — this is the only fix for papers Semantic Scholar "
                "has no vector for. Papers missing a title or abstract can't be embedded "
                "at all; fix those via metadata rehydration first (see the missing-abstract "
                "/ missing-title gaps above)."
                if local_ok
                else "Couldn't measure — see logs."
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
#
# The leading logic-version literal forces a one-time rebuild whenever the
# *assessment logic* changes (the data fingerprint can't see code changes — e.g.
# adding actions to a dimension). Bump it when assess_corpus' output shape /
# dimensions / actions change.
# H-1: the paper count AND the embedding count are both over the CANONICAL
# universe (the helper), matching embedding_coverage's numerator/denominator —
# the fingerprint used to mix canonical papers with ALL embedding rows, so an
# alias gaining/losing a vector wouldn't invalidate the snapshot. v4→v5 forces
# one rebuild so the corrected coverage lands.
_HEALTH_CORPUS_FINGERPRINT_SQL = f"""
    SELECT
      'health-logic-v6',
      (SELECT COUNT(*) FROM papers p WHERE {canonical_paper_filter('p')}),
      (SELECT COALESCE(MAX(updated_at),'') FROM papers),
      (SELECT COUNT(*) FROM paper_enrichment_status),
      (SELECT COALESCE(MAX(updated_at),'') FROM paper_enrichment_status),
      (SELECT COUNT(*) FROM publication_embeddings pe
         JOIN papers p ON p.id = pe.paper_id WHERE {canonical_paper_filter('p')}),
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


# --------------------------------------------------------------------------
# Author identity health — the canonical counterpart to assess_corpus.
# --------------------------------------------------------------------------

HEALTH_AUTHORS_VIEW_KEY = "health:authors"

_AUTHOR_REVIEW_ACTION = [
    {
        "label": "Review on the Authors page",
        "kind": "link",
        "operation_key": "",
        "target": "/api/v1/authors/needs-attention",
    }
]


def assess_authors(conn: sqlite3.Connection) -> dict[str, Any]:
    """Canonical author-identity-health snapshot — the ``health:authors`` build_fn.

    Counts the same attention buckets the ``/authors/needs-attention`` endpoint
    surfaces (via the shared ladder above) plus the unresolved-merge and
    affiliation conflicts, so the Health page author cards reconcile with the
    needs-attention list by construction. Read via ``mv.get`` like the corpus
    assessor; degrades to zeros if the resolution columns aren't present yet.
    """
    total = 0
    n_error = n_no_match = n_review = n_followed_unresolved = 0
    try:
        row = conn.execute(
            f"""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN COALESCE(a.id_resolution_status, '') = 'error'
                       THEN 1 ELSE 0 END) AS n_error,
              SUM(CASE WHEN COALESCE(a.id_resolution_status, '') = 'no_match'
                       THEN 1 ELSE 0 END) AS n_no_match,
              SUM(CASE WHEN COALESCE(a.id_resolution_status, '') = 'needs_manual_review'
                       THEN 1 ELSE 0 END) AS n_review,
              SUM(CASE WHEN ({_AUTHOR_FOLLOWED_UNRESOLVED_SQL})
                       THEN 1 ELSE 0 END) AS n_followed_unresolved
            FROM authors a
            """
        ).fetchone()
        if row:
            total = int(row["total"] or 0)
            n_error = int(row["n_error"] or 0)
            n_no_match = int(row["n_no_match"] or 0)
            n_review = int(row["n_review"] or 0)
            n_followed_unresolved = int(row["n_followed_unresolved"] or 0)
    except sqlite3.OperationalError as exc:
        # Documented degradation: the id_resolution_* columns may not exist yet on
        # a mid-migration DB → zeros are the right answer there. But log it (H-2)
        # so a real SQL regression isn't fully silent.
        logger.warning("author identity counts unavailable (schema not ready?): %s", exc)

    # Conflict counts come from the same helpers the endpoint uses, so the
    # numbers can't diverge from the rows it renders. H-2: a failed assessor must
    # surface an ``error`` state (loud in the log), never a healthy 0.
    def _count_merge_conflicts() -> int:
        from alma.application.author_merge import list_unresolved_conflicts

        return len(list_unresolved_conflicts(conn) or [])

    def _count_affiliation_conflicts() -> int:
        from alma.application.author_affiliation import list_affiliation_conflicts

        return len(list_affiliation_conflicts(conn, limit=500) or [])

    merge_conflicts, merge_ok = _safe_assess("author_merge_conflicts", _count_merge_conflicts)
    affiliation_conflicts, affil_ok = _safe_assess(
        "author_affiliation_conflicts", _count_affiliation_conflicts
    )

    dims: list[dict[str, Any]] = [
        _dimension(
            key="authors.resolution_error",
            entity="author",
            label="Refresh errors",
            count=n_error,
            total=total,
            severity=_severity_from_fraction(n_error, total),
            explanation=f"{n_error} authors hit an exception on their last identity refresh.",
            impact="Their profile, affiliation, and corpus can't update until the refresh succeeds.",
            extra_actions=_AUTHOR_REVIEW_ACTION,
        ),
        _dimension(
            key="authors.no_match",
            entity="author",
            label="No OpenAlex match",
            count=n_no_match,
            total=total,
            severity=_severity_from_fraction(n_no_match, total),
            explanation=f"{n_no_match} authors returned zero OpenAlex candidates by name.",
            impact="Without an OpenAlex id an author can't be tracked or deduped automatically.",
            extra_actions=_AUTHOR_REVIEW_ACTION,
        ),
        _dimension(
            key="authors.needs_review",
            entity="author",
            label="Ambiguous candidates",
            count=n_review,
            total=total,
            severity="info" if n_review else "ok",
            explanation=f"{n_review} authors had multiple OpenAlex candidates scored too close to auto-pick.",
            impact="A human pick resolves the identity; until then the author stays unlinked.",
            extra_actions=_AUTHOR_REVIEW_ACTION,
        ),
        _dimension(
            key="authors.followed_unresolved",
            entity="author",
            label="Followed but unresolved",
            count=n_followed_unresolved,
            total=total,
            severity="warning" if n_followed_unresolved else "ok",
            explanation=(
                f"{n_followed_unresolved} followed authors still have no OpenAlex id, "
                "so their feed and corpus can't refresh cleanly."
            ),
            impact="A followed author with no identity bridge produces no new matches.",
            extra_actions=_AUTHOR_REVIEW_ACTION,
        ),
        _dimension(
            key="authors.merge_conflicts",
            entity="author",
            label="Unresolved merge conflicts",
            count=merge_conflicts if merge_ok else None,
            total=total,
            state=DIM_MEASURED if merge_ok else DIM_ERROR,
            severity=("warning" if merge_conflicts else "ok") if merge_ok else "warning",
            explanation=(
                f"{merge_conflicts} merges kept a conflicting hard identifier "
                "(orcid / scholar id) that needs a human decision."
                if merge_ok
                else "Couldn't measure merge conflicts — see logs."
            ),
            impact="A wrong identifier can mis-attribute papers across people.",
            extra_actions=_AUTHOR_REVIEW_ACTION,
        ),
        _dimension(
            key="authors.affiliation_conflicts",
            entity="author",
            label="Affiliation conflicts",
            count=affiliation_conflicts if affil_ok else None,
            total=total,
            state=DIM_MEASURED if affil_ok else DIM_ERROR,
            severity=("info" if affiliation_conflicts else "ok") if affil_ok else "warning",
            explanation=(
                f"{affiliation_conflicts} authors have affiliation evidence that "
                "disagrees across sources."
                if affil_ok
                else "Couldn't measure affiliation conflicts — see logs."
            ),
            impact="The displayed institution may be wrong until reviewed.",
            extra_actions=_AUTHOR_REVIEW_ACTION,
        ),
    ]

    by_severity = {s: 0 for s in ("ok", "info", "warning", "critical")}
    for d in dims:
        by_severity[d["severity"]] = by_severity.get(d["severity"], 0) + 1

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "authors_total": total,
            # `or 0`: a failed conflict assessor (None) must not crash the total —
            # its dimension already carries the error state.
            "attention_total": n_error
            + n_no_match
            + n_review
            + n_followed_unresolved
            + (merge_conflicts or 0)
            + (affiliation_conflicts or 0),
            "dimensions_by_severity": by_severity,
        },
        "dimensions": dims,
    }


# H-3: every input to assess_authors must appear here or the ribbon goes stale.
# The leading logic-version literal forces a one-time rebuild on assessment-logic
# changes (the data fingerprint can't see code changes); bump it when the authors
# output shape / dimensions / actions change.
_HEALTH_AUTHORS_FINGERPRINT_SQL = """
    SELECT
      'health-authors-v2',
      (SELECT COUNT(*) FROM authors),
      (SELECT COALESCE(MAX(id_resolution_updated_at), '') FROM authors),
      (SELECT COALESCE(MAX(last_fetched_at), '') FROM authors),
      (SELECT COUNT(*) FROM followed_authors),
      -- Affiliation-conflict counts feed this view, so any evidence change
      -- (a manual pick OR an auto-refresh replacing source rows) must
      -- invalidate it — otherwise the ribbon shows a stale conflict count.
      (SELECT COUNT(*) FROM author_affiliation_evidence),
      (SELECT COALESCE(MAX(observed_at), '') FROM author_affiliation_evidence),
      -- H-3: unresolved merge-conflict count (the dimension's metric) + a
      -- mutation marker, so creating OR resolving a conflict rebuilds the view
      -- even when the net count is unchanged (one resolved, one created).
      (SELECT COUNT(*) FROM author_merge_conflicts WHERE status = 'unresolved'),
      (SELECT COALESCE(MAX(created_at), '') || '|' || COALESCE(MAX(resolved_at), '')
         FROM author_merge_conflicts)
"""


mv.register(
    mv.View(
        key=HEALTH_AUTHORS_VIEW_KEY,
        fingerprint_sql=_HEALTH_AUTHORS_FINGERPRINT_SQL,
        build_fn=assess_authors,
        operation_key="materialize.health.authors",
    )
)


# --------------------------------------------------------------------------
# Dimension drilldown — list the papers affected by a dimension, so the Health
# page can show "which papers" + per-issue fix operations. Read-only; the
# predicates mirror the counts in build_enrichment_status / the candidate
# counters so the list matches the card. Always paginated (these scan papers).
# --------------------------------------------------------------------------

_SPECTER2_MODEL = "allenai/specter2_base"
_HAS_OA = "COALESCE(NULLIF(TRIM(p.openalex_id), ''), '') <> ''"

# dim key → WHERE predicate (alias ``p`` = papers). Special dims that need a
# join (s2 vectors, coverage, retry) are handled separately in dimension_items.
_DIMENSION_PREDICATES: dict[str, str] = {
    "identity.unresolved": (
        "COALESCE(NULLIF(TRIM(p.openalex_id), ''), '') = '' "
        "AND COALESCE(p.canonical_paper_id, '') = ''"
    ),
    "papers.missing_abstract": f"{_HAS_OA} AND COALESCE(NULLIF(TRIM(p.abstract), ''), '') = ''",
    "papers.missing_doi": f"{_HAS_OA} AND COALESCE(NULLIF(TRIM(p.doi), ''), '') = ''",
    "papers.missing_url": f"{_HAS_OA} AND COALESCE(NULLIF(TRIM(p.url), ''), '') = ''",
    "papers.missing_publication_date": (
        f"{_HAS_OA} AND COALESCE(NULLIF(TRIM(p.publication_date), ''), '') = ''"
    ),
    "papers.missing_authorships": (
        f"{_HAS_OA} AND NOT EXISTS "
        "(SELECT 1 FROM publication_authors pa WHERE pa.paper_id = p.id)"
    ),
    "papers.missing_topics": (
        f"{_HAS_OA} AND NOT EXISTS "
        "(SELECT 1 FROM publication_topics pt WHERE pt.paper_id = p.id)"
    ),
    "papers.missing_references": (
        f"{_HAS_OA} AND NOT EXISTS "
        "(SELECT 1 FROM publication_references pr WHERE pr.paper_id = p.id)"
    ),
    "embeddings.local_computable": (
        "NOT EXISTS (SELECT 1 FROM publication_embeddings pe "
        f"WHERE pe.paper_id = p.id AND pe.model = '{_SPECTER2_MODEL}') "
        "AND COALESCE(NULLIF(TRIM(p.title), ''), '') <> '' "
        "AND COALESCE(NULLIF(TRIM(p.abstract), ''), '') <> ''"
    ),
}

# Short, dimension-specific "what's wrong with this row" label.
_DIMENSION_DETAIL: dict[str, str] = {
    "identity.unresolved": "No OpenAlex id",
    "papers.missing_abstract": "Abstract empty",
    "papers.missing_doi": "No DOI",
    "papers.missing_url": "No URL",
    "papers.missing_publication_date": "No publication date",
    "papers.missing_authorships": "No author rows",
    "papers.missing_topics": "No topics",
    "papers.missing_references": "No references",
    "embeddings.local_computable": "Embeddable locally (SPECTER2)",
    "embeddings.s2_vector_missing": "Vector fetchable from Semantic Scholar",
    "embeddings.coverage": "No vector for the active model",
    "ledger.retry_waiting": "Cooling down before retry",
}

_ITEM_COLUMNS = (
    "p.id AS paper_id, p.title, p.publication_date, p.authors, p.status, "
    "p.doi, p.openalex_id, COALESCE(p.openalex_resolution_status, '') AS resolution_status"
)


def dimension_items(
    conn: sqlite3.Connection, key: str, *, limit: int = 20, offset: int = 0
) -> list[dict[str, Any]]:
    """Paginated list of papers affected by dimension ``key`` (read-only)."""
    limit = max(1, min(int(limit), 100))
    offset = max(0, int(offset))
    order = "ORDER BY COALESCE(p.publication_date, '') DESC, p.title"
    extra = ""  # extra selected column appended for special dims

    pred = _DIMENSION_PREDICATES.get(key)
    if pred is not None:
        sql = f"SELECT {_ITEM_COLUMNS}, '' AS extra FROM papers p WHERE {pred} {order} LIMIT ? OFFSET ?"
        params: tuple[Any, ...] = (limit, offset)
    elif key == "embeddings.s2_vector_missing":
        sql = f"""
            SELECT {_ITEM_COLUMNS}, '' AS extra
            FROM papers p
            LEFT JOIN publication_embedding_fetch_status fs
              ON fs.paper_id = p.id AND fs.model = '{_SPECTER2_MODEL}'
                 AND fs.source = 'semantic_scholar'
            WHERE (
                COALESCE(NULLIF(TRIM(p.semantic_scholar_id), ''), '') <> ''
                OR COALESCE(NULLIF(TRIM(p.doi), ''), '') <> ''
            )
            AND NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id AND pe.model = '{_SPECTER2_MODEL}'
                  AND pe.source = 'semantic_scholar'
            )
            AND COALESCE(fs.status, '') NOT IN
                ('unmatched', 'missing_vector', 'lookup_error', 'bad_local_doi')
            {order} LIMIT ? OFFSET ?
        """
        params = (limit, offset)
    elif key == "embeddings.coverage":
        try:
            from alma.discovery.similarity import get_active_embedding_model

            model = get_active_embedding_model(conn)
        except Exception:
            model = _SPECTER2_MODEL
        # Same CANONICAL universe + model as embedding_coverage()'s denominator,
        # so this affected-items list == (canonical total − covered) (H-1).
        sql = f"""
            SELECT {_ITEM_COLUMNS}, '' AS extra
            FROM papers p
            WHERE {canonical_paper_filter('p')}
              AND NOT EXISTS (
                SELECT 1 FROM publication_embeddings pe
                WHERE pe.paper_id = p.id AND pe.model = ?
              )
            {order} LIMIT ? OFFSET ?
        """
        params = (model, limit, offset)
    elif key == "ledger.retry_waiting":
        sql = f"""
            SELECT {_ITEM_COLUMNS}, COALESCE(es.next_retry_at, '') AS extra
            FROM papers p
            JOIN paper_enrichment_status es ON es.paper_id = p.id
            WHERE es.source = 'openalex' AND es.purpose = 'metadata'
              AND es.status = 'retryable_error'
              AND es.next_retry_at IS NOT NULL
              AND es.next_retry_at > strftime('%Y-%m-%dT%H:%M:%f000+00:00', 'now')
            ORDER BY es.next_retry_at ASC LIMIT ? OFFSET ?
        """
        params = (limit, offset)
    else:
        return []

    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError as exc:
        # H-2: an empty drilldown must not silently read as "no affected papers"
        # when the query actually FAILED. Log loudly; the caller can still render
        # an empty list, but the failure is now visible/actionable.
        logger.error("dimension_items drilldown %r failed: %s", key, exc, exc_info=True)
        return []

    out: list[dict[str, Any]] = []
    base_detail = _DIMENSION_DETAIL.get(key, "")
    for r in rows:
        detail = base_detail
        if key == "ledger.retry_waiting" and r["extra"]:
            detail = f"Retry at {r['extra']}"
        elif key == "identity.unresolved" and r["resolution_status"]:
            detail = f"Resolution: {r['resolution_status']}"
        out.append(
            {
                "paper_id": r["paper_id"],
                "title": r["title"] or "(untitled)",
                "publication_date": r["publication_date"] or None,
                "authors": r["authors"] or None,
                "status": r["status"],
                "doi": r["doi"] or None,
                "openalex_id": r["openalex_id"] or None,
                "detail": detail,
            }
        )
    return out


# Valid drilldown keys = simple predicates + the special-cased dims.
DIMENSION_ITEM_KEYS: frozenset[str] = frozenset(_DIMENSION_PREDICATES) | {
    "embeddings.s2_vector_missing",
    "embeddings.coverage",
    "ledger.retry_waiting",
}
