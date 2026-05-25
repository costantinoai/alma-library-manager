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
_AUTHOR_FOLLOWED_UNRESOLVED_SQL = (
    "EXISTS (SELECT 1 FROM followed_authors fa WHERE fa.author_id = a.id) "
    "AND COALESCE(a.openalex_id, '') = ''"
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
    except sqlite3.OperationalError:
        pass

    # Conflict counts come from the same helpers the endpoint uses, so the
    # numbers can't diverge from the rows it renders.
    merge_conflicts = affiliation_conflicts = 0
    try:
        from alma.application.author_merge import list_unresolved_conflicts

        merge_conflicts = len(list_unresolved_conflicts(conn) or [])
    except Exception:
        pass
    try:
        from alma.application.author_affiliation import list_affiliation_conflicts

        affiliation_conflicts = len(list_affiliation_conflicts(conn, limit=500) or [])
    except Exception:
        pass

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
            count=merge_conflicts,
            total=total,
            severity="warning" if merge_conflicts else "ok",
            explanation=(
                f"{merge_conflicts} merges kept a conflicting hard identifier "
                "(orcid / scholar id) that needs a human decision."
            ),
            impact="A wrong identifier can mis-attribute papers across people.",
            extra_actions=_AUTHOR_REVIEW_ACTION,
        ),
        _dimension(
            key="authors.affiliation_conflicts",
            entity="author",
            label="Affiliation conflicts",
            count=affiliation_conflicts,
            total=total,
            severity="info" if affiliation_conflicts else "ok",
            explanation=(
                f"{affiliation_conflicts} authors have affiliation evidence that "
                "disagrees across sources."
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
            "attention_total": n_error
            + n_no_match
            + n_review
            + n_followed_unresolved
            + merge_conflicts
            + affiliation_conflicts,
            "dimensions_by_severity": by_severity,
        },
        "dimensions": dims,
    }


_HEALTH_AUTHORS_FINGERPRINT_SQL = """
    SELECT
      (SELECT COUNT(*) FROM authors),
      (SELECT COALESCE(MAX(id_resolution_updated_at), '') FROM authors),
      (SELECT COALESCE(MAX(last_fetched_at), '') FROM authors),
      (SELECT COUNT(*) FROM followed_authors)
"""


mv.register(
    mv.View(
        key=HEALTH_AUTHORS_VIEW_KEY,
        fingerprint_sql=_HEALTH_AUTHORS_FINGERPRINT_SQL,
        build_fn=assess_authors,
        operation_key="materialize.health.authors",
    )
)
