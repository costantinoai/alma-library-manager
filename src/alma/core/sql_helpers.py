"""Shared SQL fragment builders.

Recurring `COALESCE(...)` patterns that lived in 6+ route / application
files. Centralizing them means a calibration change (e.g. changing the
year-only fallback away from the lossy ``YYYY-01-01`` and toward
``COALESCE(publication_date, fetched_at)``) is one diff away from
shipping everywhere.
"""

from __future__ import annotations

from alma.core.components import not_component_sql


def paper_date_sort_expr(alias: str = "", *, added_at_fallback: bool = False) -> str:
    """SQL expression that orders papers by publication date with a year fallback.

    Returns ``COALESCE({prefix}publication_date, printf('%04d-01-01',
    COALESCE({prefix}year, 0)), {tail})`` where ``prefix`` is the optional
    table alias (e.g. ``'p.'``).

    The empty-string sentinel is intentional: SQLite's ASC sort puts
    empty strings before any populated date, which keeps "no date at
    all" rows at the bottom in DESC order. The ``YYYY-01-01`` fallback
    is the same year-only convention `tasks/lessons.md` ("Don't
    fabricate missing timestamps") allows for *display ordering* but
    forbids in storage.

    ``added_at_fallback`` swaps the bare ``''`` sentinel for
    ``COALESCE({prefix}added_at, {prefix}created_at, '')`` so a paper with
    neither publication_date nor year still sorts by when it entered the
    library — what the author-papers and publications "recent" sorts want
    (both append a ``cited_by_count`` tiebreak of their own). Requires the
    aliased table to carry ``added_at``/``created_at`` (the ``papers`` table).

    Use this everywhere a paper list needs a stable date sort. Do not
    inline the SQL — six sites used to drift.
    """
    prefix = f"{alias}." if alias and not alias.endswith(".") else alias
    tail = f"COALESCE({prefix}added_at, {prefix}created_at, '')" if added_at_fallback else "''"
    return (
        f"COALESCE({prefix}publication_date, "
        f"printf('%04d-01-01', COALESCE({prefix}year, 0)), {tail})"
    )


def canonical_paper_filter(alias: str = "p", *, leading_and: bool = False) -> str:
    """SQL predicate selecting CANONICAL papers — those that are NOT a merged-away
    duplicate of another row.

    A paper whose ``canonical_paper_id`` points at another paper is an alias
    (preprint↔journal dedup; see ``alma.application.preprint_dedup``), not a
    distinct work. The corpus "universe" for coverage / health / dedup counts is
    canonical papers ONLY, so a metric's numerator, denominator, fingerprint, and
    affected-items drilldown must all share THIS predicate — otherwise the headline
    counts aliases the drilldown can't show, and they fail to reconcile (Health
    H-1). ``alias`` is the ``papers`` table alias; ``leading_and=True`` prepends
    ``" AND "`` for appending onto an existing ``WHERE``.
    """
    clause = f"COALESCE({alias}.canonical_paper_id, '') = ''"
    return f" AND {clause}" if leading_and else clause


def standalone_paper_sql(alias: str = "p", *, leading_and: bool = False) -> str:
    """SQL predicate selecting FIRST-CLASS papers — rows that are neither a
    dedup twin nor a part-of component.

    A row is *subordinate* (an inert appendix, not a paper to embed / count /
    graph / let influence taste) when EITHER:
      - ``canonical_paper_id`` points at another row — a preprint↔journal dedup
        twin merged upward (``alma.application.preprint_dedup``); or
      - ``component_type`` is set — a figure / supplementary / dataset /
        peer-review part-of (``alma.core.components``).

    This is the SINGLE read-gate for every surface that must see only real
    papers: embedding-candidate selection, health/coverage counts, graph node
    sets, cluster centroids, the discovery taste-centroid, and the feedback
    signal corpus. It composes ``canonical_paper_filter`` + ``not_component_sql``
    so the two-column check is defined once and never hand-rolled inline.

    Per the Health H-1 rule (see ``canonical_paper_filter``), a metric that uses
    this predicate must use it for its numerator, denominator, fingerprint, AND
    affected-items drilldown together, or the headline counts won't reconcile.

    ``alias`` is the ``papers`` table alias; ``leading_and=True`` prepends
    ``" AND "`` for appending onto an existing ``WHERE``.
    """
    clause = f"{canonical_paper_filter(alias)} AND {not_component_sql(alias)}"
    return f" AND {clause}" if leading_and else clause
