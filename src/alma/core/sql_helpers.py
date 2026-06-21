"""Shared SQL fragment builders.

Recurring `COALESCE(...)` patterns that lived in 6+ route / application
files. Centralizing them means a calibration change (e.g. changing the
year-only fallback away from the lossy ``YYYY-01-01`` and toward
``COALESCE(publication_date, fetched_at)``) is one diff away from
shipping everywhere.
"""

from __future__ import annotations


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
