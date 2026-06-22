"""Typed Library-vs-Corpus scope — DRY primitive #1 for the Insights surface.

Why this exists: the `scope` axis (does an analytics view cover only saved
Library papers, or the whole stored corpus?) was threaded as a bare string and
re-validated / re-applied ad-hoc in a dozen places:

* parse/validate — ``scope if scope in {"library", "corpus"} else "library"``
  repeated in ``api/routes/graphs.py`` (the GET handlers + the label-refresh route);
* the SQL filter — ``" AND p.status = 'library'" if scope == "library" else ""``
  repeated across ``ai/projections.py`` and ``api/routes/graphs.py``;
* the materialized-view / layout key — ``f"graph:{graph_type}:{scope}"``.

Each repetition is a place a typo (``"libary"``) silently degrades to a wrong
scope, or a place the Library filter can be forgotten (mixing corpus rows into a
Library metric — task 04 I-20). This enum is the ONE source of truth: callers
``Scope.parse`` the wire value once, then ask for the filter / key / label.

Consumed by graphs, projections, insights, and reports. See task 04
(``tasks/04_INSIGHTS_PAGE_CORRECTNESS_UX_AND_METHODS.md``) DRY primitive #1, and
its frontend mirror ``frontend/src/lib/scope.ts``.
"""

from __future__ import annotations

from enum import StrEnum


class Scope(StrEnum):
    """Which papers an analytics view covers.

    ``library`` — only papers the user saved (``papers.status = 'library'``);
    ``corpus`` — every stored paper. ``StrEnum`` so a ``Scope`` is still a plain
    ``"library"`` / ``"corpus"`` string wherever a string is expected (wire
    payloads, view keys), with no extra ``.value`` ceremony at call sites.
    """

    library = "library"
    corpus = "corpus"

    @classmethod
    def parse(cls, value: object, *, default: "Scope | None" = None) -> "Scope":
        """Coerce a request/query string to a ``Scope``; unknown/empty → default.

        Replaces the open-coded ``scope if scope in {"library","corpus"} else
        "library"`` guard. The default is ``library`` (the safe, narrow scope) so a
        missing/garbled value never silently widens a metric to the whole corpus.
        """
        fallback = default or cls.library
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value or "").strip().lower())
        except ValueError:
            return fallback

    def paper_filter(self, alias: str = "p", *, leading_and: bool = True) -> str:
        """SQL fragment restricting a ``papers`` query to this scope.

        ``library`` → ``" AND <alias>.status = 'library'"`` (or without the leading
        ``AND`` when ``leading_and=False``, for use as the first predicate after
        ``WHERE``); ``corpus`` → ``""`` (no restriction). This is the canonical
        replacement for the scattered ``" AND p.status = 'library'" if scope ==
        "library" else ""`` expressions — keeping the Library filter impossible to
        forget and impossible to mis-spell.
        """
        if self is Scope.corpus:
            return ""
        clause = f"{alias}.status = 'library'"
        return f" AND {clause}" if leading_and else clause

    def view_key(self, graph_type: str) -> str:
        """Materialized-view / persisted-layout key for ``graph_type`` in this scope.

        e.g. ``Scope.library.view_key("paper_map") == "graph:paper_map:library"``.
        One spelling for the key so a rebuild, a fingerprint, a GET, and a cache
        read can never disagree about which slot they mean (task 04 I-1/I-3).
        """
        return f"graph:{graph_type}:{self.value}"

    def label(self) -> str:
        """Human label for UI scope chips/annotations."""
        return "Library" if self is Scope.library else "Corpus"
