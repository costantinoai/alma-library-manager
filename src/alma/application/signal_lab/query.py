"""Pure query identity shared by acquisition, ledger, fit, and evidence."""

from __future__ import annotations


def canonical_query_key(game_id: str, shown: list[str] | tuple[str, ...]) -> str:
    """Stable identity of one unordered game query.

    Presentation order is randomised to remove position bias. It cannot turn
    the same paper set into a new statistical query.
    """
    return f"{game_id}|{'|'.join(sorted(str(paper_id) for paper_id in shown))}"
