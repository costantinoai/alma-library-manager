"""Shared co-occurrence / coupling primitive for the graph edge layers.

Every "two entities are linked because they share features" computation in the
graphs is the SAME operation — only the entity and feature differ:

  * paper bibliographic coupling  — papers sharing referenced works
  * paper co-authorship           — papers sharing authors
  * author co-authorship          — authors sharing papers
  * author bibliographic coupling — authors sharing referenced works

These were implemented FOUR separate ways — two SQL self-joins and two hand-rolled
inverted indexes — which is both a DRY violation and a performance trap: a feature
shared by ``df`` entities produces ``df²`` join rows, so a hub feature (a
field-defining reference, a 1000-author consortium paper) explodes. The corpus
bibliographic-coupling self-join was 372s for exactly this reason, and the author
co-authorship self-join (a double ``IN (...)`` over ~16k authors) had the same
shape.

This module is the ONE inverted-index implementation behind all four: build
``feature -> {entities}`` once, then emit entity pairs per feature, with a
document-frequency cap that drops hub features. The cap is both a perf bound
(work is ``O(Σ min(df, cap)²)`` instead of ``O(Σ df²)``) and a quality filter —
a hub feature couples everyone and discriminates nothing (the IDF intuition),
whether it's an over-cited classic or a mega-author paper.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping, Optional


def cooccurrence_pairs(
    entity_features: Mapping[str, Iterable[str]],
    *,
    min_shared: int = 1,
    max_feature_df: Optional[int] = None,
) -> dict[tuple[str, str], int]:
    """Count shared features between entity pairs via an inverted index.

    Args:
        entity_features: maps each entity id to its features — a paper's
            references, a paper's authors, an author's papers, an author's
            references. Features may repeat / be any iterable; they are
            de-duplicated per entity.
        min_shared: keep only pairs sharing at least this many features.
        max_feature_df: skip any feature shared by more than this many entities
            BEFORE pairing. A feature shared by ``df`` entities emits ``df²/2``
            pairs, so an uncapped hub feature both explodes the work and adds
            non-discriminative edges. ``None`` disables the cap (use when every
            feature is inherently low-fan-out, e.g. papers-per-author, where the
            "hub" is a legitimately prolific author whose links you want kept).

    Returns:
        ``{(lo, hi): shared_count}`` for every unordered entity pair sharing
        ``>= min_shared`` features — keyed so each pair appears exactly once.
    """
    # feature -> set(entities) : the inverted index (built once, one pass).
    inverted: dict[str, set[str]] = defaultdict(set)
    for entity, features in entity_features.items():
        e = str(entity)
        for feature in features:
            f = str(feature).strip()
            if f:
                inverted[f].add(e)

    pairs: dict[tuple[str, str], int] = defaultdict(int)
    for members in inverted.values():
        n = len(members)
        if n < 2:
            continue  # a feature held by a single entity couples no pair
        if max_feature_df is not None and n > max_feature_df:
            continue  # hub feature: non-discriminative + the O(df²) blow-up
        ordered = sorted(members)
        for i in range(n):
            a = ordered[i]
            for j in range(i + 1, n):
                pairs[(a, ordered[j])] += 1

    if min_shared > 1:
        return {pair: shared for pair, shared in pairs.items() if shared >= min_shared}
    return dict(pairs)
