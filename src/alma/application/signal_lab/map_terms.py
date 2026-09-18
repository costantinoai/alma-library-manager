"""Signal Lab → paper-terrain bridge.

Semantic positions stay stable: games express taste, not paper meaning. The
learned region head therefore bends only the live terrain valence at read time.
Disabling Signal Lab returns ``None`` before reading its retained model, so all
game signals are ignored without being deleted.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field

from alma.application.signal_lab.utility import project_utility_to_regions


@dataclass(frozen=True)
class LabMapContext:
    strength: float
    region_offsets: dict[int, float]
    region_utility: dict[int, float]
    cluster_to_region: dict[int, int]
    paper_overrides: dict[str, int]
    #: Fitted per-author win rates, by ranker match key. The author map's
    #: terrain reads these so answering a round moves BOTH maps; before this,
    #: the paper terrain learned from Signal Lab and the author terrain did not,
    #: so the same answers visibly bent one map and left the other flat.
    author_offsets: dict[str, float] = field(default_factory=dict)

    def offset_for(self, paper_id: str, cluster_id: int) -> float:
        region_id = self.paper_overrides.get(
            paper_id,
            self.cluster_to_region.get(cluster_id),
        )
        if region_id is None:
            return 0.0
        raw = self.region_offsets.get(region_id, 0.0) + self.region_utility.get(region_id, 0.0)
        return self.strength * max(-1.0, min(1.0, raw))

    def author_offset_for(self, *match_keys: str) -> float:
        """Learned tint for one author, by any of the keys that name them.

        Strongest-magnitude wins when a person is indexed under several keys
        (full name and `last|initial`), matching how the ranker resolves the
        same collision.
        """
        best = 0.0
        for key in match_keys:
            value = self.author_offsets.get(key)
            if value is not None and abs(value) > abs(best):
                best = float(value)
        return self.strength * max(-1.0, min(1.0, best))


def load_lab_map_context(conn: sqlite3.Connection) -> LabMapContext | None:
    """Load the retained model once for one field read; ``None`` means no tint."""
    from alma.application import materialized_views as mv
    from alma.application import super_regions as sr
    from alma.application.signal_lab.fit import MODEL_VIEW_KEY
    from alma.application.signal_lab.settings import read

    settings = read(conn)
    if not settings.enabled or settings.map_tint_strength <= 0:
        return None

    model_stored = mv.get_stored(conn, MODEL_VIEW_KEY)
    regions_stored = mv.get_stored(conn, sr.VIEW_KEY)
    if model_stored is None or regions_stored is None:
        return None

    model = model_stored["payload"]
    regions = regions_stored["payload"]
    offsets = {
        int(region_id): float(value)
        for region_id, value in (model.get("region_offsets") or {}).items()
    }
    region_utility = project_utility_to_regions(model, regions)
    cluster_to_region = {
        int(cluster_id): int(region_id)
        for cluster_id, region_id in (regions.get("cluster_to_region") or {}).items()
    }
    author_offsets = {
        str(key): float(value)
        for key, value in (model.get("author_offsets") or {}).items()
    }
    if (not offsets and not region_utility and not author_offsets) or not cluster_to_region:
        return None

    return LabMapContext(
        strength=settings.map_tint_strength,
        region_offsets=offsets,
        region_utility=region_utility,
        cluster_to_region=cluster_to_region,
        paper_overrides={
            str(paper_id): int(details["region_id"])
            for paper_id, details in (model.get("region_overrides") or {}).items()
        },
        author_offsets=author_offsets,
    )


def apply_lab_author_tint(
    base_valence: float | None,
    *,
    match_keys: tuple[str, ...],
    context: LabMapContext | None,
) -> float | None:
    """The author analogue of :func:`apply_lab_map_tint`.

    `None` in means "no opinion, leave the paper bare" — and the lab may CREATE
    an opinion where feedback had none, because a learned preference for a
    person is exactly the signal the terrain is for. The canonical `[-1, 1]`
    domain is preserved either way.
    """
    if context is None:
        return base_valence
    offset = context.author_offset_for(*match_keys)
    if offset == 0.0:
        return base_valence
    return max(-1.0, min(1.0, (base_valence or 0.0) + offset))


def apply_lab_map_tint(
    base_valence: float,
    *,
    paper_id: str,
    cluster_id: int,
    context: LabMapContext | None,
) -> float:
    """Add the learned region term and preserve the canonical valence domain."""
    if context is None:
        return base_valence
    adjusted = base_valence + context.offset_for(paper_id, cluster_id)
    return max(-1.0, min(1.0, adjusted))
