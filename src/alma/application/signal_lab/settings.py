"""Signal Lab feature settings.

Signal Lab is a first-class ALMa feature, not an integration plugin. Its
configuration therefore lives with the feature and is served from the
``/signal-lab`` route family. Disabling it is a reversible consumption gate:
rounds and the fitted model remain, while Home, scoring, and maps ignore them.
"""

from __future__ import annotations

import sqlite3
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field

from alma.core.db_write import run_write_unit

LAB_HEAD_MAX_POINTS = 10.0
"""Ceiling for one Signal Lab head, in points on the 0-100 score.

Was 2.5, which put the whole lab BELOW `citation_quality` (5 points) — your
explicit pairwise taste judgements counting for less than how many strangers
cited a paper. That is backwards for a signal whose entire purpose is to record
what you actually prefer.

10 puts a fully-evidenced head on par with `feedback_adj` and
`preference_affinity`, the other two signals that encode your own opinions.

Raising it is safe because the ceiling is NOT what protects against a thin fit:
the evidence dampers do (`map_terms.utility_confidence` and
`region_confidence`), continuously and in proportion to how much you have
actually answered. A low ceiling only guaranteed the feature could never
matter, even at full evidence."""

LAB_HEAD_DEFAULT_POINTS = 5.0
"""Default weight per head.

Non-zero (was 0.0) so a fitted head takes effect without a manual promotion
step. There is nothing to promote: `load_lab_scoring_context` already
early-returns when no usable model exists, so an unplayed install is unaffected,
and the dampers make an under-evidenced one small on their own."""


class SignalLabSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(
        True,
        title="Enable Signal Lab",
        description="Offer games on Home and consume their retained model.",
    )
    region_offset_points: Annotated[float, Field(ge=0, le=LAB_HEAD_MAX_POINTS)] = Field(
        LAB_HEAD_DEFAULT_POINTS,
        title="Region scoring nudge",
        description=(
            "Maximum additive Discovery/Feed points from region preference, "
            "before the evidence damper scales it down. Reaches full strength "
            "only once a region has actually been judged several times."
        ),
    )
    utility_points: Annotated[float, Field(ge=0, le=LAB_HEAD_MAX_POINTS)] = Field(
        LAB_HEAD_DEFAULT_POINTS,
        title="Utility scoring nudge",
        description=(
            "Maximum additive Discovery/Feed points from the confidence-scaled "
            "learned utility direction."
        ),
    )
    author_offset_points: Annotated[float, Field(ge=0, le=LAB_HEAD_MAX_POINTS)] = Field(
        LAB_HEAD_DEFAULT_POINTS,
        title="Author scoring nudge",
        description=(
            "How much affinity a fully-preferred author gains. Fitted from "
            "within-region comparisons only, and ADDED to the author signal "
            "your Library already produces — never a replacement for it."
        ),
    )
    map_tint_strength: Annotated[float, Field(ge=0, le=1)] = Field(
        0.45,
        title="Map taste tint",
        description="How strongly learned region preference bends terrain.",
    )
    ring_decay: Annotated[float, Field(gt=0, le=1)] = Field(
        0.35,
        title="Ring decay",
        description="Library-outward sampling decay γ.",
    )
    exploration_rate: Annotated[float, Field(ge=0, le=1)] = Field(
        0.20,
        title="Exploration rate",
        description="Ring-uniform sampling fraction ε.",
    )
    coverage_target: Annotated[int, Field(ge=1, le=500)] = 20
    refit_every_rounds: Annotated[int, Field(ge=1, le=100)] = 5
    holdout_percent: Annotated[int, Field(ge=0, le=50)] = 15
    override_min_votes: Annotated[int, Field(ge=1, le=100)] = 3


_KEYS = {
    "enabled": "signal_lab.enabled",
    "region_offset_points": "weights.lab_region_offset",
    "utility_points": "weights.lab_utility",
    "author_offset_points": "weights.lab_author_offset",
    "map_tint_strength": "signal_lab.map_tint_strength",
    "ring_decay": "signal_lab.gamma_start",
    "exploration_rate": "signal_lab.epsilon",
    "coverage_target": "signal_lab.coverage_target",
    "refit_every_rounds": "signal_lab.refit_every_rounds",
    "holdout_percent": "signal_lab.holdout_percent",
    "override_min_votes": "signal_lab.override_min_votes",
}


def read(db: sqlite3.Connection) -> SignalLabSettings:
    from alma.application.discovery.lens_crud import read_settings

    stored = read_settings(db)
    return SignalLabSettings(
        enabled=stored[_KEYS["enabled"]].lower() == "true",
        region_offset_points=float(stored[_KEYS["region_offset_points"]]),
        utility_points=float(stored[_KEYS["utility_points"]]),
        author_offset_points=float(stored[_KEYS["author_offset_points"]]),
        map_tint_strength=float(stored[_KEYS["map_tint_strength"]]),
        ring_decay=float(stored[_KEYS["ring_decay"]]),
        exploration_rate=float(stored[_KEYS["exploration_rate"]]),
        coverage_target=int(float(stored[_KEYS["coverage_target"]])),
        refit_every_rounds=int(float(stored[_KEYS["refit_every_rounds"]])),
        holdout_percent=int(float(stored[_KEYS["holdout_percent"]])),
        override_min_votes=int(float(stored[_KEYS["override_min_votes"]])),
    )


def write(db: sqlite3.Connection, settings: SignalLabSettings) -> SignalLabSettings:
    from alma.application.discovery.lens_crud import upsert_setting

    validated = SignalLabSettings.model_validate(settings)

    def _write() -> None:
        values = validated.model_dump()
        for field_name, storage_key in _KEYS.items():
            value = values[field_name]
            stored = str(value).lower() if isinstance(value, bool) else str(value)
            upsert_setting(db, storage_key, stored)

    run_write_unit(db, _write, label="signal_lab.settings")
    return read(db)


def is_enabled(db: sqlite3.Connection) -> bool:
    """The one shared consumption gate."""
    return read(db).enabled
