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

# The two head constants live with the setting defaults (`alma.discovery.
# defaults`) because the ranker reads them too; re-exported here so the feature's
# own callers keep one import path.
from alma.discovery.defaults import LAB_HEAD_DEFAULT_POINTS, LAB_HEAD_MAX_POINTS, lab_enabled

__all__ = [
    "LAB_HEAD_DEFAULT_POINTS",
    "LAB_HEAD_MAX_POINTS",
    "SignalLabHeadLimits",
    "SignalLabSettings",
    "SignalLabSettingsView",
    "is_enabled",
    "read",
    "read_view",
    "write",
]


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
    venue_offset_points: Annotated[float, Field(ge=0, le=LAB_HEAD_MAX_POINTS)] = Field(
        LAB_HEAD_DEFAULT_POINTS,
        title="Venue scoring nudge",
        description=(
            "How much affinity a fully-preferred journal gains. Fitted only "
            "from same-field, different-venue rounds — the one place ALMa can "
            "learn which venue you would rather read at equal topic — and "
            "ADDED to the venue signal your Library already produces."
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


class SignalLabHeadLimits(BaseModel):
    """The head-weight contract, served with the settings so the UI cannot
    hard-code a ceiling the backend does not enforce (it did: 2.5 against 10)."""

    model_config = ConfigDict(extra="forbid")

    head_points_max: float = LAB_HEAD_MAX_POINTS
    head_points_default: float = LAB_HEAD_DEFAULT_POINTS


class SignalLabSettingsView(SignalLabSettings):
    """``GET /signal-lab/settings``: the settings plus their read-only limits.

    Writes take :class:`SignalLabSettings` only — ``limits`` is server truth,
    not a knob, and the strict ``extra="forbid"`` on the write model is what
    keeps it that way.
    """

    limits: SignalLabHeadLimits = Field(default_factory=SignalLabHeadLimits)


_KEYS = {
    "enabled": "signal_lab.enabled",
    "region_offset_points": "weights.lab_region_offset",
    "utility_points": "weights.lab_utility",
    "author_offset_points": "weights.lab_author_offset",
    "venue_offset_points": "weights.lab_venue_offset",
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
        enabled=lab_enabled(stored),
        region_offset_points=float(stored[_KEYS["region_offset_points"]]),
        utility_points=float(stored[_KEYS["utility_points"]]),
        author_offset_points=float(stored[_KEYS["author_offset_points"]]),
        venue_offset_points=float(stored[_KEYS["venue_offset_points"]]),
        map_tint_strength=float(stored[_KEYS["map_tint_strength"]]),
        ring_decay=float(stored[_KEYS["ring_decay"]]),
        exploration_rate=float(stored[_KEYS["exploration_rate"]]),
        coverage_target=int(float(stored[_KEYS["coverage_target"]])),
        refit_every_rounds=int(float(stored[_KEYS["refit_every_rounds"]])),
        holdout_percent=int(float(stored[_KEYS["holdout_percent"]])),
        override_min_votes=int(float(stored[_KEYS["override_min_votes"]])),
    )


def read_view(db: sqlite3.Connection) -> SignalLabSettingsView:
    """The settings as the UI needs them: values plus the head-weight limits."""

    return SignalLabSettingsView(**read(db).model_dump())


#: The knobs `fit.build_signal_lab_model` actually consumes. Changing one makes
#: the retained model a fit of different inputs, so it must be refitted; changing
#: the map tint or a sampler knob does not, and refitting on those is churn. Kept
#: in step with the `tuning` term of `fit._FINGERPRINT_SQL` — the fingerprint is
#: what makes a MISSED refit recoverable, this list is what makes it immediate.
FIT_INPUT_FIELDS = ("ring_decay", "override_min_votes", "coverage_target")


def write(db: sqlite3.Connection, settings: SignalLabSettings) -> SignalLabSettings:
    from alma.application.discovery.lens_crud import upsert_setting
    from alma.application.signal_lab.fit import enqueue_model_refit

    validated = SignalLabSettings.model_validate(settings)
    previous = read(db)

    def _write() -> None:
        values = validated.model_dump()
        for field_name, storage_key in _KEYS.items():
            value = values[field_name]
            stored = str(value).lower() if isinstance(value, bool) else str(value)
            upsert_setting(db, storage_key, stored)

    run_write_unit(db, _write, label="signal_lab.settings")
    # Saving a fitting knob is a user action with a visible promise ("this is how
    # the lab weighs your answers"), so it takes effect now rather than at the
    # next freshness tick hours later. The enqueue defers past this thread's
    # write lock; see `fit.enqueue_model_refit`.
    if any(
        getattr(previous, field) != getattr(validated, field)
        for field in FIT_INPUT_FIELDS
    ):
        enqueue_model_refit(db, label="signal_lab.settings refit")
    if previous != validated:
        from alma.application import materialized_views as mv
        from alma.application.signal_lab.eval import EVAL_VIEW_KEY

        mv.enqueue_after_write(db, EVAL_VIEW_KEY, label="signal_lab.settings replay")
    return read(db)


def is_enabled(db: sqlite3.Connection) -> bool:
    """The one shared consumption gate."""
    from alma.application.discovery.lens_crud import read_settings

    return lab_enabled(read_settings(db))
