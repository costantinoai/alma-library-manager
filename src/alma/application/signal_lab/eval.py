"""Signal Lab eval — what the fitted heads DO to your decks (task 54 §6, 67 C1).

The ``insights:signal_lab_eval`` view answers two questions:

* **Model number** — held-out pairwise accuracy per nested head (computed by
  the fit; re-served here alongside the outcome number).
* **Outcome number** — a *replay*: every lens's latest immutable ranking
  snapshots are re-scored through the ONE ranker, once with the Lab heads at
  zero and once at the current Settings weights, and the two orderings are
  compared (``core.scoring_math.rank_churn``). Accuracy says the model learned
  something; churn says whether it visibly matters on the decks you see.

The replay is honest by construction:

* it uses the signed inputs recorded at ranking time (``reward_features``,
  schema v4+) and the family weights recorded beside them — never a guessed
  feature or a re-measured one;
* it applies the SAME evidence damper the runtime applied (it is inside the
  stored ``lab_utility_raw``) and the SAME clamped settings the runtime reads
  (``ranker.resolve_lab_points``), so there is no private "what-if" bonus —
  the previous probe added 2.5 points per head with ``confidence=1`` onto
  stored scores, which described an effect the runtime never had (bug B1);
* a lens whose snapshots predate the Lab inputs, or were ranked with no Lab
  model loaded, is reported as **unassessable** with the reason, not scored
  with invented zeros;
* re-scoring a snapshot under its own recorded weights must reproduce its
  stored ``prior_score``. That parity count is a correctness gate on the
  ranker itself; a mismatch means replay and runtime have diverged.

Churn is diagnostic only. It says the heads *reorder*; it never says the
reordering is *better* — that is the calibration lane's question.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from alma.ai.graph_versions import (
    SIGNAL_LAB_EVAL_VERSION,
    SIGNAL_LAB_FIT_VERSION,
    with_version,
)
from alma.application import materialized_views as mv
from alma.application.discovery.calibration import ScoringCalibration
from alma.application.discovery.features import FEATURE_SCHEMA_VERSION
from alma.application.discovery.ranker import (
    LAB_ADJUSTMENTS,
    RANKER_VERSION,
    repaired_prior_score,
    resolve_lab_points,
)
from alma.core.scoring_math import rank_churn

EVAL_VIEW_KEY = "insights:signal_lab_eval"

# The window the ordering comparison is measured on, per lens.
TOP_N = 20

# Replaying a snapshot under its own recorded weights must land on its stored
# score. 6-dp rounding of ~14 published terms cannot drift past this.
_PARITY_TOLERANCE = 1e-3

# Every input the replay reads, so the view rebuilds when — and only when —
# one of them changes: the rounds (→ model), the fitted model row itself, the
# newest snapshot per lens, the Lab weights + enable switch, and the code
# versions of fit, eval, ranker and snapshot schema.
_FINGERPRINT_SQL = with_version(
    """
    SELECT (SELECT COUNT(*) FROM signal_lab_rounds),
           (SELECT COALESCE(MAX(id), 0) FROM signal_lab_rounds),
           (SELECT COALESCE(fingerprint, '') FROM materialized_views
             WHERE view_key = 'signal_lab:model'),
           (SELECT COALESCE(MAX(created_at), '') FROM suggestion_sets),
           (SELECT COUNT(*) FROM suggestion_sets),
           (SELECT COALESCE(GROUP_CONCAT(key || '=' || value, ';'), '')
              FROM (SELECT key, value FROM discovery_settings
                     WHERE key IN ('signal_lab.enabled',
                                   'weights.lab_region_offset',
                                   'weights.lab_utility')
                     ORDER BY key))
    """,
    SIGNAL_LAB_FIT_VERSION,
    SIGNAL_LAB_EVAL_VERSION,
    RANKER_VERSION,
    FEATURE_SCHEMA_VERSION,
)

_ZERO_LAB_POINTS = {spec.key: 0.0 for spec in LAB_ADJUSTMENTS}
_LAB_ATOM_KEYS = tuple(spec.atom_key for spec in LAB_ADJUSTMENTS)


def _latest_snapshot_per_lens(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT l.id AS lens_id, l.name AS lens_name, ss.id AS suggestion_set_id,
               ss.ranker_version AS set_ranker_version
        FROM discovery_lenses l
        JOIN suggestion_sets ss ON ss.id = (
            SELECT id FROM suggestion_sets
            WHERE lens_id = l.id
            ORDER BY COALESCE(created_at, '') DESC, id DESC
            LIMIT 1
        )
        ORDER BY l.name, l.id
        """
    ).fetchall()


def _snapshot_rows(conn: sqlite3.Connection, suggestion_set_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT id, prior_score, reward_features, exposure_features,
               feature_schema_version
        FROM discovery_ranking_candidates
        WHERE suggestion_set_id = ?
        ORDER BY fused_rank
        """,
        (suggestion_set_id,),
    ).fetchall()


def _lab_measured(reward: dict) -> bool:
    return any(
        bool((reward.get(key) or {}).get("availability")) for key in _LAB_ATOM_KEYS
    )


def _replay_lens(
    lens: sqlite3.Row,
    rows: list[sqlite3.Row],
    *,
    lab_points: dict[str, float],
) -> dict[str, Any]:
    """One lens's replay verdict. Never guesses: a snapshot that cannot be
    replayed faithfully makes the lens *unassessable*, with the reason."""

    report: dict[str, Any] = {
        "lens_id": str(lens["lens_id"]),
        "lens_name": lens["lens_name"],
        "suggestion_set_id": str(lens["suggestion_set_id"]),
        "candidates": len(rows),
        "status": "unassessable",
        "reason": None,
        "parity": {"checked": 0, "mismatched": 0},
        "churn": None,
    }
    if not rows:
        report["reason"] = "no immutable ranking snapshots for this lens; refresh it"
        return report
    versions = {str(row["feature_schema_version"] or "") for row in rows}
    if versions != {FEATURE_SCHEMA_VERSION}:
        report["reason"] = (
            "snapshots predate the recorded Lab inputs "
            f"({', '.join(sorted(versions))}); refresh the lens"
        )
        return report

    baseline: dict[str, float] = {}
    adjusted: dict[str, float] = {}
    measured = 0
    checked = mismatched = 0
    for row in rows:
        try:
            reward = json.loads(row["reward_features"] or "{}")
            exposure = json.loads(row["exposure_features"] or "{}")
        except (TypeError, ValueError):
            report["reason"] = "a snapshot's stored features could not be decoded"
            return report
        recorded = exposure.get("ranking_weights") or {}
        families = recorded.get("families")
        calibration = ScoringCalibration.from_dict(recorded.get("calibration"))
        if not isinstance(families, dict) or not families:
            report["reason"] = "snapshots carry no recorded family weights; refresh the lens"
            return report
        if _lab_measured(reward):
            measured += 1
        # Parity: the ranker must reproduce what it stored, under what it stored.
        replayed, _ = repaired_prior_score(
            reward, weights=families, lab_points=recorded.get("lab_points") or _ZERO_LAB_POINTS,
            calibration=calibration,
        )
        checked += 1
        if abs(replayed - float(row["prior_score"] or 0.0)) > _PARITY_TOLERANCE:
            mismatched += 1
        cid = str(row["id"])
        baseline[cid], _ = repaired_prior_score(
            reward, weights=families, lab_points=_ZERO_LAB_POINTS, calibration=calibration
        )
        adjusted[cid], _ = repaired_prior_score(
            reward, weights=families, lab_points=lab_points, calibration=calibration
        )

    report["parity"] = {"checked": checked, "mismatched": mismatched}
    if measured == 0:
        report["reason"] = (
            "Lab inputs were not measured when this deck was ranked "
            "(Lab off or no fitted model at the time); refresh the lens"
        )
        return report
    report["status"] = "ok"
    report["lab_measured"] = measured
    report["churn"] = rank_churn(baseline, adjusted, top_n=TOP_N)
    return report


def build_signal_lab_eval(conn: sqlite3.Connection) -> dict[str, Any]:
    from alma.application.discovery.lens_crud import read_settings
    from alma.application.signal_lab.fit import MODEL_VIEW_KEY
    from alma.application.signal_lab.settings import is_enabled

    model_stored = mv.get_stored(conn, MODEL_VIEW_KEY)
    if model_stored is None:
        return {"ready": False}
    payload = model_stored["payload"]

    settings = read_settings(conn)
    lab_points = resolve_lab_points(settings)
    lenses = [
        _replay_lens(lens, _snapshot_rows(conn, str(lens["suggestion_set_id"])), lab_points=lab_points)
        for lens in _latest_snapshot_per_lens(conn)
    ]
    assessed = [lens for lens in lenses if lens["status"] == "ok"]
    parity_checked = sum(lens["parity"]["checked"] for lens in lenses)
    parity_mismatched = sum(lens["parity"]["mismatched"] for lens in lenses)

    replay: dict[str, Any] = {
        "status": "ok" if assessed else "insufficient_evidence",
        "reason": None,
        "enabled": is_enabled(conn),
        "lab_points": lab_points,
        "ranker_version": RANKER_VERSION,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "top_n": TOP_N,
        "lenses_total": len(lenses),
        "lenses_assessed": len(assessed),
        "candidates": sum(lens["candidates"] for lens in assessed),
        "entered_top": None,
        "mean_rank_displacement": None,
        "parity": {"checked": parity_checked, "mismatched": parity_mismatched},
        "lenses": lenses,
    }
    if assessed:
        replay["entered_top"] = sum(int(lens["churn"]["entered_top"]) for lens in assessed)
        replay["mean_rank_displacement"] = round(
            sum(float(lens["churn"]["mean_rank_displacement"]) for lens in assessed)
            / len(assessed),
            3,
        )
    elif not lenses:
        replay["reason"] = "no lens has a ranked deck yet"
    else:
        replay["reason"] = (
            "no lens has snapshots that recorded Lab inputs; refresh a lens "
            "with the Lab enabled and a fitted model"
        )

    return {
        "ready": True,
        "holdout": payload.get("holdout", {}),
        "counts": payload.get("counts", {}),
        "replay": replay,
    }


mv.register(
    mv.View(
        key=EVAL_VIEW_KEY,
        fingerprint_sql=_FINGERPRINT_SQL,
        build_fn=build_signal_lab_eval,
        operation_key="materialize.signal_lab.eval",
    )
)
