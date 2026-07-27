"""Compact, honest Signal Lab evidence for Home.

Combinatorial ``n choose 3`` is not progress: it explodes with one giant
region, mixes incompatible games, and says nothing about fitted information.
This read model reports recorded/unique observations, fit freshness and
constraints, structural region/edge coverage, and active fitted effects.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from alma.application import materialized_views as mv
from alma.application import super_regions as sr
from alma.application.signal_lab.evidence import edge_key, load_ledger_evidence
from alma.application.signal_lab.fit import MODEL_VIEW_KEY
from alma.application.signal_lab.map_terms import project_utility_to_regions
from alma.application.signal_lab.settings import is_enabled


def _all_edges(regions_payload: dict[str, Any]) -> set[tuple[int, int]]:
    edges: set[tuple[int, int]] = set()
    for raw_region, neighbours in (regions_payload.get("adjacency") or {}).items():
        region_id = int(raw_region)
        for neighbour in neighbours:
            if region_id != int(neighbour):
                edges.add(edge_key(region_id, int(neighbour)))
    return edges


def build_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    """Return Home-ready progress and active direction evidence. Pure read."""
    regions_stored = mv.get_stored(conn, sr.VIEW_KEY)
    model_stored = mv.get_stored(conn, MODEL_VIEW_KEY)
    regions_payload = regions_stored["payload"] if regions_stored else {}
    model_payload = model_stored["payload"] if model_stored else {}
    ledger = load_ledger_evidence(conn)
    counts = model_payload.get("counts") or {}

    source_rounds = int(counts.get("rounds") or 0)
    fitted_queries = max(0, source_rounds - int(counts.get("duplicate_rounds") or 0))
    utility_preferences = int(counts.get("train_prefs") or 0) + int(
        counts.get("holdout_prefs") or 0
    )
    metric_constraints = int(counts.get("train_sims") or 0) + int(counts.get("holdout_sims") or 0)
    all_edges = _all_edges(regions_payload)
    observed_edges = {key for key, evidence in ledger.edges.items() if evidence.answered > 0}
    observed_regions = {
        region_id for region_id, evidence in ledger.regions.items() if evidence.answered > 0
    }

    active = is_enabled(conn)
    utility = project_utility_to_regions(model_payload, regions_payload) if active else {}
    offsets = (
        {
            int(region_id): float(value)
            for region_id, value in (model_payload.get("region_offsets") or {}).items()
        }
        if active
        else {}
    )
    labels = {
        int(region["id"]): str(region.get("label") or f"Region {region['id']}")
        for region in regions_payload.get("regions") or []
    }
    directions = []
    for region_id in set(offsets) | set(utility):
        value = max(
            -1.0,
            min(1.0, offsets.get(region_id, 0.0) + utility.get(region_id, 0.0)),
        )
        if abs(value) < 1e-6:
            continue
        directions.append(
            {
                "region_id": region_id,
                "label": labels.get(region_id, f"Region {region_id}"),
                "value": round(value, 4),
            }
        )

    upward = sorted(
        (item for item in directions if item["value"] > 0),
        key=lambda item: item["value"],
        reverse=True,
    )[:2]
    downward = sorted(
        (item for item in directions if item["value"] < 0),
        key=lambda item: item["value"],
    )[:2]
    return {
        "active": active,
        "rounds": {
            "today": ledger.today,
            "total": ledger.total,
            "answered": ledger.answered,
            "skipped": ledger.skipped,
            "unique_queries": ledger.unique_queries,
            "duplicate_queries": ledger.duplicate_queries,
        },
        "fit": {
            "ready": model_stored is not None,
            "fresh": model_stored is not None and source_rounds == ledger.total,
            "source_rounds": source_rounds,
            "fitted_queries": fitted_queries,
            "fitted_observations": int(counts.get("answered") or 0),
            "pending_rounds": max(0, ledger.total - source_rounds),
            "utility_preferences": utility_preferences,
            "metric_constraints": metric_constraints,
        },
        "coverage": {
            "regions_observed": len(observed_regions),
            "regions_total": len(regions_payload.get("regions") or []),
            "edges_observed": len(observed_edges & all_edges),
            "edges_total": len(all_edges),
        },
        "effects": {
            "upward": upward,
            "downward": downward,
            "regions_moving": len(directions),
            "boundary_overrides": len(model_payload.get("region_overrides") or {}),
            **_author_effects(model_payload),
        },
    }


def _author_effects(model_payload: dict[str, Any]) -> dict[str, Any]:
    """The author head, as the same up/down shape the regions report.

    Keys are the ranker's match keys, so a name that resolved to
    ``"lastname|f"`` is not something to show a person — prefer the readable
    full-name key when the same author produced both.
    """
    offsets = model_payload.get("author_offsets") or {}
    readable = [
        {"key": key, "label": key, "value": round(float(value), 4)}
        for key, value in offsets.items()
        if "|" not in key and abs(float(value)) >= 1e-6
    ]
    return {
        "authors_up": sorted(
            (item for item in readable if item["value"] > 0),
            key=lambda i: i["value"],
            reverse=True,
        )[:2],
        "authors_down": sorted(
            (item for item in readable if item["value"] < 0), key=lambda i: i["value"]
        )[:2],
        "authors_moving": len(readable),
    }
