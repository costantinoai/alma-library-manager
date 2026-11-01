"""Tag merge endpoints: find overlapping tags and fold one into another."""

import logging
import re
import sqlite3

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from alma.api.deps import get_current_user, get_db
from alma.api.helpers import raise_internal
from alma.core import addons
from alma.core.db_write import run_write_unit

logger = logging.getLogger(__name__)

router = APIRouter(
    dependencies=[Depends(get_current_user)],
    responses={401: {"description": "Unauthorized"}},
)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------


class TagMergeSuggestionResponse(BaseModel):
    """Suggested merge candidate for duplicate/overlapping tags."""

    source_tag_id: str
    source_tag: str
    target_tag_id: str
    target_tag: str
    confidence: float
    reason: str


class TagMergeRequest(BaseModel):
    """Request body for merging tags."""

    source_tag_id: str
    target_tag_id: str


def _normalize_tag_name(value: str) -> str:
    return " ".join(re.sub(r"[^a-z0-9\s]+", " ", (value or "").lower()).split())


def _token_set(value: str) -> set[str]:
    normalized = _normalize_tag_name(value)
    if not normalized:
        return set()
    return {part for part in normalized.split(" ") if part}


def _merge_pair_score(name_a: str, name_b: str) -> tuple[float, str]:
    norm_a = _normalize_tag_name(name_a)
    norm_b = _normalize_tag_name(name_b)
    if not norm_a or not norm_b:
        return (0.0, "")
    if norm_a == norm_b:
        return (0.99, "normalized_exact")

    singular_a = norm_a[:-1] if norm_a.endswith("s") else norm_a
    singular_b = norm_b[:-1] if norm_b.endswith("s") else norm_b
    if singular_a == singular_b:
        return (0.9, "pluralization")

    tokens_a = _token_set(name_a)
    tokens_b = _token_set(name_b)
    if not tokens_a or not tokens_b:
        return (0.0, "")

    inter = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    jaccard = float(inter / union) if union else 0.0
    if jaccard >= 0.8:
        return (round(jaccard, 3), "token_overlap")
    return (0.0, "")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/merge-suggestions",
    response_model=list[TagMergeSuggestionResponse],
    summary="List suggested tag merges",
    description="Find likely duplicate/overlapping tags based on normalized names and token overlap.",
)
def list_tag_merge_suggestions(
    limit: int = 25,
    min_confidence: float = 0.8,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> list[TagMergeSuggestionResponse]:
    try:
        rows = db.execute(
            """
            SELECT
                t.id,
                t.name,
                COUNT(pt.paper_id) AS usage_count
            FROM tags t
            LEFT JOIN publication_tags pt ON pt.tag_id = t.id
            GROUP BY t.id, t.name
            ORDER BY t.name
            """
        ).fetchall()
        tags = [dict(r) for r in rows]
        if len(tags) < 2:
            return []

        suggestions: list[TagMergeSuggestionResponse] = []
        seen_pairs: set[tuple[str, str]] = set()

        for i in range(len(tags)):
            for j in range(i + 1, len(tags)):
                a = tags[i]
                b = tags[j]
                score, reason = _merge_pair_score(str(a["name"]), str(b["name"]))
                if score < float(min_confidence):
                    continue

                source = a
                target = b
                usage_a = int(a.get("usage_count") or 0)
                usage_b = int(b.get("usage_count") or 0)
                name_a = str(a.get("name") or "").lower()
                name_b = str(b.get("name") or "").lower()
                prefer_a = usage_a > usage_b or (usage_a == usage_b and name_a < name_b)
                if prefer_a:
                    source = b
                    target = a
                pair_key = (str(source["id"]), str(target["id"]))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                suggestions.append(
                    TagMergeSuggestionResponse(
                        source_tag_id=str(source["id"]),
                        source_tag=str(source["name"]),
                        target_tag_id=str(target["id"]),
                        target_tag=str(target["name"]),
                        confidence=round(float(score), 3),
                        reason=reason,
                    )
                )

        suggestions.sort(key=lambda x: x.confidence, reverse=True)
        return suggestions[: max(1, int(limit))]
    except Exception as exc:
        raise_internal("Failed to compute tag merge suggestions", exc)


@router.post(
    "/merge",
    summary="Merge one tag into another",
    description="Reassign every use of the source tag to the target tag and delete the source tag.",
)
def merge_tags(
    body: TagMergeRequest,
    db: sqlite3.Connection = Depends(get_db),
    _user: dict = Depends(get_current_user),
) -> dict:
    source_tag_id = (body.source_tag_id or "").strip()
    target_tag_id = (body.target_tag_id or "").strip()
    if not source_tag_id or not target_tag_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="source_tag_id and target_tag_id are required")
    if source_tag_id == target_tag_id:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="source_tag_id and target_tag_id must differ")

    source = db.execute("SELECT id, name FROM tags WHERE id = ?", (source_tag_id,)).fetchone()
    target = db.execute("SELECT id, name FROM tags WHERE id = ?", (target_tag_id,)).fetchone()
    if source is None or target is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Source or target tag not found")

    def _persist() -> None:
        db.execute(
            """
            INSERT OR IGNORE INTO publication_tags (paper_id, tag_id)
            SELECT paper_id, ?
            FROM publication_tags
            WHERE tag_id = ?
            """,
            (target_tag_id, source_tag_id),
        )
        db.execute("DELETE FROM publication_tags WHERE tag_id = ?", (source_tag_id,))

        addons.tag_merged(db, source, target)
        db.execute("DELETE FROM tags WHERE id = ?", (source_tag_id,))

    run_write_unit(db, _persist, label="tag_merge")

    return {
        "success": True,
        "source_tag_id": source_tag_id,
        "source_tag": source["name"],
        "target_tag_id": target_tag_id,
        "target_tag": target["name"],
    }
