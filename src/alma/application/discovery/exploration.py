"""MMR slate construction with explicit randomized exploration propensities."""

from __future__ import annotations

import math
import random
import re
import secrets
from collections import Counter
from typing import Any

from .retrieval._common import (
    _candidate_author_keys,
    _candidate_topic_keys,
    _candidate_venue_key,
)

POLICY_VERSION = "mmr-semantic-explore-v2"
EXPLORATION_SLOTS = 4


def _tokens(candidate: dict) -> set[str]:
    text = " ".join(
        [
            str(candidate.get("title") or ""),
            str(candidate.get("authors") or ""),
            str(candidate.get("journal") or ""),
            " ".join(
                str(topic.get("term") or topic.get("name") or "")
                if isinstance(topic, dict)
                else str(topic or "")
                for topic in candidate.get("topics") or []
            ),
        ]
    ).lower()
    return {token for token in re.findall(r"[a-z0-9]{3,}", text)}


def _semantic_similarity(left: dict, right: dict) -> float | None:
    left_model = str(left.get("_ranking_embedding_model") or "").strip()
    right_model = str(right.get("_ranking_embedding_model") or "").strip()
    left_vector = left.get("_ranking_embedding")
    right_vector = right.get("_ranking_embedding")
    if not left_model or left_model != right_model:
        return None
    try:
        if len(left_vector) != len(right_vector) or not len(left_vector):
            return None
        dot = sum(float(a) * float(b) for a, b in zip(left_vector, right_vector))
        left_norm = math.sqrt(sum(float(value) ** 2 for value in left_vector))
        right_norm = math.sqrt(sum(float(value) ** 2 for value in right_vector))
    except (TypeError, ValueError):
        return None
    if left_norm <= 0.0 or right_norm <= 0.0:
        return None
    return max(0.0, min(1.0, dot / (left_norm * right_norm)))


def _similarity(left: dict, right: dict) -> float:
    semantic = _semantic_similarity(left, right)
    if semantic is not None:
        return semantic
    a, b = _tokens(left), _tokens(right)
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _safety_caps(target: int) -> dict[str, int]:
    return {
        "author": max(3, int(math.ceil(target * 0.16))),
        "venue": max(4, int(math.ceil(target * 0.22))),
        "topic": max(5, int(math.ceil(target * 0.30))),
    }


def _would_exceed_caps(
    candidate: dict,
    *,
    authors: Counter[str],
    venues: Counter[str],
    topics: Counter[str],
    caps: dict[str, int],
) -> bool:
    if any(
        authors[key] >= caps["author"]
        for key in _candidate_author_keys(candidate)
    ):
        return True
    venue = _candidate_venue_key(candidate)
    if venue and venues[venue] >= caps["venue"]:
        return True
    return any(
        topics[key] >= caps["topic"]
        for key in _candidate_topic_keys(candidate)[:3]
    )


def _add_to_counts(
    candidate: dict,
    *,
    authors: Counter[str],
    venues: Counter[str],
    topics: Counter[str],
) -> None:
    authors.update(_candidate_author_keys(candidate))
    venue = _candidate_venue_key(candidate)
    if venue:
        venues[venue] += 1
    topics.update(_candidate_topic_keys(candidate)[:3])


def _mmr(
    candidates: list[dict],
    count: int,
    *,
    caps: dict[str, int],
    diversity: float = 0.24,
) -> tuple[list[dict], Counter[str], Counter[str], Counter[str]]:
    remaining = list(candidates)
    selected: list[dict] = []
    authors: Counter[str] = Counter()
    venues: Counter[str] = Counter()
    topics: Counter[str] = Counter()
    max_score = max((float(item.get("score") or 0.0) for item in remaining), default=1.0) or 1.0
    while remaining and len(selected) < count:
        eligible = [
            item
            for item in remaining
            if not _would_exceed_caps(
                item,
                authors=authors,
                venues=venues,
                topics=topics,
                caps=caps,
            )
        ]
        if not eligible:
            break
        choice = max(
            eligible,
            key=lambda item: (
                (1.0 - diversity) * float(item.get("score") or 0.0) / max_score
                - diversity * max((_similarity(item, prior) for prior in selected), default=0.0),
                float(item.get("score") or 0.0),
                str(item.get("candidate_key") or ""),
            ),
        )
        remaining.remove(choice)
        selected.append(choice)
        _add_to_counts(
            choice,
            authors=authors,
            venues=venues,
            topics=topics,
        )
    return selected, authors, venues, topics


def build_slate(
    candidates: list[dict],
    *,
    limit: int,
    random_seed: int | None = None,
) -> tuple[list[dict], dict[str, Any]]:
    """Return MMR exploit + uniform-without-replacement exploration slate."""

    pool = list(candidates)
    for item in pool:
        item.pop("_selection", None)
    target = min(max(0, int(limit)), len(pool))
    requested_explore_count = min(
        EXPLORATION_SLOTS,
        max(0, target - 1),
    )
    requested_exploit_count = target - requested_explore_count
    caps = _safety_caps(target)
    exploit, authors, venues, topics = _mmr(
        pool,
        requested_exploit_count,
        caps=caps,
    )
    exploit_ids = {id(item) for item in exploit}

    # Declare a tail whose complete membership fits the remaining hard caps.
    # Uniform sampling any subset from this tail therefore cannot violate the
    # slate contract, and every marginal inclusion probability remains E/N.
    eligible_tail: list[dict] = []
    for item in pool:
        if id(item) in exploit_ids:
            continue
        if _would_exceed_caps(
            item,
            authors=authors,
            venues=venues,
            topics=topics,
            caps=caps,
        ):
            continue
        eligible_tail.append(item)
        _add_to_counts(
            item,
            authors=authors,
            venues=venues,
            topics=topics,
        )
    explore_count = min(requested_explore_count, len(eligible_tail))
    seed = int(random_seed if random_seed is not None else secrets.randbits(63))
    rng = random.Random(seed)
    explored = (
        rng.sample(eligible_tail, explore_count)
        if explore_count
        else []
    )
    rng.shuffle(explored)
    actual_target = len(exploit) + len(explored)

    if explore_count:
        reserved = sorted(
            {
                min(
                    actual_target,
                    max(
                        1,
                        round(
                            (idx + 1)
                            * actual_target
                            / (explore_count + 1)
                        ),
                    ),
                )
                for idx in range(explore_count)
            }
        )
        while len(reserved) < explore_count:
            reserved.append(
                actual_target - (explore_count - len(reserved)) + 1
            )
        reserved = sorted(reserved[:explore_count])
    else:
        reserved = []
    explore_by_position = dict(zip(reserved, explored))
    exploit_iter = iter(exploit)
    slate: list[dict] = []
    for position in range(1, actual_target + 1):
        item = explore_by_position.get(position)
        if item is None:
            item = next(exploit_iter)
            item["_selection"] = {
                "exploration": False,
                "inclusion_probability": 1.0,
                "position_probability": 1.0,
            }
        else:
            item["_selection"] = {
                "exploration": True,
                "inclusion_probability": explore_count
                / max(1, len(eligible_tail)),
                "position_probability": 1.0 / explore_count,
            }
        item["_selection"]["final_position"] = position
        slate.append(item)

    tail_probability = (
        explore_count / len(eligible_tail)
        if eligible_tail
        else 0.0
    )
    eligible_tail_ids = {id(item) for item in eligible_tail}
    explored_ids = {id(item) for item in explored}
    for item in pool:
        if id(item) in exploit_ids or id(item) in explored_ids:
            continue
        item["_selection"] = {
            "exploration": False,
            "inclusion_probability": (
                tail_probability
                if id(item) in eligible_tail_ids
                else 0.0
            ),
            "position_probability": None,
            "final_position": None,
        }
    return slate, {
        "policy_version": POLICY_VERSION,
        "rng_seed": seed,
        "candidate_pool": len(pool),
        "requested_slate_size": target,
        "exploit_count": len(exploit),
        "exploration_count": explore_count,
        "exploration_pool_size": len(eligible_tail),
        "reserved_positions": reserved,
        "safety_caps": caps,
        "safety_filtered_count": (
            len(pool) - len(exploit) - len(eligible_tail)
        ),
    }
