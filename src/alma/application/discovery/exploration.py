"""MMR slate construction with explicit randomized exploration propensities."""

from __future__ import annotations

import math
import random
import re
import secrets
from collections import Counter
from collections.abc import Callable, Sequence
from typing import Any

from .retrieval._common import (
    _candidate_author_keys,
    _candidate_topic_keys,
    _candidate_venue_key,
)

POLICY_VERSION = "mmr-semantic-explore-v2"
EXPLORATION_SLOTS = 4
#: Cards every active branch keeps in a deck. Two, not one: a single card is one
#: coin toss for a whole branch, and its verdict cannot start that branch's own
#: learning (D26). More would hand the ranked deck to branches the ranker doubts:
#: at ten branches two slots already reserve at most 20 of 46 ranked places.
BRANCH_FLOOR_SLOTS = 2


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


def _remembered_similarity() -> Callable[[dict, dict], float]:
    """`_similarity` that computes each pair once: MMR asks for it on every pick."""
    known: dict[tuple[int, int], float] = {}

    def similarity(left: dict, right: dict) -> float:
        key = (id(left), id(right))
        if key not in known:
            known[key] = _similarity(left, right)
        return known[key]

    return similarity


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
    fixed: Sequence[dict] = (),
    similarity: Callable[[dict, dict], float] = _similarity,
    diversity: float = 0.24,
) -> tuple[list[dict], Counter[str], Counter[str], Counter[str]]:
    """Greedy diversified pick. `fixed` cards are already in: they count against
    the caps and repel similar picks, and they are not returned."""
    remaining = [item for item in candidates if not any(item is card for card in fixed)]
    selected: list[dict] = []
    authors: Counter[str] = Counter()
    venues: Counter[str] = Counter()
    topics: Counter[str] = Counter()
    for card in fixed:
        _add_to_counts(card, authors=authors, venues=venues, topics=topics)
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
                - diversity
                * max((similarity(item, prior) for prior in [*fixed, *selected]), default=0.0),
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


def _floor_deficit(
    pool: list[dict],
    deck: list[dict],
    branch_ids: Sequence[str],
    *,
    room: int,
) -> list[dict]:
    """The best cards each branch still needs to hold its floor, first branch first."""
    shown = Counter(str(card.get("branch_id") or "") for card in deck)
    in_deck = {id(card) for card in deck}
    needed: list[dict] = []
    for branch_id in branch_ids:
        waiting = sorted(
            (
                item
                for item in pool
                if str(item.get("branch_id") or "") == branch_id
                and id(item) not in in_deck
            ),
            key=lambda item: -float(item.get("score") or 0.0),
        )
        needed.extend(waiting[: max(0, BRANCH_FLOOR_SLOTS - shown[branch_id])])
    return needed[: max(0, room)]


def _exploit_with_floor(
    pool: list[dict],
    count: int,
    *,
    caps: dict[str, int],
    floor_branch_ids: Sequence[str],
) -> tuple[list[dict], list[dict], Counter[str], Counter[str], Counter[str]]:
    """MMR picks, then each branch's floor cards in the last ranked places.

    Runs after ranking and moves no score. Giving a floor card its slot can push
    another branch's last card out, so the pick repeats until no branch is short;
    each pass adds a floor card, so it ends. A floor card ignores the safety caps
    on purpose: they stop one author or venue dominating a deck, and a branch's
    only way in is not that.
    """
    floor: list[dict] = []
    similarity = _remembered_similarity()
    while True:
        picked, authors, venues, topics = _mmr(
            pool, count - len(floor), caps=caps, fixed=floor, similarity=similarity
        )
        missing = _floor_deficit(
            pool, [*picked, *floor], floor_branch_ids, room=count - len(floor)
        )
        if not missing:
            return (
                picked,
                sorted(floor, key=lambda item: -float(item.get("score") or 0.0)),
                authors,
                venues,
                topics,
            )
        floor.extend(missing)


def build_slate(
    candidates: list[dict],
    *,
    limit: int,
    random_seed: int | None = None,
    floor_branch_ids: Sequence[str] = (),
) -> tuple[list[dict], dict[str, Any]]:
    """Return MMR exploit + uniform-without-replacement exploration slate.

    ``floor_branch_ids``, best branch first, each keep `BRANCH_FLOOR_SLOTS` cards.
    Such a card is chosen deterministically from the pool, so its probabilities
    are 1.0 and it is never `exploration`: the randomized evidence stays the
    uniform tail, and the tail's E/N stays exact because it is declared after
    the floor is settled.
    """

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
    ranked, floor, authors, venues, topics = _exploit_with_floor(
        pool,
        requested_exploit_count,
        caps=caps,
        floor_branch_ids=floor_branch_ids,
    )
    exploit = [*ranked, *floor]
    exploit_ids = {id(item) for item in exploit}
    floor_ids = {id(item) for item in floor}

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
                "branch_floor": id(item) in floor_ids,
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
        "branch_floor_count": len(floor),
        "exploration_count": explore_count,
        "exploration_pool_size": len(eligible_tail),
        "reserved_positions": reserved,
        "safety_caps": caps,
        "safety_filtered_count": (
            len(pool) - len(exploit) - len(eligible_tail)
        ),
    }
