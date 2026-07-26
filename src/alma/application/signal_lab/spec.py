"""Signal Lab pure types — the whole vocabulary of the minigame layer.

D20 (locked 2026-07-26): a minigame is **data plus one pure function**. A
:class:`MiniGame` carries presentation data, a :class:`DrawSpec` telling the
policy what shape of round to draw, and ``interpret`` — a pure function from
one answered round row to the constraints it implies. Games perform no I/O:
no connection ever appears in a game signature, so the guard test
(``tests/test_signal_lab_layer_contract.py``) can make misuse structurally
impossible instead of documented-against.

``interpret`` runs at FIT time, not at submit time. Submitting an answer is
one INSERT; the meaning of that answer is derived when the model refits.
That is what makes interpretation bugs fixable retroactively — correct the
function, and the whole history re-derives (task 54 §0).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

# What shape of triplet the policy should draw for a game.
#   within   — all members from one region (fine preference calibration)
#   boundary — two from region r, one from adjacent region s, all low-margin
#              (metric / boundary information)
RegionMode = Literal["within", "boundary"]


@dataclass(frozen=True)
class DrawSpec:
    """Parameters the policy needs to draw one round for a game."""

    region_mode: RegionMode
    k: int = 3  # papers per round


@dataclass(frozen=True)
class RoundRow:
    """One answered ``signal_lab_rounds`` row, parsed for ``interpret``.

    ``shown`` preserves presentation order (position-bias analysis);
    ``answer`` is the game-specific dict the frontend posted, ``None``/
    ``skipped`` when the user couldn't tell — a skip is the absence of a
    verdict and must interpret to zero constraints, never to a weak one.
    """

    id: int
    game_id: str
    region_id: int | None
    pair_region_id: int | None
    region_version: int | None
    ring: int | None
    policy_version: int
    shown: list[str]
    answer: dict[str, Any] | None
    skipped: bool
    holdout: bool


@dataclass(frozen=True)
class Pref:
    """Utility constraint: the user prefers paper ``a`` over paper ``b``."""

    a: str
    b: str


@dataclass(frozen=True)
class Sim:
    """Metric constraint: ``anchor`` is closer to ``near`` than to ``far``."""

    anchor: str
    near: str
    far: str


@dataclass(frozen=True)
class RegionVote:
    """Boundary vote: ``paper_id`` reads as belonging to ``region_id``."""

    paper_id: str
    region_id: int


Constraint = Pref | Sim | RegionVote


@dataclass(frozen=True)
class MiniGame:
    """One minigame: presentation data + draw shape + one pure interpreter.

    ``options`` is the answer vocabulary the frontend renders (and the only
    values ``interpret`` needs to understand). Everything else — pool
    filtering, region choice, triplet scoring, persistence, fitting — is
    owned by the layer, never the game.
    """

    id: str
    title: str
    question: str
    options: tuple[str, ...]
    draw: DrawSpec
    interpret: Callable[[RoundRow], list[Constraint]] = field(repr=False)
