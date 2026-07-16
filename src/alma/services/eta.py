"""ETA estimates for the external-API maintenance / repair operations.

These operations drain a backlog of eligible items by making *batched* calls to
an external API. The estimate is:

    requests_needed = ceil(items / batch_size)
    seconds         = requests_needed / requests_per_second

where ``requests_per_second`` is the **endpoint's** rate rule and depends on
whether we hold an API key for that source. The point of surfacing it: a
one-call-per-paper sweep over a 7k-paper corpus at 1 req/s takes ~2 hours, and
the user should see that *before* pressing Run.

Local compute (SPECTER2 on the GPU) is deliberately **not** estimated here — it
isn't rate-limited and finishes quickly; only the network-bound operations get
an ETA.

``PROFILES`` below is the **single source of truth** for this math. If an API
changes its limits, edit the table here and nothing else — both the Health
maintenance cards and the Settings repair lanes read their ETA from this module.

Rate rules (as of 2026-05, encoded from the live adapters + each API's docs):
- **OpenAlex** — polite pool ~10 req/s *with* a key (required since Feb 2026);
  without a key requests are throttled and at risk, modelled here at ~1 req/s.
  A key therefore *does* change the rate. Batch = 50 ids/request.
- **Semantic Scholar** — ``/paper/batch`` and ``/paper/search`` are capped at
  **1 req/s even with a key** (the key buys reliability, not a higher rate).
  ``/paper/batch`` takes up to 500 ids; we send 250. Title search is 1 id/request
  — this is the slow, one-call-per-paper sweep.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RateProfile:
    """How fast one operation can drain its backlog over the network."""

    source: str  # human label shown in the basis string ("OpenAlex", …)
    batch_size: int  # default items per request (1 = one-at-a-time)
    rps_authed: float  # requests/sec when the source's key is configured
    rps_anon: float  # requests/sec without a key
    auth_affects_rate: bool  # whether a key changes the *rate* (vs only reliability)
    auth_source: str  # "openalex" | "semantic_scholar"
    # Largest batch the endpoint accepts; when set (> batch_size) the op exposes a
    # per-op batch-size override (the ETA + the runner both honor it). ``None`` =
    # the batch is fixed (per-item ops, or a multi-phase op with no single knob).
    max_batch_size: int | None = None


# Keyed by maintenance ``task.key``; the Settings repair lanes reuse the same keys.
# Local / local-DB ops (``embedding``, ``gc_orphan_authors``, ``dedup_preprint_twins``)
# intentionally have no profile — no ETA is shown.
#
# The author ops are approximations: they make several upstream calls *per author*
# (not true batches), so batch_size is 1 and ``rps`` reflects the binding limit:
# - author_metadata: OpenAlex/ORCID/S2/Crossref per author-source candidate; S2 (1/s)
#   is the bottleneck → ~1/s, key-neutral.
# - refresh_authors: full pipeline per author (identity + profile + works + SPECTER2
#   vectors + centroid) → heavier, ~0.5/s.
# - dedup_orcid: one OpenAlex ``filter=orcid:`` lookup per followed author → OpenAlex rate.
PROFILES: dict[str, RateProfile] = {
    "corpus_metadata": RateProfile("OpenAlex", 50, 10.0, 1.0, True, "openalex"),
    # S2 /paper/batch takes up to 500 ids; default 250. Overridable per-op — at the
    # 1 req/s cap a bigger batch is proportionally faster, so this knob matters.
    "s2_vector": RateProfile("Semantic Scholar", 250, 1.0, 1.0, False, "semantic_scholar", max_batch_size=500),
    "title_resolution": RateProfile("Semantic Scholar", 1, 1.0, 1.0, False, "semantic_scholar"),
    "author_metadata": RateProfile("OpenAlex / ORCID / S2 / Crossref", 1, 1.0, 1.0, False, "semantic_scholar"),
    "refresh_authors": RateProfile("multi-source per author", 1, 0.5, 0.5, False, "semantic_scholar"),
    "dedup_orcid": RateProfile("OpenAlex", 1, 10.0, 1.0, True, "openalex"),
}


def batch_bounds(key: str) -> tuple[int, int] | None:
    """``(default_batch, max_batch)`` if op ``key`` exposes a batch-size override,
    else ``None`` (fixed batch). Lets the registry/UI offer a bounded control."""
    p = PROFILES.get(key)
    if p is None or not p.max_batch_size or p.max_batch_size <= p.batch_size:
        return None
    return p.batch_size, p.max_batch_size


def effective_batch_size(key: str, override: int | None) -> int:
    """The batch size op ``key`` will actually use: the override (clamped to the
    endpoint's [1, max]) when the op is overridable, else the profile default.
    Single source of truth so the ETA and the runner can't disagree."""
    p = PROFILES.get(key)
    if p is None:
        return max(1, int(override or 1))
    if override is None or not p.max_batch_size:
        return p.batch_size
    return max(1, min(int(override), p.max_batch_size))


def detect_auth() -> tuple[bool, bool]:
    """``(openalex_authed, semantic_scholar_authed)`` from the env / secret store."""
    from alma.config import get_openalex_api_key, get_semantic_scholar_api_key

    return bool(get_openalex_api_key()), bool(get_semantic_scholar_api_key())


def _fmt_rate(rps: float) -> str:
    """``1 req/s`` / ``10 req/s`` / ``0.5 req/s``."""
    return f"{rps:g} req/s"


def human_duration(seconds: float) -> str:
    """Short, friendly ETA label: ``~6 sec`` / ``~3 min`` / ``~1.4 hr``."""
    if seconds < 90:
        return f"~{max(1, round(seconds))} sec"
    if seconds < 90 * 60:
        return f"~{round(seconds / 60)} min"
    return f"~{seconds / 3600:.1f} hr"


def estimate_eta(
    key: str,
    items: int,
    *,
    openalex_authed: bool,
    s2_authed: bool,
    batch_size: int | None = None,
) -> dict[str, Any] | None:
    """Estimate how long operation ``key`` needs to drain ``items`` eligible rows.

    ``batch_size`` overrides the profile default for overridable ops (clamped to
    the endpoint max); the request count — and therefore the ETA — recomputes from
    it, so the frontend can show a live ETA as the user changes the batch.

    Returns ``None`` when there's nothing to do, or the operation isn't network-
    bound (no profile) — so callers can simply omit the ETA. The returned dict is
    the wire shape the frontend renders:

        {items, requests, batch_size, seconds, label, source,
         authenticated, auth_affects_rate, basis}
    """
    profile = PROFILES.get(key)
    if profile is None or items <= 0:
        return None

    batch = effective_batch_size(key, batch_size)
    requests = math.ceil(items / batch)
    authed = openalex_authed if profile.auth_source == "openalex" else s2_authed
    rps = (profile.rps_authed if authed else profile.rps_anon) or 0.5
    seconds = requests / rps

    if profile.auth_affects_rate:
        key_note = "key set" if authed else "much slower without an API key"
    else:
        key_note = "a key doesn't raise this limit"
    batch_note = f" of {batch}" if batch > 1 else ""
    plural = "s" if requests != 1 else ""
    basis = (
        f"≈{requests:,} request{plural}{batch_note} at {profile.source}'s "
        f"{_fmt_rate(rps)} limit ({key_note})."
    )

    return {
        "items": int(items),
        "requests": int(requests),
        "batch_size": int(batch),
        "seconds": round(float(seconds), 1),
        "label": human_duration(seconds),
        "source": profile.source,
        "authenticated": bool(authed),
        "auth_affects_rate": bool(profile.auth_affects_rate),
        "basis": basis,
    }
