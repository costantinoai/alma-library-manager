"""Personal-name parsing + fuzzy compatibility for author deduplication.

The dedup scan flags two author rows as a possible duplicate when their names are
*compatible* — same surname, and given names that line up allowing for initials.
This is what catches "E. van Hove" ≈ "Emily van Hove" (and "González" ≈
"Gonzalez"), which neither the exact-normalized-name deduper nor the ORCID sweep
sees. It is deliberately diacritic- and format-insensitive.

Kept in `core` (no DB, no I/O) so it is unit-testable in isolation. The matcher
is intentionally a *recall* tool — false positives are expected and handled by
the user-facing reject system (`author_merge_rejections`), not by trying to be
clever here.
"""

from __future__ import annotations

import re
import unicodedata
from typing import NamedTuple, Optional

# Surname particles (tussenvoegsels / nobiliary particles) that bind to the
# surname rather than standing as a given name. Lower-cased, diacritics stripped.
# Not exhaustive, but covers the common European + transliterated-Arabic cases.
_PARTICLES: frozenset[str] = frozenset(
    {
        "van", "von", "der", "den", "de", "del", "della", "di", "da", "dos", "das",
        "du", "la", "le", "el", "lo", "ter", "ten", "te", "bin", "ibn", "al",
        "abu", "mac", "mc", "st", "saint", "san", "santa", "vander", "vande",
        "op", "ut", "zur", "zum", "av", "af",
    }
)


class ParsedName(NamedTuple):
    given: tuple[str, ...]  # given-name tokens, in order (initials are 1 char)
    surname: str            # normalized surname, particles included (space-joined)


def _strip_diacritics(value: str) -> str:
    """NFKD-decompose and drop combining marks so "González" == "Gonzalez"."""
    decomposed = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def _normalize(value: str) -> str:
    """Lower-case, strip diacritics, drop periods, collapse separators."""
    text = _strip_diacritics(value or "").lower()
    # Periods/dashes/apostrophes inside names are noise for matching
    # ("J.-P." -> "j p", "O'Brien" -> "obrien" via the apostrophe drop below).
    for ch in (".", "’"):
        text = text.replace(ch, "" if ch == "’" else " ")
    text = text.replace("-", " ")
    return " ".join(text.split())


def parse_person_name(name: str) -> Optional[ParsedName]:
    """Parse a display name into (given tokens, surname).

    Handles "Given Surname", "Surname, Given", multi-token surnames with
    particles ("van Hove", "de la Cruz"), and initials ("E.", "J-P"). Returns
    None when there's no usable surname (e.g. a single mononym or empty).
    """
    raw = (name or "").strip()
    if not raw:
        return None

    # "Surname, Given" form — the part before the first comma is the surname.
    if "," in raw:
        surname_part, _, given_part = raw.partition(",")
        surname_tokens = _normalize(surname_part).split()
        given_tokens = _normalize(given_part).split()
        if not surname_tokens:
            return None
        return ParsedName(tuple(given_tokens), " ".join(surname_tokens))

    tokens = _normalize(raw).split()
    if not tokens:
        return None
    if len(tokens) == 1:
        # A mononym — no given/surname split to match on. Treat as surname-only;
        # the matcher requires given tokens on both sides, so this won't match.
        return ParsedName((), tokens[0])

    # Surname = the last token plus any preceding run of particles
    # ("emily van hove" -> surname "van hove", given ["emily"]).
    i = len(tokens) - 1
    surname_parts = [tokens[i]]
    while i - 1 >= 1 and tokens[i - 1] in _PARTICLES:
        i -= 1
        surname_parts.insert(0, tokens[i])
    given = tokens[: i]
    return ParsedName(tuple(given), " ".join(surname_parts))


def _token_compatible(a: str, b: str) -> bool:
    """Two given-name tokens line up: equal, or one is an initial that prefixes
    the other ("e" ~ "emily", "j" ~ "john"). Empty never matches."""
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) == 1:
        return b.startswith(a)
    if len(b) == 1:
        return a.startswith(b)
    return False


# Confidence tiers, strongest first — the merge-review badge shows these.
CONFIDENCE_HIGH = "high"      # full first names equal (format/diacritic variant)
CONFIDENCE_MEDIUM = "medium"  # initial ↔ full first name (E. ↔ Emily)
CONFIDENCE_LOW = "low"        # initials only / weak


def name_match_confidence(name_a: str, name_b: str) -> Optional[str]:
    """Confidence that two display names are the same person, or None if not a
    candidate at all.

    Rule (aggressive / max-recall): surnames must be equal and every aligned
    given token compatible (allowing initials); both names must carry at least
    one given token (a bare surname is too weak to propose). The tier reflects
    the strength of the FIRST given name:
      - high   : both full and equal           (emily ~ emily)
      - medium : one initial, one full          (e ~ emily)
      - low    : both initials                  (e ~ e)
    """
    pa = parse_person_name(name_a)
    pb = parse_person_name(name_b)
    if pa is None or pb is None:
        return None
    if not pa.surname or pa.surname != pb.surname:
        return None
    if not pa.given or not pb.given:
        return None
    # Every overlapping given position must be compatible (extra trailing middle
    # names on the longer one are fine).
    for ta, tb in zip(pa.given, pb.given):
        if not _token_compatible(ta, tb):
            return None
    fa, fb = pa.given[0], pb.given[0]
    a_initial, b_initial = len(fa) == 1, len(fb) == 1
    if not a_initial and not b_initial:
        return CONFIDENCE_HIGH  # both full and (compatible →) equal
    if a_initial and b_initial:
        return CONFIDENCE_LOW
    return CONFIDENCE_MEDIUM


# Generic institution words that carry no discriminating power — two unrelated
# affiliations both contain "University" / "Department", so a shared one of these
# is NOT corroboration. Diacritics already stripped by `_normalize`.
_AFFILIATION_STOPWORDS: frozenset[str] = frozenset(
    {
        "university", "universiteit", "universite", "universidad", "universitat",
        "universita", "college", "institute", "institut", "instituto",
        "department", "dept", "school", "faculty", "lab", "labs", "laboratory",
        "center", "centre", "research", "group", "division", "unit", "section",
        "hospital", "medical", "clinic", "national", "international", "state",
        "academy", "foundation", "society", "council", "and", "the", "for", "of",
        "des", "der", "von", "van", "di", "da",
    }
)


def _significant_tokens(affiliation: Optional[str]) -> set[str]:
    """Discriminating affiliation tokens (≥3 chars, not a generic institution
    word). "KU Leuven" → {leuven}; "Katholieke Universiteit Leuven" → {katholieke,
    leuven}; "Department of Physics, MIT" → {physics, mit}."""
    norm = _normalize(affiliation or "")
    tokens = re.findall(r"[a-z0-9]+", norm)
    return {t for t in tokens if len(t) >= 3 and t not in _AFFILIATION_STOPWORDS}


def affiliations_corroborate(aff_a: Optional[str], aff_b: Optional[str]) -> bool:
    """Do two affiliations share a discriminating token? Used to gate AUTO-merging
    a name·high match: "María González" and "Maria Gonzalez" both at "KU Leuven"
    are safe to auto-merge; two "John Smith" at different (or unknown) institutions
    are not. Empty/unknown on either side → not corroborated (→ manual review)."""
    ta = _significant_tokens(aff_a)
    tb = _significant_tokens(aff_b)
    if not ta or not tb:
        return False
    return bool(ta & tb)
