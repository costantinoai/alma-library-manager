"""Author-name strings: the one parser for ``papers.authors``.

``papers.authors`` arrives in several shapes depending on where a paper came
from — OpenAlex display lists ("Jane Q. Doe, John Roe"), BibTeX ("Doe, Jane
and Roe, John"), semicolon lists, and BibTeX-style "Last, First, Last, First".
Enrichment (author matching) and the PDF store (a readable file name) both
need the individual names; they read them through here.
"""

from __future__ import annotations

import re

_NAME_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv", "v", "phd", "md", "msc", "ms", "ma"})


def is_name_suffix(value: str) -> bool:
    """True for a generational/degree suffix token ("Jr.", "III", "PhD")."""
    token = re.sub(r"[^a-z0-9]+", "", (value or "").lower())
    return bool(token) and token in _NAME_SUFFIXES


def parse_author_names(authors: str) -> list[str]:
    """Split an authors string into individual "First Last" names, in order."""
    text = (authors or "").strip()
    if not text:
        return []

    if " and " in text.lower():
        parts = [p.strip() for p in re.split(r"\band\b", text, flags=re.IGNORECASE) if p.strip()]
        return parts

    if ";" in text:
        return [p.strip() for p in text.split(";") if p.strip()]

    comma_parts = [p.strip() for p in text.split(",") if p.strip()]
    # Legacy imports may have comma-joined "First Last, First Last, ...".
    if len(comma_parts) >= 2 and all(" " in p for p in comma_parts):
        return comma_parts

    # Common BibTeX style: "Last, First[, Suffix], Last, First[, Suffix], ..."
    if len(comma_parts) >= 4:
        paired: list[str] = []
        idx = 0
        while idx + 1 < len(comma_parts):
            last = comma_parts[idx]
            first = comma_parts[idx + 1]
            idx += 2
            candidate = f"{first} {last}".strip()
            if idx < len(comma_parts) and is_name_suffix(comma_parts[idx]):
                candidate = f"{candidate} {comma_parts[idx]}".strip()
                idx += 1
            paired.append(candidate)
        if idx == len(comma_parts) and len(paired) >= 2:
            return paired

    return [text]


# Name particles that belong to the family name ("van Gogh", "Op de Beeck").
_PARTICLES = frozenset(
    {"van", "von", "der", "den", "de", "del", "della", "di", "da", "du", "dos", "das",
     "le", "la", "ter", "ten", "op", "zu", "bin", "ibn", "al", "'t"}
)


def surname(name: str) -> str:
    """The family name of one "First Last" (or "Last, First") name.

    Trailing suffixes are skipped ("Martin Luther King Jr." → "King"); a
    "Last, First" form returns the part before the comma. Particles stay with
    the family name: a lowercase particle always ("Ludwig van Beethoven" →
    "van Beethoven"), a capitalised one only when another particle follows it
    ("Hans Op de Beeck" → "Op de Beeck"; "Vincent Van Gogh" → "Gogh" is the
    accepted ambiguity).
    """
    clean = " ".join((name or "").split())
    if not clean:
        return ""
    if "," in clean:
        return clean.split(",", 1)[0].strip()
    tokens = clean.split(" ")
    while len(tokens) > 1 and is_name_suffix(tokens[-1]):
        tokens.pop()
    start = len(tokens) - 1
    while start > 1:  # never swallow the given name
        prev = tokens[start - 1]
        lowered = prev.lower()
        if lowered not in _PARTICLES:
            break
        follows_particle = tokens[start].lower() in _PARTICLES and start < len(tokens) - 1
        if prev == lowered or follows_particle:
            start -= 1
            continue
        break
    return " ".join(tokens[start:])
