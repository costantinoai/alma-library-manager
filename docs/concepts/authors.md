---
title: Authors
description: People you track. Identity resolution across OpenAlex, Semantic Scholar, ORCID, and (optionally) Google Scholar, plus multi-source author suggestions.
---

# Authors

The Authors page is where you manage the people you track. Adding
an author here creates a monitor — from then on, their new
publications flow into your [Feed](feed.md) automatically, and
their identifiers (OpenAlex, Semantic Scholar, ORCID, optionally
Google Scholar) are resolved and reconciled in the background.

Most days, you'll spend time in the Feed; you'll come to the Authors
page to follow / unfollow people, fix identifier mismatches, and
review **author suggestions** (the people you don't follow yet that
ALMa thinks you should).

## Adding an author

You can add by:

* **Name** (partial match against OpenAlex)
* **OpenAlex author ID** (e.g. `A1234567890`)
* **ORCID** (e.g. `0000-0002-1825-0097`)
* **Semantic Scholar author ID**
* **Google Scholar ID** (if `scholarly` is installed)

The **AddAuthor dialog** shows candidate matches with disambiguation
context — affiliation, paper count, recent works — so you can pick
the right person before committing.

## Identity resolver

After you add an author, ALMa runs a hierarchical identity resolution
pass:

1. Direct ID hits (OpenAlex / ORCID / S2) take precedence — exact
   match wins.
2. Name + sample-titles match against OpenAlex with affiliation /
   year-range disambiguation.
3. Reverse mapping from S2 → OpenAlex via DOI overlap on a sample
   of works.
4. (Optional) `scholarly` Google Scholar lookup as a tiebreaker.

Each source contributes evidence; the final resolution is stored on
the `authors` row with a `id_resolution_status` (`confirmed`,
`probable`, `ambiguous`, `failed`) and an `id_resolution_reason`
explaining the decision.

The **AuthorIdentifierResolution** panel inside the Authors page
exposes this state per-author and lets you re-run resolution or
pick a different candidate manually.

## Followed vs. tracked

Two states matter on the Authors page:

| State | Meaning |
|---|---|
| **Followed** | You've added them to your watchlist. ALMa creates an author monitor that pulls their new works into the Feed. |
| **Tracked** (background) | An author appearing on a paper in your Library / corpus. Useful for co-author analytics; no monitor. |

Following is the explicit, intentional act. Tracked rows accumulate
naturally as you save papers; you don't manage them by hand.

## Author suggestions

Below the Followed list, the Authors page surfaces **Author
suggestions** — people you don't follow yet that the system thinks
you should. Suggestions blend several signal sources, each stamped
on the row with a `suggestion_type` chip so you can see why:

| `suggestion_type` | Source |
|---|---|
| `library_core` | Authors of your saved Library papers, ranked by paper count + recency + topic / venue overlap. |
| `library_reference` | Authors of papers cited by your Library papers. |
| `semantic_similar` | Authors whose centroid (mean of their paper SPECTER2 vectors) is closest to your Library centroid. |
| `openalex_related` | OpenAlex's "related authors" / concept-adjacency surfaces seeded from your followed authors. |
| `s2_related` | Semantic Scholar's author-recommendation endpoints. |
| `cited_by_high_signal` | Authors whose papers are cited by your highly-rated (≥4) Library papers. |

Each source has its own dedup key (`openalex_id`) and its own weight
in the final merge (configurable in **Settings → Discovery weights**).
Rejecting a suggestion writes a negative signal regardless of which
source produced it.

## Preprint ↔ journal twin engine

A common problem in scholarly metadata: a single work shows up as
two distinct OpenAlex rows — one for the arXiv / bioRxiv preprint
and one for the published journal version. ALMa includes a
deduplication engine that detects these pairs and collapses them.

Detection signals:

1. **Vendor DOI prefix** — `10.48550/arXiv.*`, `10.1101/*`,
   `10.31234/*`, etc.
2. **Normalised title key** — punctuation / whitespace insensitive.
3. **Year proximity** — preprint and published version typically
   within ±2 years.
4. **SPECTER2 cosine ≥ 0.98** (when both rows have vectors) — the
   tiebreaker for cases where the title changed between versions.

When a pair collapses, the **journal version** wins (canonical).
The preprint row keeps its UUID for FK integrity but gets
`canonical_paper_id` stamped to the journal id, and Library /
Discovery reads filter `canonical_paper_id IS NULL` so you only see
one card per work.

## Activity-backed actions

Most heavy author-side operations run in the background and surface
in the Activity panel:

* **Refresh author** — pull latest works.
* **Refresh authors with scope** — bulk refresh for all followed,
  all tracked, or a custom selection.
* **Deep refresh all** — fully re-pull every author's works (used
  after schema changes).
* **Dedup preprint↔journal twins** — runs the twin engine across
  the corpus.

The Activity envelope reports per-source timing and per-author
results so you can see exactly which part of a slow refresh is
slow.

## API

```
GET    /api/v1/authors
POST   /api/v1/authors                          # add
DELETE /api/v1/authors/{id}                     # unfollow
POST   /api/v1/authors/{id}/refresh
POST   /api/v1/authors/deep-refresh-all
POST   /api/v1/authors/{id}/resolve-identifiers
GET    /api/v1/authors/suggestions              # multi-source

GET    /api/v1/library/followed-authors
DELETE /api/v1/library/followed-authors/{id}
```

See [REST API](../reference/api.md).
