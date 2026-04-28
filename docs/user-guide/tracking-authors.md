---
title: Tracking authors
description: Add an author once, ALMa resolves identifiers and pulls their works on a schedule.
---

# Tracking authors

## Adding an author

**Authors → Add author** opens the AddAuthor dialog. You can search
by:

* **Name** — partial match against OpenAlex.
* **OpenAlex author ID** — `A1234567890`, exact match.
* **ORCID** — `0000-0002-1825-0097`, exact match.
* **Semantic Scholar author ID** — exact match.
* **Google Scholar ID** — only if `scholarly` is installed; useful
  as a last resort.

Pick the candidate that matches your target. ALMa shows
disambiguation context (affiliation, paper count, recent works) so
you can avoid homonyms.

When you confirm, a **backfill job** queues in the background:
recent works are pulled from OpenAlex, deduplicated against
existing rows, and inserted as `tracked` papers (not yet in your
Library). Watch the Activity panel; backfills typically complete
within a minute.

## Followed vs tracked

| State | Meaning |
|---|---|
| **Followed** | You explicitly added them. ALMa creates an author monitor that surfaces their new works in your Feed. |
| **Tracked** | The author appears on a paper in your corpus but you haven't followed them. Useful for co-author analytics and as input to author suggestions. |

The Authors page lists Followed by default. The "Tracked" filter
shows everyone, including the long tail of co-authors.

## Identifier resolution

OpenAlex is the primary author registry, but real authors have
multiple identifiers across sources. ALMa runs a hierarchical
resolver after each Add to fill in:

* OpenAlex author ID (always when known)
* Semantic Scholar author ID (when matchable via DOIs / name)
* ORCID (when on the OpenAlex profile)
* Google Scholar ID (only if `scholarly` is enabled and resolves)

Each author row carries `id_resolution_status`:

| Status | Meaning |
|---|---|
| `confirmed` | A direct ID match resolved this author. |
| `probable` | Heuristic match (name + sample titles + affiliation). |
| `ambiguous` | Multiple candidates; manual confirmation needed. |
| `failed` | Nothing matched; check the spelling or try another ID. |

For ambiguous / failed authors, the Authors page shows a **Needs
attention** badge. Click in to see candidate matches and confirm
manually.

## Refreshing

Three refresh paths:

* **Per-author refresh-cache** — incremental, pulls only new works
  since the last refresh. Used by the nightly scheduler.
* **Per-author deep refresh** — re-pulls the full bibliography.
  Use this after schema changes or if you suspect drift.
* **Deep refresh all** — bulk equivalent of the above. Heavy; runs
  for several minutes on a large follow list.

All three are Activity-backed and report per-author progress.

## Author suggestions

Below the Followed list, the page surfaces **Author suggestions** —
candidates you don't follow yet. Each suggestion carries a
`suggestion_type` chip showing where the signal came from
(library_core, library_reference, semantic_similar, openalex_related,
s2_related, cited_by_high_signal). See
[Authors](../concepts/authors.md#author-suggestions) for the full
list.

Reject a suggestion to write a negative signal — the recommender
will down-weight that author across all sources.

## Removing

Removing an author **stops the monitor** but does not delete their
papers from your Library. Saved papers stay saved; new works just
stop flowing into your Feed.

## API

```bash
# add an author by name
curl -X POST http://localhost:8000/api/v1/authors \
  -H 'Content-Type: application/json' \
  -d '{"name":"Andrew Ng","follow":true}'

# refresh
curl -X POST http://localhost:8000/api/v1/authors/A1234567890/refresh-cache

# resolve identifiers
curl -X POST http://localhost:8000/api/v1/authors/A1234567890/resolve-identifiers
```

See the [REST API](../reference/api.md#authors) for the full set.
