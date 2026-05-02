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

Below the Followed list, the page surfaces **Author suggestions**
— candidates you don't follow yet. Each suggestion carries a
`suggestion_type` chip (`library_core`, `cited_by_high_signal`,
`adjacent`, `semantic_similar`, `openalex_related`, `s2_related`)
showing the bucket it primarily came from, plus a `Suggested by N
sources` indicator when multiple buckets independently agreed.

See [Authors → Author suggestions](../concepts/authors.md#author-suggestions)
for what each bucket means and how scoring works.

### Getting better suggestions

The rail is built fresh on every visit by combining six
independent buckets. The signals that drive ranking are entirely
fed by your actions, so a few small habits make a big difference:

1. **Rate your Library papers.** Each star on a paper amplifies
   its co-authors in the rail:
   - **5★** → co-authors carry **3×** the weight of an unrated
     paper's co-authors.
   - **4★** → 2×.
   - **3★ / unrated** → 1× (neutral).
   - **2★** → 0.5×.
   - **1★** → 0.2× (still in the pool but barely contributes).
   First / last authors on a rated paper get an additional 1.5×
   on top — they're the lead and senior author signals.
2. **Give feedback on papers.** Paper feedback now also influences
   author suggestions. Liking or loving a paper gives a capped bump
   to its authors, nearby co-authors, topics, venues, keywords, and
   tags. Dismissing, disliking, removing, or low-rating a paper gives
   a capped negative signal to the same connected attributes. This is
   ranking-only; it does not delete papers or unfollow anyone.
3. **Dismiss authors you don't want.** Rejecting a suggestion
   does two things:
   - Suppresses that exact author for ~250 days.
   - Adds their profile (topics, venues, co-authors, institution)
     to a learned **dismissal cluster** — future candidates whose
     attributes overlap that cluster lose up to 30 points (out of
     100). So dismissing 2-3 authors from a sub-area you don't
     care about will start filtering similar authors automatically.
   The cluster has a 100-day lookback so old dismissals decay out
   as your interests shift. Co-authorship overlap counts as a very
   light penalty (≈0.8/paper) — you stay free to follow people the
   dismissed author wrote with.
4. **Refresh network buckets.** The `openalex_related` and
   `s2_related` buckets read from a cache that's refreshed
   asynchronously. Use **Authors → ⋯ → Refresh network buckets**
   when you want fresh external candidates beyond what your
   Library co-author / citation graph already shows.
5. **Tune bucket weights** in
   **Settings → Discovery → Author suggestion weights** if you
   want to tilt the rail toward discovery (boost
   `openalex_related` / `s2_related`) or toward your existing
   reading (boost `library_core` / `cited_by_high_signal`).
   Defaults are deliberately tilted toward `library_core`
   because it's the strongest evidence; equal-weight network
   buckets at 0.9 give external sources real airtime without
   overrunning your Library signal.

A candidate working in your **#1 library topic** will rank far
higher than one sharing a fringe topic — the system weights
shared topics by how prevalent they are in your Library, so your
core area dominates naturally.

If a suggestion repeatedly surfaces a category of author you
don't want, dismiss them — the rail will learn the cluster and
stop bothering you.

## Removing

Removing an author **stops the monitor** but does not delete their
papers from your Library. Saved papers stay saved; new works just
stop flowing into your Feed.

Follow and remove actions also feed Discovery ranking. Following an
author gives their author profile a positive signal; removing or
dismissing an author gives their profile a negative signal. Both are
ranking-only effects: they help paper recommendations move up or down
without changing any paper's Library or reading state.

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
