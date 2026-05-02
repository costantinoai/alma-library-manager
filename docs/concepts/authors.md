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

![Authors page with followed authors and the suggestions rail](../screenshots/desktop-authors.png)

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
suggestions** — people you don't follow yet that ALMa thinks you
should. The rail is built fresh on every visit by running six
"buckets" in parallel and merging the results into a single
ranked list. Each suggestion carries a `suggestion_type` chip
showing the bucket it primarily came from, plus a `Suggested by N
sources` indicator if multiple buckets independently agreed on
the same person.

### The six buckets

| `suggestion_type` | What it surfaces |
|---|---|
| `library_core` | Authors who appear on papers in your saved Library. The closest-to-home signal. |
| `cited_by_high_signal` | Authors whose work is cited by your highly-rated (≥ 4★) Library papers. Strong endorsement-via-citation signal. |
| `adjacent` | Authors whose papers your Library cites, or who share many topics + venues with your Library. Citation-graph and content-graph neighbours. |
| `semantic_similar` | Authors whose paper-embedding centroid is closest to your Library's centroid (SPECTER2 cosine). Catches "vibe" matches that don't share topics or co-authors. |
| `openalex_related` | OpenAlex's own related-authors graph, seeded from people you follow. Pure discovery — these are usually authors you've never co-authored with or cited. |
| `s2_related` | Semantic Scholar's related-authors graph, same pattern. Independent second opinion to OpenAlex. |

The first four buckets read your local data; the last two read
from a cache (`author_suggestion_cache`) that's refreshed
asynchronously by **Refresh network** (Authors → ⋯ → Refresh
network buckets) so the rail never makes blocking external API
calls.

### How a candidate's score is built

Every candidate gets a 0–100 score that drives the rail order.
Four things go into it.

**1. Per-bucket evidence.** Each bucket computes its own raw
score:

- `library_core` weights every co-authorship by **`rating ×
  position ÷ √N`** where `N` is the paper's author count. So a
  first-author of a 5★ 3-person paper carries far more weight
  than a middle-author of a 30-person consortium paper. This
  means **rating your Library papers feeds the model**: a 5★
  rating amplifies the candidate by 3× compared with an unrated
  paper, a 1★ rating shrinks them to 0.2×.
- `cited_by_high_signal` uses the same shape: each citing-paper's
  rating × the candidate's position on the cited paper, divided
  by √N of the cited paper's author count.
- `adjacent` rewards citation-graph proximity + topic / venue
  overlap.
- `semantic_similar` is a direct cosine: 0.9 cosine ≈ 90 score.
- The two network buckets read pre-computed composite scores
  from the cache.

**2. Topic / venue prevalence weighting.** When a candidate
shares topics or venues with your Library, the contribution
isn't a flat count — it's weighted by **how dominant that topic
is in your Library**. A candidate sharing your #1 library topic
contributes ~5× more than one sharing your #30 topic. The weight
is `log(1 + count) / log(1 + max_count)`, so the head dominates
smoothly without zeroing the long tail.

**3. Multi-source consensus bonus.** If a candidate is
independently surfaced by N > 1 buckets, the score gets a
diminishing-returns bonus:

| Buckets agreeing | Bonus |
|---|---|
| 1 (single bucket) | 0 |
| 2 buckets | +12 |
| 3 buckets | +17 |
| 4 buckets | +21 |
| 5 buckets | +24 |

This expresses "many independent systems think this person is
relevant" — strong confidence signal that a single bucket can't
fake. The buckets that confirmed appear in the suggestion's
`consensus_buckets` field.

**4. Dismissal cluster penalty.** When you reject a suggestion
(or remove a followed author), ALMa records a negative signal on
that person — but it doesn't stop there. It also remembers the
**cluster of attributes** that author belonged to and penalizes
future candidates that overlap the cluster. The penalty has four
dimensions:

| Dimension | Penalty per hit | Why this weight |
|---|---|---|
| Topic | 2.0 / shared topic-cluster hit | Strong signal of "this kind of research" |
| Venue | 1.5 / shared venue | Moderate: venues are broader buckets |
| Institution | 1.0 / shared institution | Light: many candidates share institutions for non-cluster reasons |
| Coauthor | 0.8 / paper co-authored with dismissed pool | Lightest: dismissing an author often means "not this person", not "no one they've written with" — co-authorship is noisy negative evidence, so only deep collaboration (10+ shared papers) registers meaningfully |

Total penalty per candidate is capped at **30 points (30% of the
band)**. Even a perfect cluster match never fully zeros a
candidate — if you really don't want them, dismiss them
explicitly. The lookback is 100 days, so old dismissals decay out
and your taste can shift.

When this fires, the suggestion carries a `dismissal_penalty`
field showing how many points were subtracted.

### How your actions shape the rail

| Action | Effect on suggestions |
|---|---|
| **Save a paper to Library** | Adds the paper's co-authors to the `library_core` bucket. |
| **Rate a Library paper 5★** | Triples the weight of every co-author on that paper; doubly amplifies first / last authors. |
| **Rate a Library paper 1-2★** | Shrinks the weight of those co-authors (0.2× / 0.5×). The paper still feeds the model — it just barely contributes. |
| **Follow an author** | Removes them from suggestions; their followed status seeds the network buckets on next refresh. |
| **Dismiss / remove a suggested author** | (a) Suppresses that author for 250+ days. (b) Adds their topic / venue / coauthor / institution profile to the dismissal cluster — future similar candidates lose up to 30 points. |
| **Refresh network buckets** | Fetches fresh `openalex_related` and `s2_related` candidates from OpenAlex / S2 and writes them to the cache. The next rail visit reads them. |

Each bucket has its own weight in the final merge (configurable
in **Settings → Discovery → Author suggestion weights**). By
default, `library_core` = 1.0, `cited_by_high_signal` = 0.9,
`semantic_similar` = 0.8, `adjacent` = 0.7, `openalex_related` /
`s2_related` = 0.9 each.

Tilting weights toward the network buckets surfaces more authors
you don't already know; tilting toward `library_core` /
`adjacent` keeps the rail tied to your existing reading.

For the precise formulas, constants, and tuning surface, see
[Scoring formulas → Author suggestions](../reference/scoring.md#author-suggestions).

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
