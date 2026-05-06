---
title: Scoring formulas
description: The 10-weight Discovery scorer, in numbers.
---

# Scoring formulas

A Discovery candidate is a paper proposed by one of the
[retrieval channels](../concepts/discovery.md#retrieval-channels). The
scorer combines several signals into a single number used for
ranking. This page documents how.

## The hybrid scorer

For each candidate $c$ in lens $L$:

$$
\text{score}(c, L) = \sum_{i} w_i \cdot s_i(c, L)
$$

where $w_i$ is the weight from
[Discovery settings](../reference/configuration.md#where-each-settings-card-stores-its-values)
and $s_i$ is one of the signal functions below.

## Signals

### `source_relevance`

How strongly the channel that produced this candidate "votes" for
it. OpenAlex `related-works` votes hard (≈1.0); broad topic search
votes lower (≈0.3). SPECTER2 cosine votes proportionally to the
cosine value.

Range: 0…1.

### `text_similarity`

Two paths, blended:

1. **Semantic** — SPECTER2 cosine between the candidate's vector
   and the lens's preference centroid (mean of saved-paper
   vectors). Available when embeddings are enabled.
2. **Lexical** — TF-IDF + character n-gram + scholarly term overlap
   over a richer scholarly document (title + abstract + topic terms
   + venue), not just plain title/abstract.

Both are calibrated through piecewise curves so method-level
matches aren't compressed into near-zero values. The two paths
average; if only one is available, that one is used.

Range: 0…1.

### `author_affinity`

Has the candidate's author appeared in your Library, your follow
list, or as a co-author of your saved authors?

Computed as a weighted sum:

| Match | Weight |
|---|---|
| Followed author | 1.0 |
| Author of a saved Library paper | 0.7 |
| Co-author of a saved-Library author | 0.4 |
| Author cited by saved papers | 0.3 |

Range: 0…1 (clamped).

### `journal_affinity`

Does the candidate's venue (journal / conference) appear often in
your Library? The user's preference profile stores per-venue
prevalence weights via $\log(1 + n) / \log(1 + N)$ where $n$ is
the count of saved papers in this venue and $N$ is the count in
your most-saved venue. The candidate's venue is matched against
this dict; the resulting weight is the signal value.

This is a log-prevalence scheme — sharing the user's #1 venue gets
weight 1.0, and a venue that only appears in 5/50 of the user's
papers gets ~0.42 (versus ~0.10 under naive linear normalization).
Long-tail venues stay visible in scoring instead of being drowned
by the dominant outlet. Same shape as `topic_score` and the
author-rail prevalence pattern.

Range: 0…1.

### `recency_boost`

A small boost for newer papers, decaying linearly:

$$
\text{recency}(c) =
\max(0, 1 - \frac{\text{years\_since}(c)}{R})
$$

where $R$ is `discovery.limits.recency_window_years` (default 5).

Range: 0…1.

### `citation_quality`

Log-scaled citation count:

$$
\text{citation\_quality}(c) = \frac{\log(1 + \text{cited\_by})}{\log(1 + 10000)}
$$

A paper with 10k citations gets ≈1.0; a paper with 0 gets 0. The
denominator is fixed so the function is interpretable across
candidates.

Range: 0…1.

### `feedback_adj`

Adjusts the score based on prior feedback on the candidate's
attributes (paper, topics, authors, venue, keywords, and tags). ALMa
reads three canonical preference sources through
`alma.application.signal_projection` and folds each into the same
per-paper signal map before fanning out:

| Source | Weight | What it captures |
|---|---|---|
| `feedback_events` (`paper_action` + legacy single-action types) | 1.0 | Canonical write path (save / like / love / dismiss / remove). |
| `papers.rating` | 0.6 | Library star ratings. No time decay (a 5★ paper is still a 5★ paper). |
| `recommendations.user_action` | 0.5 | Legacy per-recommendation actions, age-decayed like `feedback_events`. |

Each signed paper signal then projects to the connected graph:

| Target | Propagation rule |
|---|---|
| Paper | Direct signed signal |
| Authors / co-authors | Position-weighted, damped by `1 / sqrt(author_count)` |
| Topics | Topic score times the signed paper signal |
| Venue | Weak, capped venue prior |
| Keywords / tags | Tags stronger than extracted keywords |
| Semantic neighbours | Close active-model embedding neighbours only |
| Citation neighbours | Local incoming and outgoing citation edges |
| Author follow / reject | Direct author signal plus weak profile spillover to topics, venues, keywords, tags, **direct coauthors**, and **same-institution colleagues** |

The last row spreads followed-author signal slightly wider than the
direct author: the followed author's frequent collaborators inherit a
weak positive prior, and other authors at the same institution
inherit a weaker one (capped to ≤400-author affiliations to skip
mega-universities). Symmetric for `missing_author_feedback` rejects.

Each prior feedback event contributes:

* **Positive** (rating ≥ 4) → small boost.
* **Negative** (rating ≤ 2) → small penalty.

Decayed over time using two windows:

| Window | Default | Weight |
|---|---|---|
| `feedback_decay_days_full` | 30 | 1.0 |
| `feedback_decay_days_half` | 90 | 0.5 |

Beyond `_half` days, weight tapers to 0.

Range before normalization: -1…+1. The weighted scorer stores it as a
0…1 value in the final score, and the explanation payload includes
`projected_feedback_raw` so the signed contribution remains visible.

### `preference_affinity`

Distance from the candidate's vector to the lens's
`preference_profiles` centroid (a learned projection of your
positive-rated saves). Lower distance → higher score.

Computed as $1 - \text{cosine\_distance}$, with optional
non-linear calibration to spread out the top.

Range: 0…1. Available only when embeddings are enabled.

### Two more weights

The `discovery_settings.weights` object has a couple more knobs that
the UI exposes:

* **`source_relevance` boost per channel** — per-channel multipliers
  used by the retrieval phase before the global scorer.
* **`usefulness_boost`** — a small explicit per-source bonus that
  lets you say "I trust S2 recs more than topic search" without
  changing the channel weights.

## Multi-source consensus bonus

After the 10-signal weighted score is computed, candidates that were
independently surfaced by more than one retrieval source get a
band-relative bonus on top. This mirrors the author-suggestion
consensus pattern and rewards multi-source agreement as a confidence
signal — a paper found by SPECTER2 vector search *and* OpenAlex
related-works *and* S2 recommendations is much stronger evidence than
any one of those alone.

Buckets are assembled in `_merge_channel_candidates`:

* Each non-external retrieval channel (`lexical`, `vector`, `graph`)
  contributes one bucket per channel name, e.g. `channel:lexical`.
* The `external` channel contributes one bucket per **distinct
  `source_api`**: a paper surfaced by both the OpenAlex lane *and*
  the Semantic Scholar lane inside `external` counts as 2
  confirmations, not 1. Buckets look like `external:openalex`,
  `external:semantic_scholar`.

The bonus formula matches the author rail:

$$
\text{bonus}(c) = 0.12 \times 100 \times \sqrt{N - 1}
$$

where $N$ is `consensus_count = len(consensus_buckets)` (only applied
when $N > 1$). With the current calibration:

| `consensus_count` | Bonus |
|---:|---:|
| 1 | 0 |
| 2 | +12 |
| 3 | ≈ +17 |
| 4 | ≈ +21 |
| 5 | +24 |

The bonus is added to the weighted score and clamped at 100, so a
saturated single-channel signal can't be doubled, but a moderately
scored candidate confirmed by 3+ independent sources reliably climbs
the rail. Pre-bonus value is preserved as
`weighted_score_pre_consensus` in the breakdown for provenance.

## Dismissal cluster penalty

After consensus, paper Discovery applies a dedicated penalty that
mirrors the author-suggestion rail's `_dismissal_overlap_penalty`.
`feedback_adj` already pulls in projected dismissal evidence, but it's
bounded to ±0.6 and weighted at 0.10 — so direct user dismissals can
move a candidate by at most ~10 points on the 100-band score. That
ceiling is intentionally low for *projected* feedback (one dismissed
paper shouldn't dominate similarity); but for the *cluster* of things
the user has dismissed, the rail wants to pull harder. The dismissal
cluster pass is that harder pull.

For each candidate the scorer reads the negative side of
`ProjectedPaperSignals.{topic, venue, author, author_name, keyword,
tag}` and accumulates score-point penalties:

| Axis | Per-hit penalty | Rationale |
|---|---:|---|
| Topic | 4.0 × magnitude × topic_strength | Strongest cluster evidence |
| Venue | 3.0 × magnitude | Next-strongest — venue matches a research community |
| Author (OpenAlex id) | 2.0 × magnitude | Identity-level signal |
| Author name | 1.5 × magnitude | Fallback when no id |
| Keyword / tag | 1.0 × magnitude (each) | Noisier; lower per-hit weight |

Total is capped at **30 points** so a candidate is never zeroed by
penalty alone — the user can still dismiss them explicitly. Applied
*after* the consensus bonus (so multi-source agreement can't fully
rescue a candidate matching a dismissed cluster) and the result is
clamped at 0.

The breakdown carries `score_pre_dismissal`, `dismissal_penalty`, and
`dismissal_penalty_parts` (the per-axis decomposition) for provenance.
A clean run with no dismissal evidence returns
`dismissal_penalty=0.0` and `dismissal_penalty_parts={}`.

## Outcome calibration

After consensus, every candidate's `source_relevance` is multiplied
by an outcome-derived calibration multiplier. The multiplier is the
composition of three independent axes:

| Axis | Grouping key | Source |
|---|---|---|
| `source_api` | The API that surfaced the candidate (`openalex` / `semantic_scholar` / …) | `recommendations.source_api` × `feedback_events` |
| `branch_mode` | The retrieval lane (`core` / `explore` / `safe`) | `recommendations.branch_mode` |
| `branch_id` | The specific branch within the lens | `recommendations.branch_id` |

Each axis runs the same Beta-Bernoulli posterior over a 180-day
window with a 60-day half-life decay:

$$
\text{quality}(k) = \frac{\text{positives}(k) + \alpha}{\text{positives}(k) + \text{negatives}(k) + \alpha + \beta}
$$

with $\alpha = \beta = 2$. A fresh DB returns 0.5 → multiplier 1.0
(no behavior change). A source where saves dominate climbs toward
1.5×; one where dismisses dominate falls toward 0.5×. The three
axes compose multiplicatively in log space, then the composite is
clamped back to `[0.5, 1.5]` so three independent positive axes
can't push past the per-axis ceiling.

Per-candidate breakdown carries the composite as
`source_calibration_multiplier` and the per-axis components as
`source_calibration_components.{source_api, branch_mode, branch_id}`.
The full snapshot — quality, multipliers, raw counts, impressions —
also lives on `retrieval_summary.calibration.{source_api, branch_mode,
branch_id}`.

### Author rail bucket calibration

The Suggested Authors rail uses the same machinery on a different
grouping. Each rail card carries a `suggestion_type` (the bucket:
`library_core` / `cited_by_high_signal` / `adjacent` /
`semantic_similar` / `openalex_related` / `s2_related`). Two log
tables capture per-bucket outcomes:

- `author_suggestion_follow_log` — one row per rail-originated
  follow, with the bucket label.
- `missing_author_feedback` — one row per reject (`signal_value < 0`),
  with the bucket label since Phase 4.

`compute_author_bucket_calibration(db)` aggregates both into the
same posterior shape, producing `{bucket: multiplier}`. Inside
`list_author_suggestions` the multiplier is folded into the existing
per-bucket weight pass:

$$
\text{score}(c) = \min\bigl(100, \text{raw}(c) \cdot w_{\text{bucket}} \cdot m_{\text{bucket}}\bigr)
$$

The card response carries `bucket_calibration_multiplier` for
provenance. As with paper Discovery, a fresh DB returns no
multipliers → 1.0 → no behavior change until follow / reject events
accumulate.

## Defaults

The default weights err on the side of "balanced". Every signal is
weighted, the weights are read from `discovery_settings.weights.*`,
and the ranker normalizes them to sum to 1.0 before computing the
final 0–100 score. Source of truth:
`src/alma/discovery/scoring.py` (`weights = {...}` near the top of
`score_candidate`):

| Signal | Raw weight | Normalized share | What it captures |
|---|---|---|---|
| `topic_score` | 0.20 | ~17% | Overlap with your top library topics (log-prevalence weighted). |
| `text_similarity` | 0.20 | ~17% | Semantic SPECTER2 cosine **blended with** lexical TF-IDF + char n-grams + scholarly term overlap. The semantic / lexical sub-weights are dynamic per candidate and reported as `text_similarity_semantic_weight` + `text_similarity_lexical_weight` in the breakdown. |
| `source_relevance` | 0.15 | ~13% | How strong the retrieval signal was that surfaced the candidate (high for related-works, lower for broad topic search). |
| `author_affinity` | 0.15 | ~13% | Has the candidate's author appeared in your library or follow list? Log-prevalence weighted so a single dominant author can't crowd the long tail. |
| `recency_boost` | 0.10 | ~9% | Newer papers get a bump (linear decay over `recency_window_years`, default 10). Reads `year`, falls back to `publication_date[:4]`. |
| `feedback_adj` | 0.10 | ~9% | Liked / disliked papers and their projected graph neighbours. |
| `preference_affinity` | 0.10 | ~9% | Distance from your `preference_profiles` centroid. |
| `usefulness_boost` | 0.06 | ~5% | Explicit per-source bonus, partly tied to recency and citation_quality. |
| `journal_affinity` | 0.05 | ~4% | Does the candidate's venue appear often in your library (log-prevalence). |
| `citation_quality` | 0.05 | ~4% | `log(effective_citations + 1) / log(1000)` where `effective_citations = max(cited_by_count, 2 * influential_citation_count)`. |

So a perfect SPECTER2 cosine of 1.00 contributes at most ~17% of the
final score (≈17 points on the 0–100 scale), tied with topic_score.
SPECTER2 dominates `text_similarity` only insofar as the dynamic
blend favours semantic over lexical for that candidate — and even
then it's bounded by the 17% bucket. Three knobs change this
balance:

* **Per-signal weights**: edit `weights.text_similarity` in
  `discovery_settings` (Settings → Discovery weights). Lowering it to
  e.g. 0.10 caps SPECTER2's contribution at ~9% of the final score.
* **Recommendation mode**: `recommendation_mode` reads `balanced` /
  `explore` / `exploit`. Explore multiplies `recency_boost` by 1.5×
  and halves `author_affinity`, `journal_affinity`, `citation_quality`
  before normalization; exploit does the opposite. The shift
  re-normalizes against the same 1.0 budget, so all signals still
  participate proportionally.
* **Per-lens overrides**: each lens carries its own
  `weights.*` overrides. They're merged on top of the global
  defaults at refresh time.

The `recommendations` table caches the last batch so you don't lose
results when re-tuning — only the next refresh applies the new
weights.

## Score breakdown

`GET /api/v1/discovery/recommendations/{id}/explain` returns the
per-signal contribution for one recommendation:

```json
{
  "id": "rec-abc",
  "paper_id": "p-xyz",
  "score": 0.71,
  "score_breakdown": {
    "source_relevance": 0.95,
    "text_similarity": 0.62,
    "author_affinity": 0.40,
    "journal_affinity": 0.10,
    "recency_boost": 0.80,
    "citation_quality": 0.55,
    "feedback_adj": 0.0,
    "preference_affinity": 0.71
  },
  "weights_used": { "...": 0 },
  "channel": "openalex_related"
}
```

The UI's "why this paper?" hover surfaces this breakdown so you can
see which signals pushed each recommendation up.

---

# Author suggestions

The Authors page rail (`GET /api/v1/authors/suggestions`,
implemented in `alma.application.authors.list_author_suggestions`)
runs a separate scoring pipeline from Discovery. Same band,
different formulas.

The pipeline has five phases:

1. **Six bucket scans** populate a candidate list, each emitting
   a per-bucket raw score in 0…`_MAX_SUGGESTION_SCORE` (= 100).
2. **Multi-source consensus pass** boosts candidates that
   appeared in more than one bucket.
3. **Paper-feedback projection pass** bumps or penalizes candidates
   whose author, topics, venues, keywords, or tags are connected to
   liked/dismissed papers.
4. **Dismissal cluster pass** subtracts a penalty from candidates
   whose attributes overlap recently dismissed authors'.
5. **Per-bucket weight + sort** applies the
   `discovery_settings.author_suggestion_weights.*` multipliers
   and orders the rail.

All scoring constants are at the top of `application/authors.py`:

```python
_MAX_SUGGESTION_SCORE = 100.0                           # band ceiling
_CONSENSUS_BONUS_FRACTION = 0.12                        # 5-bucket → ~24% of band
_DISMISSAL_TOPIC_PENALTY_PER_HIT = 0.020 * _MAX        # = 2.0
_DISMISSAL_VENUE_PENALTY_PER_HIT = 0.015 * _MAX        # = 1.5
_DISMISSAL_COAUTHOR_PENALTY_PER_HIT = 0.008 * _MAX     # = 0.8 (intentionally light: see rationale)
_DISMISSAL_INSTITUTION_PENALTY_PER_HIT = 0.010 * _MAX  # = 1.0
_DISMISSAL_PENALTY_CAP = 0.30 * _MAX                   # = 30.0
```

Penalties / bonuses are expressed as fractions of the band so
they stay calibrated if the band ever rescales — change
`_MAX_SUGGESTION_SCORE` and every formula stays proportional.

## Bucket-level formulas

### `library_core`

Authors who appear on papers in your saved Library.

For each (candidate, library-paper) pair, contribute:

$$
\frac{\text{rating\_w}(p) \times \text{position\_w}(\text{pa}) \times \text{recency\_w}(p)}{\sqrt{N_{\text{authors}}(p)}}
$$

with:

| Factor | Mapping |
|---|---|
| `rating_w(p)` | 0:1.0 (unrated = neutral) · 1:0.2 · 2:0.5 · 3:1.0 · 4:2.0 · 5:3.0 |
| `position_w(pa)` | first/last:1.5 · middle:1.0 |
| `recency_w(p)` | 1.3 if year ≥ current_year - 3 else 1.0 |
| `N` | author count of `p` from `publication_authors` |

Sum over the candidate's library papers gives
`weighted_contribution`. Per-bucket score:

$$
\text{score} = \min\left(_{\max},\ 24 \cdot wc + \sum_{t \in T} 8 \cdot \text{prevalence}(t) + \sum_{v \in V} 6 \cdot \text{prevalence}(v)\right)
$$

`24` is the outer multiplier that puts the band around 0–100;
topic / venue overlap contributions are prevalence-weighted
(see [topic / venue weighting](#topic-venue-prevalence-weighting)
below).

A 5★ first-author of a 1-person paper saturates near 100; a
middle author of a 30-person consortium paper rated neutrally
lands around 7.

### `cited_by_high_signal`

Authors whose works are cited by your Library papers rated ≥ 4★.

For each (candidate, library-citing-paper) pair, contribute:

$$
\frac{\text{citing\_rating\_w} \times \text{position\_w}(\text{pa}) }{\sqrt{N_{\text{cited\_authors}}}}
$$

with `citing_rating_w` = 1.5 if 5★ else 1.0 (the `min_rating=4`
gate already drops 1-3★). Sum gives `weighted_endorsement`.
Per-bucket score:

$$
\text{score} = \min\left(_{\max},\ 30 \cdot we + 4 \cdot c\right)
$$

where `c` is the count of distinct cited papers (a small
breadth tiebreaker).

### `adjacent`

Two SQL passes, OR'd:

1. **Citation-graph proximity** — authors whose papers are
   directly cited by your Library papers (joined via
   `publication_references`).
2. **Topic / venue overlap fallback** — authors whose
   publication record shares ≥ 2 of your top 12 library topics
   OR ≥ 1 of your top 8 library venues.

Per-bucket score:

$$
\text{score} = \min\left(_{\max},\ 20 sp + 8 lp + 4 rp + 8 \sum \text{topic\_prev} + 6 \sum \text{venue\_prev} + 5 |\text{shared\_lib\_authors}|\right)
$$

with `sp` = shared papers, `lp` = candidate's local paper count,
`rp` = recent local paper count.

### `semantic_similar`

SPECTER2 cosine of the candidate's paper-embedding centroid
against your Library centroid (helper:
`_semantic_similar_candidates`).

$$
\text{score} = \min\left(_{\max},\ 90 \cdot \text{cos} + \min(\text{embedded}, 10)\right)
$$

A 0.9 cosine maps to 90; the small `embedded` term is a tiebreak
for candidates with more than one embedded paper.

### `openalex_related` / `s2_related`

Pure cache reads from `author_suggestion_cache`, populated
asynchronously by `POST /authors/suggestions/refresh-network`.
Each cached row carries a `composite_score` ∈ [0, 1] computed
externally; the bucket simply rescales:

$$
\text{score} = \min\left(_{\max},\ 100 \cdot \text{composite}\right)
$$

Each network bucket gets `network_slot_cap = max(2, ⌈limit/3⌉)`
*new* slots so that even a Library that saturates `library_core`
still sees external suggestions. Overlap with prior buckets
feeds the consensus pass, not the slot cap.

## Topic / venue / author prevalence weighting

`_top_topics_for_library(db, limit=12)` and
`_top_venues_for_library(db, limit=8)` return
`{label: paper_count}`. `_build_prevalence_weights` converts to
log-normalized weights:

$$
\text{prevalence}(t) = \frac{\log(1 + \text{count}(t))}{\log(1 + \text{count}_{\max})}
$$

so the top library topic = 1.0 and a topic with count=1 in a
library where the max is 20 gets ≈0.23.

The same `log_prevalence_weights` transform applies to
**`author_affinity`** in `discovery/scoring.py`. Authors used to
be linearly max-normalized on the rationale that "you wrote with
this person or you didn't" — but on heavily skewed libraries (one
PI on 70% of saved papers) that scheme floored every other author
at <0.1 and let the dominant author crowd the top-K. Log-prevalence
gives a co-author on 5 of 100 saved papers a meaningful `0.4`
instead of an invisible `0.05`. The structural per-author cap in
`engine.diversity_interleave` is the second guardrail.

`_weighted_overlap_score(shared, weights, scale)` sums prevalence
weights for the candidate's overlap × scale. This is what the
`8 ∑ topic_prev` / `6 ∑ venue_prev` terms in the bucket formulas
above mean. Multipliers were bumped from the pre-2026-05 values
of 5 / 4 so a top-topic match is *more* valuable than the old
equal-count scheme, not just redistributed.

## Multi-source consensus bonus

After all buckets run, each candidate's `consensus_buckets` list
contains the labels of every bucket that surfaced them. The
post-pass adds:

$$
\text{bonus}(N) = _{\text{frac}} \cdot _{\max} \cdot \sqrt{N - 1}
$$

where `_frac = _CONSENSUS_BONUS_FRACTION = 0.12` and `N =
len(consensus_buckets)`.

| N | Bonus today |
|---|---|
| 1 | 0 |
| 2 | 12 |
| 3 | ~17 |
| 4 | ~21 |
| 5 | ~24 |
| 6 | ~27 |

Diminishing returns are intentional: 5+ buckets agreeing is
strong evidence but should never trivially saturate the band
against a high-confidence single-bucket signal.

For overlap to even be detected, each bucket helper passes only
`followed_ids` to its SQL `exclude_ids` parameter (NOT
`followed_ids | seen_candidates`). The loop body's
`if oid in seen_candidates: _record_consensus(...)` then captures
the multi-bucket appearance instead of dropping the row.

## Dismissal cluster penalty

`_load_dismissal_signature(db, lookback_days=100)` builds four
dicts from authors with `signal_value < 0` in
`missing_author_feedback` over the lookback window:

| Signature | Shape | Built from |
|---|---|---|
| `topic_sig` | `{topic: dismissed_author_count}` | `publication_topics` join |
| `venue_sig` | `{venue: dismissed_author_count}` | `papers.journal` join |
| `coauthor_sig` | `{coauthor_oid: shared_paper_count}` | `publication_authors` self-join |
| `institution_sig` | `{institution: dismissed_author_count}` | `publication_authors.institution` |

Coauthor signature uses **paper count, not dismissed-author
count** — collaboration depth is the relevant signal: a candidate
on 5 papers with one dismissed author is more cluster-bound than
one on 1 paper each with 5 dismissed authors. The per-hit penalty
is intentionally low (`0.008 × _MAX = 0.8` per shared paper)
because dismissing an author often means "not this person", NOT
"none of their co-authors". Only deep collaboration (10+ shared
papers) climbs to a meaningful penalty (≥ 8 points); a single
co-authorship barely registers.

`_dismissal_overlap_penalty` computes the per-candidate penalty:

$$
\text{penalty} = \min\left(_{\text{cap}},\ \sum_t \text{topic\_sig}[t] \cdot p_t + \sum_v \text{venue\_sig}[v] \cdot p_v + \text{coauthor\_sig}[c_{oid}] \cdot p_c + \sum_i \text{inst\_sig}[i] \cdot p_i \right)
$$

with per-hit constants from the top of `authors.py`. Topic /
venue / institution use list-overlap; coauthor is a single-ID
match against the candidate's own `openalex_id`.

The cap (`_DISMISSAL_PENALTY_CAP = 30.0`) is load-bearing: it
prevents the rail from permanently zeroing a candidate based on
cluster overlap alone. Explicit dismissal is the only mechanism
that fully removes someone.

Penalties land on each entry as a `dismissal_penalty` field for
debugging / UI, and are subtracted from the per-bucket score
**after** the consensus bonus, **before** the per-bucket weight
multiplier. Ordering rationale: consensus is positive evidence
about the bucket signal; dismissal is a learned negative that
must attenuate even confirmed candidates; bucket weight is the
final tunable normalization.

## Per-bucket weights

Stored under `discovery_settings.author_suggestion_weights.*`.
Defaults from `alma.discovery.defaults`:

| Bucket | Default weight | Rationale |
|---|---|---|
| `library_core` | 1.0 | Strongest evidence — direct co-authorship. |
| `cited_by_high_signal` | 0.9 | Uses ratings end-to-end now; nearly equal to library_core. |
| `openalex_related` | 0.9 | External discovery; equal-footing-ish so the rail isn't dominated by local data. |
| `s2_related` | 0.9 | Same as openalex_related; independent source. |
| `semantic_similar` | 0.8 | Less interpretable than the others, so slightly lower. |
| `adjacent` | 0.7 | Citation/topic adjacency is a weaker primary signal than direct co-authorship. |

The weight applies to the per-bucket raw score AFTER the
consensus bonus and dismissal penalty:

```
final = weight × min(_MAX, raw_bucket_score + consensus_bonus - dismissal_penalty)
```

## Final sort and trim

After weighting, candidates are sorted by:

1. `-score` (highest first)
2. bucket priority (`library_core` < `cited_by_high_signal` ==
   `adjacent` < `semantic_similar` < network buckets) — only
   matters as a tiebreak between equal scores.
3. `-local_paper_count`, `-recent_paper_count`, then name.

Then **same-human dedup** collapses entries whose normalized
display names match (handles OpenAlex split profiles for the same
human; the highest-scoring row wins, dropped IDs go to
`alt_openalex_ids` on the survivor).

Finally, `_diversify_final` trims to the requested limit while
guaranteeing at least one slot per populated bucket so a
high-volume bucket cannot crowd out the others.

## Per-suggestion fields

Each entry returned by `list_author_suggestions` carries:

| Field | Purpose |
|---|---|
| `score` | Final 0–100 number after consensus + dismissal + weight. |
| `suggestion_type` | The primary bucket label (used for the UI chip). |
| `weighted_contribution` | Raw `library_core` SUM (when applicable). |
| `weighted_endorsement` | Raw `cited_by_high_signal` SUM (when applicable). |
| `consensus_buckets` | List of bucket labels that surfaced this candidate. |
| `consensus_count` | `len(consensus_buckets)`. |
| `dismissal_penalty` | Subtracted points from cluster penalty (only set when > 0). |
| `signals` | Priority-ordered evidence chips for the UI ("co-author of X", "SPECTER 0.83", …). |
| `shared_topics` / `shared_venues` / `shared_followed_authors` | Display-side overlap lists. |

## Tests pinning the contract

`tests/test_author_suggestions_scoring.py` covers:

- Consortium middle-author down-weight via `1/√N`.
- Rating-based separation of co-authors (5★ vs 1★).
- Cited-by-high-signal lead vs consortium-middle.
- Top-topic match outranking rare-topic match
  (prevalence weighting).
- Dismissal penalty firing on topic, coauthor, and institution
  cluster overlap.
- Multi-source consensus bumping above single-source.
- Unrated rating=0 treated as neutral (=3), not negative.

When changing any constant or formula above, update or add a
test there. The project-internal lessons file (`tasks/lessons.md`,
gitignored) captures the rationale and gotchas under the headings
"Author suggestion scoring: weight, don't count", "Author
suggestion buckets must collect consensus", "Topic / venue
overlap is not a count", and "Dismissal propagation".
