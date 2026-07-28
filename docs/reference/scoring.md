---
title: Scoring formulas
description: The ten ranking families, in numbers.
---

# Scoring formulas

A Discovery candidate is a paper proposed by one of the
[retrieval families](./discovery-pipeline.md#1-retrieval-four-evidence-families).
One ranker turns it into a number, on every surface that shows a paper score.
This page documents that arithmetic: the families, their weights, and the atoms
each family is built from.

> **Read [Discovery pipeline](./discovery-pipeline.md) first** for the
> architecture this sits inside: how lanes retrieve, how their ranks are fused
> (two-level RRF), which features are admissible as *reward* versus *exposure*,
> why the ranker is a prior-centred linear model rather than a bigger one, how
> exploration keeps the feedback loop honest, and where Signal Lab enters.

## One ranker, ten families

Every paper score in ALMa — Discovery, Feed, Online Search — is produced by
`alma.application.discovery.ranker`, in two steps that are always taken
together:

1. **Measure.** `alma.discovery.scoring.measure_candidate` observes everything
   observable about a candidate and returns a breakdown. It deliberately
   returns **no score**: it decides nothing about importance.
2. **Rank.** `ranker.rank_candidate` (single) or `ranker.apply_repaired_prior`
   (bulk) turns that measurement into a number.

Keeping them apart is the point. Until 2026-07-28 the scorer *also* combined
its own measurements into a composite — nine weights, a consensus bonus, a
citation-fabric bonus, a Signal Lab bonus and a 30-point dismissal penalty —
and the ranker then discarded that number and replaced it. The composite was
dead on Discovery and live on Feed and Online Search, so the same paper scored
differently depending on which page you opened, and the UI drew the discarded
decomposition beside the surviving score. The combination stage is gone.

### The score

For candidate $c$, over the families $F$ that were **measured** for it:

$$
\text{score}(c) = 100 \cdot \mathrm{clip}_{[0,1]}\!\left(
  \sum_{f \in F} \hat{w}_f \cdot v_f(c)  -  0.45 \cdot \mathbb{1}[\text{retracted}]
\right),
\qquad
\hat{w}_f = \frac{w_f}{\sum_{g \in F} w_g}
$$

Two properties follow, and both are load-bearing:

* **Weights are FIXED — never rescaled per paper.** A score is a ranking key;
  its only job is comparing papers, so a denominator that changes per paper
  destroys exactly that. A family that could not be measured is **imputed at
  its corpus prior mean** (`FamilySpec.prior_mean`), so not knowing something
  neither helps nor hurts, and two papers scoring 69 mean the same thing.

  v0.22.0 briefly renormalised over the measured families instead. That broke
  comparability, and with a bias: the families that go missing are the ones
  papers score badly on (corpus means: citation 0.28, lexical 0.27, semantic
  0.51, against feedback 0.96 and topic 0.74), so dropping a weak family and
  handing its weight to the strong ones was a free upgrade — a paper rose by
  having less evidence. Prod showed Feed rows (three families missing)
  averaging 68.1 against Discovery's 62.0 with all ten. Zero-filling is the
  opposite error: it ranks by hydration completeness, which is the trap that
  got `usefulness_boost` deleted.
* **Closure.** The published explanation's family points, adjustments and
  clipping term sum to the final score exactly. The UI renders that sum, so the
  bars and the number can never be different quantities again. Guarded by
  `tests/test_score_explanation_closure.py`.

### The families

`ranker.FAMILY_SPECS` is the single source of truth: it declares each family's
atoms, their sub-weights and their combinator, and **both** the value and its
explanation are derived from it. There is no second table to keep in sync, and
the UI cannot describe a formula the scorer is not running.

| Family | Default weight | Built from |
|---|---|---|
| `semantic` | 0.14 | max(library centroid, closest exemplar, support set) − 0.5 · similarity to passed-on papers |
| `topic` | 0.20 | `topic_score` |
| `retrieval` | 0.15 | 0.75 · max(RRF over the four channels) + 0.25 · (channels that agreed / 4) |
| `author` | 0.15 | `author_affinity` |
| `lexical` | 0.06 | 0.45 word + 0.35 char n-gram + 0.20 key term − 0.5 · overlap with passed-on papers |
| `recency` | 0.10 | `recency_boost` |
| `citation` | 0.05 | 0.50 `citation_quality` + 0.10 (`fwci`/3) + 0.20 max(coupling, co-citation) + 0.20 max(PPR library, PPR loved) |
| `feedback` | 0.10 | `feedback_adj` |
| `preference` | 0.10 | `preference_affinity` |
| `venue` | 0.05 | `journal_affinity` |

Weights come from `discovery_settings.weights.*` (Settings → Discovery). One
slider, `weights.text_similarity`, drives two families — `semantic` takes 70%
of it and `lexical` 30% — which is why the defaults above are 0.14 / 0.06 for a
slider set to 0.20. Weights are normalised to sum to 1 before scoring.

**Three combinators**, declared per atom:

* `sum` — adds `weight × value`.
* `max` — competes inside a group; the group pays its weight **once**, to the
  winner. This is how correlated views of the same evidence avoid being
  double-paid: three similarity views of one embedding, two graph views of one
  citation neighbourhood, four retrieval channels ranking the same paper.
* `penalty` — subtracts, and never makes a family "available" on its own.

**Negative preference** enters through the `penalty` atoms
(`semantic_similarity_negative_raw`, `lexical_similarity_negative_penalty`) —
inside the families, bounded by them. The old free-standing 30-point dismissal
cluster penalty is retired.

**Multi-source agreement** enters as the `retrieval_family_count` atom. The old
free-standing consensus bonus is retired for papers. (The *author suggestion*
rail keeps its own consensus bonus and dismissal penalty — a separate,
independent implementation documented in the second half of this page.)

### Recommendation mode

`recommendation_mode` (`balanced` / `explore` / `exploit`) multiplies family
weights before normalisation, so the modes are zero-sum:

| Mode | Multipliers |
|---|---|
| `explore` | recency ×1.5; citation, author, venue ×0.5 |
| `exploit` | author, venue, preference ×1.5; recency ×0.5 |

It also widens or narrows branch spread at seed time
(`seed_profile._resolve_branch_temperature`).

### The prior is a prior

These weights are a **prior**, not a fit. They are the centre that the shadow
prior-centred ridge model shrinks toward; see
[the ranker ladder](./discovery-pipeline.md#4-ranking) for what has to be true
before a fitted model is promoted over them.

## Signal atoms

The functions below are what `measure_candidate` computes. They are the atoms
the families above are assembled from — each one a measurement, with no opinion
about its own importance.

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

Does the candidate's author match the canonical author signal ALMa has
learned from your Library, ratings, graph context, and embeddings?

ALMa computes one shared author signal and reuses it here. The Discovery
ranker takes the stable components only:

| Component | What it means |
|---|---|
| Saved footprint | Saved Library papers by this author, normalized against the author with the largest saved footprint. |
| Rating | Average rating of saved papers by this author; 5★ is positive, 3★ neutral, 1★ negative. |
| Similarity | Author SPECTER2 centroid against the Library centroid. Author centroids use their own scale: cosine ≤0.35 is absent/0, cosine 1.0 is 100, and values between scale linearly. |
| Neighborhood | Co-authorship with your Library circle and cited-by-Library adjacency, using soft saturation so one/few links are visible but do not become 100. |

The volatile interaction component (fresh likes, dislikes, follows,
and projected paper feedback) flows through `feedback_adj` instead, so the
same feedback is not counted twice.

Range: -1…1 internally. Negative author affinity can lower candidates;
the UI displays positive affinity as 0…100.

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

**Second, gated input: Signal Lab's venue head.** Prevalence answers which
venues you save *from*. Signal Lab's *Same field* rounds — two papers on one
topic, differing on journal — answer which you would *choose between* at equal
topic, which nothing else in the ranker can learn (SPECTER2 does not encode the
journal). The fitted offsets are ADDED into this same map after normalisation,
behind `signal_lab.enabled` and `weights.lab_venue_offset`; at zero weight the
model view is never even read. There is deliberately no parallel `lab_venue`
signal — one question, one signal.
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

where $R$ is `discovery.limits.recency_window_years` (default 10).

Range: 0…1.

### `citation_quality`

Log-scaled citation count, with an "influential citation" floor:

$$
\text{eff} = \max(\text{cited\_by},\ 2 \times \text{influential\_citations})
$$
$$
\text{citation\_quality} = \min\!\left(1,\ \frac{\log(\text{eff} + 1)}{\log(1000)}\right)
$$

A paper with ~1000 effective citations gets ≈1.0; a paper with 0 gets
0. The `2 × influential_citation_count` floor lets a highly-influential
paper score well even with a modest raw count. The denominator is fixed
so the function is interpretable across candidates.

Range: 0…1.

### `feedback_adj`

Adjusts the score based on prior feedback on the candidate's
attributes (paper, topics, authors, venue, keywords, and tags). ALMa
reads three canonical preference sources through
`alma.application.signal_projection` and folds each into the same
per-paper signal map before fanning out:

| Source | Weight | What it captures |
|---|---|---|
| `feedback_events` (`paper_action` + legacy single-action types) | 1.0 | Canonical write path (save / like / love / dislike / remove). Historical dismiss events normalize to zero because visibility is not preference. |
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

Two configurable windows exist as defaults:

| Window | Default | Weight |
|---|---|---|
| `feedback_decay_days_full` | 90 | 1.0 |
| `feedback_decay_days_half` | 180 | 0.5 |

Note: the projection path that actually drives author-suggestion and
paper feedback (`application/signal_projection.py`) does **not** read
these settings — it hardcodes a tanh decay with a **180-day half-life**
and a **730-day max age**, beyond which weight tapers to 0.

Range before normalization: -1…+1. The weighted scorer stores it as a
0…1 value in the final score, and the explanation payload includes
`projected_feedback_raw` so the signed contribution remains visible.

### `preference_affinity`

A **confidence-weighted mean of your recorded affinities** for the entities this
candidate touches — its topics, its authors, and the source it came from — read
from `preference_profiles` (`feedback_substrate.get_preference_affinity_signal`).
It is not a vector distance; every entity that matches a profile row contributes
its `affinity_weight`, weighted by that row's `confidence`:

$$\text{raw} = \frac{\sum_i w_i c_i}{\sum_i c_i} \times \text{volume\_scale}$$

where $w_i \in [-1, 1]$ is the stored affinity, $c_i = \min(1,\,n_i/20)$ is that
entity's reliability from its own interaction count, and `volume_scale` ramps
0.3 → 1.0 over 2–10 interactions across the *whole* profile. Shifted to
$[0, 1]$ for the breakdown, so **0.500 means "no matching evidence"**.

Dividing by $\sum c_i$ rather than by the match count matters more than it
looks: until 2026-07-27 the code divided by the count, which turned a per-entity
reliability into a multiplier on the output. On a personal corpus an entity is
seen once or twice, so $c \approx 0.05$–$0.15$, and the signal was pinned within
±0.015 of 0.500 on 85% of rows. See
[discovery-pipeline §7.3](./discovery-pipeline.md) for the full diagnosis.

Range: 0…1. Requires `preference_profiles` to have entries; no embeddings needed.

### `source_relevance` boost per channel

`discovery_settings.weights` also carries per-channel multipliers used by the
retrieval phase, before ranking. They shape which candidates exist, not what
they score.

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
1.5×; one where explicit negative preference dominates falls toward 0.5×. The three
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

## Tuning

Three knobs change the balance:

* **Per-family weights** — Settings → Discovery. Lowering
  `weights.text_similarity` to 0.10 caps the semantic + lexical families at
  ~10% of the normalised budget between them.
* **Recommendation mode** — see above.
* **Per-lens overrides** — each lens carries its own `weights.*`, merged over
  the global defaults at refresh time.

The `recommendations` table caches the last batch, so re-tuning does not lose
results — only the next refresh applies new weights.

## Score breakdown

The stored `score_breakdown` carries the closed decomposition under
`explanation`, and `GET /api/v1/discovery/recommendations/{id}/explain` returns
it verbatim:

```json
{
  "explanation": {
    "ranker_version": "discovery-v4-family-prior",
    "final_score": 61.4,
    "families": [
      {
        "key": "semantic",
        "label": "Semantic",
        "description": "Embedding similarity to what you already keep.",
        "value": 0.76,
        "weight": 0.127,
        "points": 9.67,
        "available": true,
        "atoms": [
          {"key": "semantic_similarity_centroid_raw", "label": "Library centroid",
           "value": 0.81, "weight": 1.0, "role": "max", "group": "positive",
           "available": true}
        ]
      }
    ],
    "adjustments": [{"key": "retraction", "label": "Retracted", "points": 0.0,
                     "available": false}],
    "clipped": 0.0
  }
}
```

`Σ families.points + Σ adjustments.points + clipped === final_score`. The
paper card's **Why** panel renders exactly these rows, each expandable to its
atoms, and nothing else — so what you read is what ranked the paper.

Alongside `explanation`, the breakdown carries the raw measurements
(`semantic_similarity_*`, `lexical_similarity_*`, `coupling_strength`, …) and
retrieval provenance (`matched_query`, `consensus_count`, `provenance.*`).
Those are diagnostics and retrieval evidence — they explain why the paper
*surfaced*, not what it *scored*.

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
   liked/disliked/removed papers.
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
(see [topic / venue weighting](#topic-venue-author-prevalence-weighting)
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
