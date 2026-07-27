---
title: Discovery pipeline — retrieval, fusion, features, ranking
---

# Discovery pipeline

How a paper you have never seen becomes a suggestion, end to end: which lanes
look for it, how their disagreeing opinions are combined, what is measured about
it, how it is scored, and what stops the whole loop from feeding on itself.

Per-signal formulas live in [scoring.md](./scoring.md). This page is the
architecture and the reasoning.

```text
lane runs
  → RetrievalHit[]            every appearance kept, never a winner-only dict
  → identity + evidence merge  field-level union across sources
  → two-level family RRF       fuse inside a family, then across families
  → top-200 recall pool
  → bounded enrichment / frontier cache
  → immutable feature snapshot reward + exposure, versioned and timestamped
  → prior-centred score        exact linear contributions = the explanation
  → MMR slate diversification
  → known-probability exploration slots
  → persist candidates + recommendations
  → log actual item impressions + linked outcomes
```

---

## 1. Retrieval: four evidence families

A **family** groups retrievers that fail the same way. Fusing inside a family
first is what stops sixteen topic queries from outvoting one vector run.

| family | retrievers | what it knows | how it fails |
|---|---|---|---|
| `lexical` | per-topic query over local corpus + frontier | surface wording | misses paraphrase, rewards common words |
| `semantic` | SPECTER2 kNN per branch centroid, over corpus ∪ frontier | meaning | confidently wrong on jargon collisions; blind without a vector |
| `citation` | local references, coupling, co-citation, OpenAlex `cites:`, S2 related, Personalized PageRank | who builds on whom | silent on brand-new work with no citations yet |
| `taste` | author-id lane, venue lane, followed authors, S2 recommendations, branch queries | your declared interests | echoes what you already know |

They are deliberately **not** merged into one score before fusion. Each is a
different view of "relevant", and their disagreement is information.

### Why lanes query the way they do

Three corrections that changed what the lanes actually retrieve:

**OpenAlex boolean syntax.** OpenAlex parses `search` with Elasticsearch
query-string syntax: adjacent bare words are combined with an implicit **AND**,
and operators must be uppercase. So the historical `" OR ".join(topics)` did not
mean what it looked like —

```
working memory OR visual cortex      →  working AND (memory OR visual) AND cortex
```

a conjunction of four unrelated tokens. `openalex_related.build_openalex_query`
quotes multi-word phrases so the expression means the union it claims. Measured
live: the old form returned 1,976,906 works headed by *"A Tutorial on Support
Vector Machines for Pattern Recognition"* — matched purely on the loose token
`recognition`. The quoted form returns 659,765 headed by Lowe's SIFT, CIFAR and
Hubel & Wiesel.

**Author and venue lanes need filters, not free text.** OpenAlex `search`
covers title, abstract and fulltext. It does **not** index author names or venue
names. Sending "Jane Smith" as free text retrieves papers that *mention* Jane
Smith. The lanes now use `filter=authorships.author.id:A123` and
`filter=primary_location.source.display_name.search:…`.

**Semantic Scholar endpoint choice.** `/paper/search/bulk` has no relevance
sort — its default is `paperId:asc`, effectively a hash order — so it is used
only for recent ingestion with an explicit `year` filter and
`sort=publicationDate:desc`, then re-ranked locally. General Discovery uses the
relevance-ranked `/paper/search`. The recommendations endpoints are separate
sources: `POST /recommendations/v1/papers` returns brand-new work (pos + neg
seeds), `GET /forpaper?from=all-cs` returns all-time related work.

### The hit contract

Every lane emits one `RetrievalHit` per **(candidate, run)** — not per
candidate:

```python
RetrievalHit(candidate_key, family, retriever_id, source_api,
             rank, pool_size, raw_score, query_key, seed_key,
             relation, branch_id, branch_mode, retrieved_at)
```

A paper found by four topic queries carries four hits. That is the evidence the
fusion and consensus features consume, and it is exactly what the previous
winner-takes-all merge discarded. `retriever_id` names the *algorithm surface*,
never the API — API identity must never become an extra relevance vote.

---

## 2. Fusion: two-level reciprocal rank fusion

Scores from different retrievers are not comparable. A SPECTER2 cosine of 0.86
and an OpenAlex relevance score of 41.2 and a rank-decay of 0.7 mean nothing to
each other. Summing them — which is what the old channel merge did — lets the
channel with the narrowest score range dominate silently. The dense lane emitted
`(cos+1)/2`, and same-domain SPECTER2 cosines run 0.6–0.9, so **every** vector
candidate arrived at 0.80–0.95 with almost no dynamic range, at the heaviest
channel weight.

RRF fixes this by discarding the scores and keeping only the ranks
([Cormack et al., SIGIR 2009](https://research.google/pubs/reciprocal-rank-fusion-outperforms-condorcet-and-individual-rank-learning-methods/)):

```
rrf_weight(rank) = 1 / (RRF_K + rank)          RRF_K = 60
```

`RRF_K = 60` is the constant from that paper. It compresses the gap between
rank 1 and rank 10 enough that **agreement across retrievers outweighs any
single retriever's top slot**. It is declared once, in
`core.scoring_math`, alongside `rrf_weight` and `rrf_score_normalized`.

Two levels:

1. **Inside a family** — sum the top few `rrf_weight` values per retriever, then
   take the *mean across retrievers*. The mean is deliberate: a family with more
   configured APIs must not gain a structural advantage from that alone.
2. **Across families** — rank candidates within each family, convert to
   `rrf_score_normalized` (rank 1 → 1.0), and blend by the lens's channel
   weights.

RRF is robust unsupervised fusion. It is not claimed to be universally optimal —
it is the right default when you cannot calibrate the retrievers against each
other, which is our situation.

---

## 3. Features: reward versus exposure

Every atomic value is computed and persisted at ranking time with a
`feature_schema_version`, a `feature_timestamp`, the value, its availability,
and an evidence count. Nothing is recomputed later and back-filled into a
training row.

### Reward features — what the paper *is*

- **Semantic** — max similarity to individual positives, top-k support, max
  similarity to negatives, signed margin, per-branch/cluster similarity.
- **Lexical** — word overlap, character n-gram, TF-IDF/term similarity, kept
  separate rather than averaged.
- **Topic** — weighted overlap, matched support, negative overlap, ontology
  distance. No raw high-cardinality topic id.
- **Author** — max/mean/**first**/**last** affinity (last author is the lab in
  life sciences; averaging destroys that), followed flag, negative affinity,
  co-author graph distance. Stable author ids required.
- **Venue** — affinity, library/negative membership, preprint/type.
- **Citation intrinsic** — log citations, year percentile, FWCI, reference
  count, bibliographic coupling, co-citation, PPR.
- **Taste propagation** — separately for references, citers and co-cited
  neighbours: prior-shrunk signed mean of *explicit* outcomes plus a support
  count, library/loved fractions. Only outcomes known at feature time.
- **Lifecycle** — age, retraction, open-access status. A missing date is not a
  relevance penalty.
- **Explicit projection** — all nine `ProjectedPaperSignals` axes (`paper`,
  `author`, `author_name`, `topic`, `venue`, `keyword`, `tag`,
  `semantic_neighbor`, `citation_neighbor`) logged raw, before any weighting or
  clamping.
- **Retrieval evidence** — family-level RRF components and the count of
  independent evidence families.

### Exposure features — how the paper *got here*

Source/lane identity, raw rank, pool size, query, metadata completeness, missing
masks, hydration path, timeouts, slate position, and the exact selection
probability.

**These never enter the reward model.** They describe the current retrieval
policy, not the paper. Fitting on them teaches the ranker "candidates from lane
X get engaged with", which is true only because lane X is what the deck was made
of — the model learns to reproduce its own logging policy.

This is not theoretical. `is_lexical_fallback` — a flag recording which *scoring
path* ran — produced the largest sign flip in the whole feature set (marginal
Cohen's d −0.67, partial β +1.397) and was then shown to be an artifact of a
write-then-read race, not a property of any paper. Provenance features are nine
more of that shape.

### Forbidden as reward inputs

API/source/lane identity · metadata completeness · timeout or hydration path ·
the candidate's own label · a neighbour's model score · future feedback or
future graph state · raw geometry coordinates · deterministic composites of
other inputs.

That last one has a concrete case: `usefulness_boost` was
`0.45·novelty + 0.25·recency + 0.20·citation_quality + 0.10·metadata_quality`,
a composite of features that were *already independent inputs to the same linear
model*. It double-counted recency and citation quality, made the fit collinear
by construction, and had the largest marginal effect of any feature (d = −1.84)
while being absent from every accuracy report. It is now computed and logged
for diagnostics but **carries no weight**; its atomic ingredients remain.

---

## 4. Ranking

Nine weighted signals feed the production score:

```
source_relevance · topic_score · text_similarity · author_affinity
journal_affinity · recency_boost · citation_quality · feedback_adj
preference_affinity
```

### Why not a bigger model, yet

The honest constraint is data, not ambition. The dev database has **106 engaged
rows covering 96 distinct papers**, with 32 negative outcomes. Measured on that
data with 5-fold × 10-repeat CV:

| model | CV AUROC |
|---|---|
| shipped hand weights | 0.778 *(in-sample; honest CV is lower)* |
| fitted ridge linear | 0.884 ± 0.070 |
| + context features | 0.890 ± 0.077 |
| GBDT, depth 3 | 0.928 ± 0.057 |

Those intervals are **not** trustworthy as printed: repeated k-fold reuses the
same points across overlapping folds, and the across-fold standard deviation
systematically underestimates generalisation variance
([Nadeau & Bengio, 2003](https://link.springer.com/article/10.1023/A:1024068626366);
[Bengio & Grandvalet, JMLR 2004](https://jmlr.org/papers/v5/grandvalet04a.html)
show there is no unbiased estimator of it). With 32 minority events, the
conventional 10-events-per-variable floor admits roughly **three** parameters —
even the linear model is over-parameterised.

So the production ladder is deliberately conservative:

1. **Now** — repaired hand prior over non-duplicated family composites.
2. **Shadow now** — prior-centred logistic ridge, penalising `‖β − β_prior‖²`
   rather than `‖β‖²`. It degrades continuously to the shipped behaviour as data
   thins, needs no fallback ladder, and its exact per-candidate contributions
   *are* the explanation — no SHAP approximation layer.
3. **Promotion** — only on forward v3 observations, rolling-origin, grouped by
   suggestion set **and canonical paper** so one paper's outcome can never
   appear on both sides of a split.
4. **Wide feature families / projection axes** — activated per family behind
   coverage and outcome thresholds, never all at once.
5. **Interactions and non-linear challengers** — only past ~400 valid engaged
   observations, with strong heredity, pre-declared crosses or stability
   selection, and a selected-max permutation test.

That last gate is not hypothetical either. The strongest interaction found in a
91-pair sweep, `author_affinity × citation_quality` (+0.0201 CV AUROC), was
tested against the null distribution of the *selected maximum* under label
permutation: null median **+0.0300**, observed **+0.0223**, **p = 0.703**. The
best of 91 candidates was weaker than what noise produces. It was dropped.

Tabular foundation models (TabPFN, TabICL) currently lead small-tabular
benchmarks and may become offline challengers, but none ships here: 106 biased
labels, no valid historical propensities, and an explanation contract to honour.

---

## 5. Exploration and honest evaluation

Every label we have comes from a paper the current ranker chose to show. Fitting
on that without correction tightens a loop until Discovery can only surface what
it already surfaces — the central problem in
[counterfactual learning to rank](https://arxiv.org/pdf/2005.10615).

Two mechanisms, both required:

- **Reserved exploration positions.** After MMR diversification, four slate
  positions are drawn uniformly without replacement from a declared eligible
  tail, then randomly permuted across those positions. The pool size, RNG and
  policy version, marginal inclusion probability, conditional position
  probability, exploration flag and final position are all logged.
- **Actual impressions.** The frontend logs which recommendation ids were
  really visible, with position and timestamp. A lens-level "seen" stamp is not
  an item impression, and an unviewed item is **never** a negative.

Clipped SNIPS / counterfactual estimates apply **only** to the randomised
post-v3 cohort where selection and position probabilities are known. Historical
rank is not a propensity and no propensity is invented for old rows. Deterministic
exploit history does not become unbiased by relabelling.

Re-scoring old rows with today's vectors or today's graph is a **diagnostic**,
never training data — the features would carry information from after the
decision was made.

---

## 6. The frontier: how new papers enter at all

The dense lane used to query `publication_embeddings JOIN papers`. It could only
return a paper already in the corpus, which meant the heaviest retrieval channel
structurally contributed **zero** new papers, and everything genuinely new had to
come from live text search inside the request. A lens refresh measured 191 s
average, 792 s worst, against a <10 s target — all of it blocking HTTP.

The frontier separates universe construction from ranking:

- **Offline** (background job) — walk the citation graph outward from the
  Library, resolve metadata and SPECTER2 vectors, persist to
  `discovery_frontier` / `discovery_frontier_edges`.
- **Online** (refresh) — a numpy matmul against a preloaded matrix.

Sources: bibliographic coupling, co-citation and citers, S2 new recommendations,
S2 all-time related, OpenAlex query/taste lanes, Europe PMC, plus residual S2
batch vector fill and local SPECTER2 fill.

What picks the candidates costs nothing: bibliographic coupling is computed from
reference edges we already hold. On the dev corpus, 395,560 stored edges yield
5,178 works cited by a Library paper that we have no metadata for — the frontier
is already implied by the database.

A frontier row is a **lead, not a corpus citizen**. It carries no membership
state, so it never enters the map, Insights counts, dedup, or any preference
query. It is promoted into `papers` only when actually staged as a suggestion.

### Personalized PageRank

A random walker starts on your Library (or only your loved papers), follows
citation edges, and restarts from the seeds with probability `1 − α` (α = 0.85).
The stationary distribution ranks every reachable paper by proximity to your
region of the literature.

The graph is **symmetrised** before the walk. Citation edges are directed, but
influence flows both ways for similarity — a paper is close to your taste
whether it cites your Library or is cited by it. An asymmetric walk drifts
toward highly-cited hubs, which is prestige, not proximity. Dangling mass
returns through the personalized restart vector, not a uniform one, so the walk
solves the intended Markov chain.

Measured: 0.35 s over 395,560 edges.

**Why this in addition to the embedding.** SPECTER2 is itself trained on
citation triplets, so dense retrieval already encodes citation structure —
smoothed and globally. PPR is the exact, local view: it knows *this* paper is two
hops from *that* specific paper you loved, which a 768-dimensional average cannot
represent. They fail differently, which is what makes fusing them worth more than
either alone.

---

## 7. Signal Lab: where the game results actually go

Signal Lab is the triad game — you answer "which of these is closer to what you
want?" rounds over the map. Three distinct things are done with the answers.

### 7.1 What it fits

Answers accumulate into a model with two heads, refit on a debounce
(`signal_lab.refit_every_rounds`, default every 5 rounds):

- **`region_offsets`** — a per-super-region scalar: does this region of the
  corpus run hotter or colder than your baseline?
- **`utility_delta`** — a vector head in embedding space for within-region
  preference.
- **`lab_author_offset`** — the same idea over the author map, folded into the
  canonical author signal rather than scoring separately.

Two guards are built into the fitting loop. A **ring prior**
(`gamma_start`, default 0.35) decays influence outward, so ring 1 pulls about a
third as hard as ring 0. An **ε-greedy exploration share**
(`signal_lab.epsilon`, default 0.20) reserves a fifth of rounds for
ring-uniform sampling — the self-confirmation guard, the same principle as the
deck's exploration slots. `holdout_percent` (15%) is stamped at round creation
for honest evaluation, and `coverage_target` (20 answered rounds per region,
set from stage-0 empirics where error plateaued at 20 rather than 10) governs
when a region's uncertainty relaxes.

### 7.2 How it reaches recommendations

Through three separate doors, and all three are currently **shut by default**:

```
weights.lab_region_offset  = 0.0
weights.lab_utility        = 0.0
weights.lab_author_offset  = 0.0
```

`signal_lab/scoring_terms.py::load_lab_scoring_context` early-returns `None`
when both scoring weights are ≤ 0, so at the defaults the lab context is never
even loaded and scoring is **byte-identical to a lab-less build**. This is
deliberate (decision D20): a head is promoted manually, one at a time, only on
stage-1 held-out evidence plus churn evidence. A model fitted on a few hundred
game answers does not get to silently move your deck.

### 7.3 The separate `preference_affinity` signal

Distinct from the heads above, `preference_affinity` is one of the nine weighted
signals, sourced from `feedback_substrate.get_preference_affinity_signal`. It
aggregates topic and author affinity from `preference_profiles` and only
produces a non-zero value past a minimum interaction count, ramping from 0.3 to
1.0 over 2–10 interactions.

**Its measured state is worth stating plainly.** On the dev database it is the
constant `0.500` on 198 of 233 scored rows — 85% — because Signal Lab has only
17 answered rounds. It carries weight 0.10 on a feature with essentially no
variance. That is not a weighting problem to be tuned away; it is an empty
input. The fix is upstream (more rounds, or replacing the signal with the
K-dimensional cluster-anchored vector, which carries the same "which region of
the field is this, and do I like that region" information without needing game
rounds at all).

### 7.4 What Signal Lab may never do

Move a coordinate. The locked geometry rule is that position encodes what the
*literature* says a paper is about, never what you think of it. Signal Lab
output may tint the map at read time (`signal_lab.map_tint_strength`) and may
influence ranking; it may not enter layout, clustering, or community assignment.
`tests/test_geometry_admission_contract.py` enforces this by import graph — the
layout builders may not even import the lab model — so the rule survives
refactors rather than depending on reviewer memory.

The same reasoning admits PPR: seeding a walk reads Library membership as a node
*attribute* to choose a starting distribution. It is retrieval targeting, which
is ranking, not geometry.

---

## 8. Where each piece lives

| concern | module |
|---|---|
| shared scoring math (clamp, RRF, query match) | `core/scoring_math.py` |
| candidate identity | `core/utils.py::candidate_dedup_key` |
| hit contract | `application/discovery/retrieval/types.py` |
| hit emission + families | `application/discovery/retrieval/_common.py` |
| lanes | `application/discovery/retrieval/{lexical,vector,graph,external}.py` |
| two-level fusion | `application/discovery/retrieval/merge.py` |
| feature assembly | `application/discovery/features.py` |
| ranker | `application/discovery/ranker.py` |
| exploration | `application/discovery/exploration.py` |
| observations / impressions | `application/discovery/observations.py` |
| frontier | `application/discovery/frontier.py` |
| PPR | `application/discovery/ppr.py` |
| per-signal formulas | `discovery/scoring.py` |
| OpenAlex query construction | `discovery/openalex_related.py` |
| S2 transport + field contracts | `discovery/semantic_scholar.py` |
| Signal Lab heads | `application/signal_lab/` |

## See also

- [Scoring formulas](./scoring.md) — the per-signal arithmetic
- [Discovery concepts](../concepts/discovery.md) — the product view
- [Signal Lab](../concepts/signal-lab.md) — the game itself
- [External APIs](./external-apis.md) — endpoint contracts, limits, batching
- [Tuning Discovery](../user-guide/tuning-discovery.md) — the knobs
