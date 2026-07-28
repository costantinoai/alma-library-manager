---
title: Signal Lab
description: Reversible calibration games that sharpen ranking and taste terrain without touching your Library.
---

# Signal Lab

Signal Lab is a first-class ALMa intelligence feature built from small
**calibration games**. It is not an integration plugin. Its daily interaction
lives on Home immediately above Inbox; its controls and evidence live under
**Settings → Intelligence → Signal Lab**.

Each round shows three papers and asks one cheap question. Answers train a
model that can sharpen paper scores and paper-map taste terrain. It is
**signal-only**: a round never changes Library membership, ratings, reading
state, feedback profiles, or semantic coordinates.

## The three rounds

| Game | Question | What it teaches |
|---|---|---|
| **Best / worst** | Which would you read first — and which would you skip? | What should score high: region taste offsets and a utility direction. |
| **Odd one out** | Which paper does not belong with the other two? | Relative semantic distance and region-boundary overrides. |
| **Same field** | These two are on the same topic — which would you rather read? | Venue preference at equal topic. |

Always answerable with **Can't tell**; a skip records no verdict. Home queues a
deck of at least ten signed rounds, shows progress through it, and can fetch
another deck without leaving the page. The day picks the default game and the
segmented toggle overrides it; best/worst is the most common, because it trains
the utility direction every other head is measured against.

### Why a matched pair is a different instrument

The two triplet rounds show papers that differ on **every axis at once** —
topic, venue, authors, method, era, writing. The single bit of preference they
produce cannot be attributed to any one of those without confounding it with
the rest, which is exactly why they fit only three things: region, a global
utility direction, and (from within-region rounds, where topic is roughly held
constant) authors.

A **matched pair** is drawn so the two papers agree on region and differ on
exactly ONE attribute. The same click then carries clean evidence about that
attribute, because nothing else varied. It is the difference between "I
preferred this paper" and "at equal topic, I preferred this venue".

**Venue is the first axis, and topics are deliberately excluded.** SPECTER2
does not encode the journal, so the learned utility direction cannot represent
venue preference at all, and `journal_affinity` is Library prevalence only — it
knows which venues you *save from*, never which you would *choose between*. No
existing signal can learn this. Topics are the opposite case: SPECTER2 space is
largely topical, so the utility direction already encodes topic preference and
`topic_score` estimates the same quantity from hundreds of real Library
decisions. A third estimator of one quantity is how a model becomes collinear
with itself — the trap `usefulness_boost` fell into before it was deleted.

Attribution happens at **fit** time, not in the game. A game sees paper ids and
nothing else, so it cannot know the venues and must not (games are I/O-free by
contract). The fit holds the paper→venue map and keys the attribution on the
game's declared contrast axis, which is also what keeps interpretation
retroactively fixable.

## The band on Home

The same band every other Home section is: a `PageSection` (chevron, icon,
title, count pill) whose one grey line is the question itself, over a
`PaperTileGrid` of the three papers. The verdicts ride each tile's `actions`
strip — the slot Home's Inbox already uses for triage, for the same reason:
this surface owns the decision and has nowhere to hand the paper off to. The
tiles carry no `href`; the paper is the subject of a question, not a link.

It stays about the height of one paper card, because it is a five-second ritual
above the Inbox, not a section you settle into.

The verdicts carry the app's own valence colours (the `PaperActionBar` tone
map): **success** for *Read first*, **critical** for *Skip*, and **accent** for
*Odd one out*, which is a categorical call rather than a good/bad one. The whole
cell takes the tint of the verdict it holds, so an answered round reads without
being parsed.

Best/worst shows both buttons on every paper, so both verdicts are visible at
once and each is independently revisable; the round records itself when both are
given, because the pair is the datum. Odd-one-out shows one button and records
on the single mark. Giving a paper one verdict lifts any verdict it holds
elsewhere: no paper is both the one you would read and the one you would skip.

At the foot, live evidence explains what the answers are doing: regions moving
up/down, boundaries getting sharper, rounds today/total, the fitted evidence
behind the current model, and region/edge coverage. This is evidence, not a
second diagnostics product — the knobs, the model and the eval live in
**Settings → Intelligence → Signal Lab**.

The ledger deliberately does **not** say “7 of 4.1 billion triplets”. For a
region with \(n\) papers, \(\binom{n}{3}\) correctly counts unordered
three-paper sets, but one giant region makes that number enormous and it says
nothing about learning. ALMa reports:

- recorded answers and skips;
- unique unordered query sets and accidental repeats;
- observations and constraints consumed by the current fit;
- rounds awaiting the next fit;
- super-regions and adjacent edges reached by answered rounds.

Paper order remains available for position-bias analysis, but does not create a
new statistical query.

## Where rounds come from

Rounds sample the corpus map's **super-regions**, starting around your Library
and expanding outward as inner regions are learned. Only judgeable papers
appear: a title plus an abstract or TLDR.

One request designs the whole 10–30-round sheet:

1. ALMa loads every eligible paper in every super-region. There is no
   first-800 SQL prefix. Region priority combines the Library-outward ring
   prior, sub-linear affected mass, current posterior disagreement, coverage,
   answer/skip rate, elapsed-time staleness, and a protected ring-uniform
   exploration share.
2. Best/worst candidates are sampled across the complete chosen region.
   Odd-one-out candidates combine low-margin papers on an adjacent edge with a
   25% broad-pool escape route, so a mistaken current boundary cannot trap the
   sampler.
3. With fitted bootstrap heads, ALMa evaluates all six ordered MaxDiff outcomes
   or all three odd-one-out outcomes and scores their expected information
   gain. Cold start uses a D-optimal contrast-geometry score. Best/worst also
   favours comparisons whose posterior ordering can still change.
4. Later sheet rows are chosen conditionally: a small posterior-member
   log-determinant gain discourages redundant directions, while paper,
   super-region, and edge exposure penalties spread attention. This is
   BatchBALD-style batch diversity, not a claim that the response outcomes are
   independent.
5. The same unordered query cannot repeat within a sheet or within the latest
   500 recorded answers/skips. Repeated-measure trials require a future,
   explicit noise-estimation policy; reloads never create them accidentally.

This follows the core ideas behind
[BALD](https://arxiv.org/abs/1112.5745) and
[BatchBALD](https://arxiv.org/abs/1906.08158), adapted to ALMa's hierarchical
region/edge goals and multi-outcome games. It is goal-directed active design,
not a promise that one heuristic wins on every corpus.

The card remains hidden until the stored `graph:super_regions` view exists.
Graph-layout maintenance builds it, or an explicit graph rebuild can trigger
the chain.

## Model and effects

Every answer writes exactly one `signal_lab_rounds` row. The
`signal_lab:model` materialized view is recomputed wholesale from that history;
there is no incremental accumulator.

The model has:

- James–Stein-shrunk per-region taste offsets;
- James–Stein-shrunk per-**author** offsets (see below);
- a Bradley–Terry utility direction with a bootstrap ensemble;
- non-negative diagonal semantic-distance weights, shrunk strongly toward the
  identity metric, with a bootstrap ensemble;
- region-boundary overrides from odd-one-out votes;
- held-out pairwise and metric accuracy for the fitted heads.

Query tokens carry a nonce and bind the game, papers, and region context. An
answer retry with the same nonce and content returns the existing row; a
different answer for an already-used nonce is rejected. Presentation writes
nothing. The first valid answer writes one row.

A token is its JSON claims followed by a truncated HMAC, and the MAC is split
off by **fixed width**. It used to be appended after a literal `.` and recovered
with `rsplit`, which silently corrupted any token whose binary MAC happened to
contain that byte — 6.07% of them (measured 5.99% over 20,000). Those rounds
were rejected with "token invalid or from a previous backend run" and their
signal was lost. Fixed 2026-07-27; tokens minted before that no longer verify,
which is exactly what that message covers.

The region head can bend paper-space Terrain at read time. The utility head
stores only its delta from the Library prior, projects that direction onto
super-region centroids, then mass-centres the projection so weak evidence
cannot wash the whole map green. Confidence grows with answered preferences.
Neither head moves map positions: semantic coordinates describe what papers
mean, while learned offsets describe how you feel about that territory. Tint
strength is bounded, and adjusted terrain remains in the canonical
`[-1,+1]` domain.

### The author head

Regions and authors are fitted by the **same estimator** —
`shrunk_win_rates()`, a James–Stein win rate — differing only in what they
count and how hard they are pulled toward the mean. When a topic head lands it
uses that function too; two hand-rolled copies is how "similar" heads quietly
become different estimators.

The author head trains on **within-region comparisons only**. Inside one
super-region the region head cannot explain the outcome — both papers carry the
same offset — so what remains is the reader's response to the papers
themselves. Across regions topic dominates the choice, and crediting that to
whoever happened to be on the winning paper is how you learn "I love this
author" from "I love this topic". Restricting the sample removes the confound
structurally rather than subtracting an estimate of it. Two further guards: an
author on BOTH papers of a comparison is dropped from it, and an author needs
`AUTHOR_MIN_COMPARISONS` usable comparisons before being published at all, so a
prolific name cannot drift on noise.

It reaches two places, the two the locked geometry answer allows — **ranking**
and a read-time **tint** — and nowhere else.

*Ranking.* It is consumed by one reader: `build_discovery_author_affinity()` in
`application/author_signal.py`, the canonical definition of "how much do I care
about this author". Folding it in there rather than adding a second author term
to the ranker keeps one definition — a parallel `lab_author` signal beside
`author_affinity` would let the same evidence count twice. The offset is ADDED
to the signal your Library already produces; Signal Lab nudges, your Library
decides.

*Tint.* The author map's terrain reads the head through the same
`LabMapContext` the paper terrain uses, behind the same `map_tint_strength`
gate — so answering a round bends both maps or neither. Before this the paper
terrain learned from Signal Lab and the author terrain did not, which made the
same answers visibly move one map and leave the other flat. The tint may CREATE
an opinion where feedback had none (a learned preference for a person is
exactly what the terrain is for), stays inside the canonical `[-1, 1]` domain,
and never moves an author's coordinate or community.

Guarded by `tests/test_geometry_admission_contract.py`, which pins the reader
list: a new consumer has to be a deliberate edit with a reason.

### The venue head

Fitted by the same `shrunk_win_rates()` as regions and authors, from
**matched-pair rounds only** — an ordinary triplet whose papers happen to carry
journals contributes nothing to it, or the head would just be topic preference
wearing a journal's name. A pair whose two papers share a venue also
contributes nothing: nothing differed, so nothing was learned.

It reaches ranking through exactly one door, `journal_affinity` — never a
parallel `lab_venue` signal beside it, which would let the same evidence be
counted twice and drift apart. The fold is the shared
`scoring_terms.fold_lab_offsets()`, so the author and venue heads cannot
disagree about what "disabled" means.

Two shrinkage guards, both continuous rather than a gate: `VENUE_SHRINKAGE`
(8.0 — a venue judged once contributes ~11% of its raw vote, ~71% at twenty)
and `VENUE_MIN_COMPARISONS` (2), because one comparison cannot distinguish a
preference from a misclick.

### Promotion and units

Ranking terms are separately promotion-gated: `weights.lab_region_offset`,
`weights.lab_utility`, `weights.lab_author_offset` and
`weights.lab_venue_offset`, each `0…10` points on the 0-100 score.

The region and utility heads are added to that score directly. The **categorical
heads are not** — they are folded into an affinity map that the scorer then
clamps to `[0, 1]`, so their setting is converted from score points into
affinity units (`CATEGORICAL_HEAD_MAX_AFFINITY`, 0.35 at the maximum setting).
Without that conversion the default of 5.0 made a +0.2 offset land as +1.0 on a
`[0, 1]` map, and the clamp turned every judged entity into a hard 1.0 or 0.0 —
a binary override of the curated signal, in a feature whose contract is
"Signal Lab nudges, your Library decides".

Settings shows the held-out and churn evidence used to decide.

## Disable is not purge

The **Active** switch is reversible. When off:

- Home serves no game and the answer endpoint rejects writes;
- Discovery, Feed, and maps do not load or apply the retained model;
- rounds, fitted heads, metrics, and settings remain untouched.

Re-enabling makes that retained evidence consumable again.

**Purge signals** is separate, destructive, and explicit. It deletes all round
rows and invalidates the model in one write unit. Library, ratings, ordinary
feedback, activation, and knobs remain unchanged.

## API

```text
GET  /api/v1/signal-lab/games
GET  /api/v1/signal-lab/settings
PUT  /api/v1/signal-lab/settings
GET  /api/v1/signal-lab/{game}/queue?count=12
POST /api/v1/signal-lab/{game}/round/answer
GET  /api/v1/signal-lab/summary
GET  /api/v1/signal-lab/model
GET  /api/v1/signal-lab/eval
POST /api/v1/signal-lab/purge
```

The settings model strictly validates activation, map tint, promotion points,
ring decay, exploration, coverage, refit cadence, holdout share, and override
votes.

## Promotion verdict, 2026-07-27: not yet

All three scoring weights (`weights.lab_region_offset`, `weights.lab_utility`,
`weights.lab_author_offset`) are `0.0`, so the shipping build is byte-identical
to one with no Signal Lab at all — `load_lab_scoring_context` early-returns
`None` when both scoring weights are ≤ 0. That is D20 working as designed:
promotion is a deliberate act on held-out evidence, never a side effect of
fitting.

The evidence was evaluated on 2026-07-27 and does not support promotion:

| Check | Measured |
|---|---|
| Held-out preference pairs | **0** |
| Regions ever seen in a round | 11 of 32 |
| Region-boundary edges observed | 4 of 26 |
| Authors the head moves | 0 |
| Candidates entering the top-20 if promoted | 0 |
| Mean rank displacement if promoted | 0.34 |

The first row settles it: 20 rounds produced exactly one holdout round, and it
yielded no usable pairs, so there is nothing to test the head against. The last
two rows say the question is moot today anyway — promoting would reorder
essentially nothing.

**Re-check when** the holdout is non-empty (target ≥ 30 pairs) and
`utility_accuracy` beats `prior_accuracy` by a margin that survives a binomial
test at that sample size. Read it from `GET /signal-lab/eval`. Play more rounds
first; the map is two-thirds unvisited.

## Evaluation evidence

`scripts/simulate_signal_lab.py` drives the shipping full-outcome EIG and
wholesale-fit primitives against stratified-random and margin baselines.
`--corpus` uses a seeded 1,000-paper sample of the real super-region geometry
and one seeded 768→64 Gaussian projection so repeated fits stay bounded;
production always uses the full embeddings.

The checked-in corpus report is `tasks/54_stage0_report.json`. At 200 answered
rounds, EIG reached 0.6525 pairwise accuracy versus 0.6250 for
stratified-random and passed the predeclared +2-point late gate; it did not beat
random in the early checkpoints. That mixed result is kept visible: it
supports posterior-aware acquisition as one component, not EIG-only selection.
The runtime also uses ring/edge goals, answerability, staleness, protected
exploration, cold-start design, and conditional sheet diversity, which this
small utility-only simulator does not model.

Production cost is much smaller than the simulator because a user request does
not replay policies or refit hundreds of histories. On the 2026-07-27
full-vector benchmark (768 dimensions), an exact 2,000-paper corpus with 200
recorded rounds built a 12-round posterior-aware sheet in **1.278 s wall /
3.954 CPU-seconds**. One 200-round wholesale fit took **0.674 s wall /
8.330 CPU-seconds** and runs in the background every five answers. These are
brief multi-core bursts, not a sustained simulation.
