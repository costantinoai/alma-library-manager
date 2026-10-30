---
title: Discovery baseline and relevance protocol
description: Frozen baseline manifest and blinded relevance-pool protocol for comparing Discovery methods.
---

# Discovery baseline and relevance protocol

**Status:** defined 2026-10-30 for gate G0 of task 92. Nothing here has been
run, and no comparison result exists. The evaluation goals come from
[section 12 of the design](discovery-component-architecture.md#12-evaluation-proof-must-match-the-claim).

This protocol applies before any G1 to G6 algorithm comparison. It fixes what
"the baseline" means and how judged relevance is collected, so a result cannot
depend on which data, settings or papers happened to be around.

## What each artifact answers

| Claim | Evidence | Not evidence |
|---|---|---|
| A method acquires more useful papers | Blinded pool judgments, known-relevant recall, unique qualified external yield per source call | Offline AUC, retrieval rank |
| A method orders a fixed set better | Same pool, judged ordering metric | A comparison where the candidate sets differ |
| A method predicts future taste | Explicit future observations, chronological | Pool judgments, which are relevance |
| Discovery helps as a whole | Native pools under a fixed attention budget | A single method's metric |

Pool judgments state **relevance to an intent**. They are not preference labels.
They never enter `feedback_events`, `lens_signals`, a profile or a fit.

## Frozen baseline manifest

A baseline is one JSON file. A run without a complete manifest is not a baseline.
The manifest is written once and never edited. A correction is a new manifest
with a new id that names the one it replaces.

| Group | Field | Source |
|---|---|---|
| Identity | `manifest_id`, `created_at`, `supersedes` | Assigned |
| Code | Source commit, test commit, clean-tree flag | `git rev-parse` in both repositories |
| Versions | `FEATURE_SCHEMA_VERSION`, `RANKER_VERSION`, `POLICY_VERSION` | `features.py`, `ranker.py`, `exploration.py` |
| Database | Private backup path, SHA-256, size, `user_version` | `sqlite3 .backup` of a live database, never a raw file copy |
| Corpus | Standalone paper count, Library count, status counts | SQL on the backup |
| Vectors | Embedding model, row count, coverage among Library papers, latest watermark | `publication_embeddings` |
| Structure | For each lens: seed count, branch ids, state, seed sets, unassigned count | `discovery_branches`, branch plan |
| Frontier | Total, live, oldest and newest expiry, count per source key | `discovery_frontier` |
| Settings | Full `discovery_settings` dump with secret keys removed, plus its hash | SQL, then filter |
| Calibration | Generation and stored payload hash | `scoring:calibration` |
| Controls | Each lens's channel weights and `branch_controls` | `discovery_lenses` |
| Randomness | Slate seed for each refresh, pool shuffle seed | Assigned, recorded before use |
| Access | Network mode, providers, API key present or absent (never its value), rate-limit budget | Runner |
| Intents | The intent file and its hash | Section below |
| Cutoff | Time cutoff `T`: no evidence after `T` may be read | Assigned |

### Preconditions

A manifest is refused when any of these fails. Each is a check that prints the
number it read.

1. `frontier_live > 0` and the newest `last_seen_at` is younger than the expiry
   window. Dev failed this on 2026-10-30 with 0 live of 479 rows, so its external
   lane could not contribute. Dev is not a valid baseline until rebuilt.
2. The refresh on the backup finishes with all four lanes reporting no failure.
3. A stored row replays: sampled `discovery_ranking_candidates` rows reproduce
   `prior_score` exactly from their own weights and calibration. Rows written by
   an older ranker do not replay, so they cannot act as a baseline; see
   [the replay table](discovery-scope-and-learning-audit.md#snapshot-replay-measured-2026-10-30).
4. Legacy Dismiss ratings (D92-13) are repaired or listed. Otherwise a baseline
   inherits negatives that violate D6, and the manifest must say so.
5. Prod data is used only as a private backup. A run never writes to prod, and
   the backup never enters Git.

### Replaying the baseline

Comparison methods run against the same backup, offline. Network acquisition is
the one exception, because a method may be claiming better acquisition. Such a
run records every request, response hash and timestamp, and stores the returned
records beside the run, so a later reader sees the same candidates. Anything
published after `T` is removed from the pool before judging.

## Intents

An intent is a stated information need, written before any method runs.

| Field | Meaning |
|---|---|
| `intent_id` | Stable id |
| `kind` | `lens`, `branch`, `keyword`, `collection_gap`, `method_transfer` |
| `statement` | One or two sentences in the user's words |
| `reference` | Lens or branch and its seed set, if any |
| `eligibility` | What may be returned, in particular Library handling |
| `facets` | Optional: method, problem, population or data, finding |

Sampling is stratified, not by convenience. Every active branch of the chosen
lenses gets an intent, and small branches are not dropped. Keyword intents need
no seed papers (G1). At least a fifth are `method_transfer` or wrong-method
cases, since these are what broad topical similarity misses. A proposed starting
size is 30 intents. That figure is a practical budget, not a power calculation.
The user approves the list before the first pool is built.

## Pool construction

1. For each intent and each method in the manifest, take the top `k` eligible
   results. `k` is 20, the first page. Acquisition comparisons may add a second
   stratum sampled from ranks 21 to 100 at a recorded rate, so that depth can be
   estimated without judging everything.
2. Exclude nothing because a method dislikes it. Apply only the intent's
   eligibility, then record each exclusion and its reason.
3. Merge by canonical paper identity after group resolution, keeping every
   method and rank that found the paper. Canonical merge happens before blinding.
4. Add **sentinels**: about 5% known-relevant papers held out of the reference
   set by time, and about 5% off-topic papers from another branch. Sentinels are
   marked in the sealed key only.
5. Shuffle with the recorded seed. Write the sealed key (paper, intent, method,
   rank, sentinel flag) to one file and the judge's list to another.

A method's contribution to the pool is reported. A method absent when the pool
was built is compared only on the papers it shares with the pool, with its
unjudged share stated, until its results are judged too.

## Blinded judging

**The judge sees** title, abstract, year, and the intent statement.

**The judge does not see** method, rank, score, branch label, retrieval reason,
source, citation count, Library state, rating, or any earlier judgment. Authors
and venue stay hidden by default, since recognising them is a known bias. If a
paper cannot be judged without them, the judge may reveal them, and the reveal
is recorded on that judgment.

Judging happens outside the Discovery page, in a separate list that writes only
its own file. Each session covers one intent, so papers are not compared across
intents, and a session stops at the judge's chosen limit. A partial session is
resumed, not restarted.

| Grade | Meaning |
|---|---|
| 3 | Would save for this intent |
| 2 | Useful for this intent |
| 1 | On topic, not useful |
| 0 | Not relevant |
| `unknown` | Cannot judge from what is shown |

When an intent lists facets, each one may be marked `match`, `partial`,
`mismatch` or `unknown`. A facet left blank stays `unknown`.

Unjudged and `unknown` are never grade 0. A judgment records `intent_id`, paper,
grade, facets, reveal flag, timestamp and session. Judgments are append-only: a
later change of mind is a new row, and the file keeps both.

### Reliability

The judge is one person, so agreement between assessors cannot be measured, and
the results describe that one user's needs. About 10% of papers return in a later
session, at least a day apart, to give test-retest agreement. A session fails its
check if a known-relevant sentinel gets grade 0 or an off-topic sentinel gets
grade 3 more than the agreed tolerance. The tolerance is set before unblinding.
Failed sessions are repeated, not silently dropped.

## Metrics and uncertainty

Decide the primary metric for each claim before unblinding.

| Claim | Primary | Reported with |
|---|---|---|
| Acquisition | Graded recall against the pooled relevant set; known-relevant recall | Unique qualified external yield per source call, cost, failures |
| Ordering | nDCG@20 on a fixed union pool | Judged share of each top `k`, and bpref as a check on incomplete judgments |
| Facet relevance | Per-facet precision on matched and wrong-method cases | Counts for each facet |
| Slate | Coverage of branches and facets at the attention budget | Shortfall against the budget, stated honestly |

Grade 2 and above counts as relevant for binary measures. "Qualified" means grade
2 or above and not already in the Library.

The unit of analysis is the intent. Uncertainty is a paired bootstrap over
intents, or over time blocks for forward data. Papers and derived pairs are not
independent samples, so they never set the sample size. Every report gives the
number of intents, judged papers and unknowns, the interval, and the slices for
no-vector papers and for each facet. A failed or neutral comparison is reported
the same way.

## Rules that protect the result

- Write down the hypothesis, primary metric and decision rule before unblinding.
- Do not tune a method on the intents used to compare it. Split intents into a
  development set and a held-out test set before the first run. The test set is
  opened once for each comparison.
- Do not unseal the key until judging for that pool is complete.
- Do not treat retrieval as relevance, or relevance as taste.
- Do not fill an unjudged paper with 0, or an absent value with a default score.
- Do not compare across different manifests. A changed baseline needs a new pool
  or an explicit statement of what carried over.
- Do not reuse judgments silently. Reused judgments keep their original session
  and intent, and they are valid only for the same intent statement.
- Do not store judgments, intents or backups in a shipped path. Personal data
  stays in a private directory, gitignored.

## Files one comparison produces

| File | Content |
|---|---|
| `manifest.json` | The frozen manifest |
| `intents.json` | The approved intents, split into development and test |
| `plan.md` | Hypothesis, primary metric, decision rule, tolerance |
| `pool.sealed.json` | Sealed key |
| `pool.judge.json` | Blinded list |
| `judgments.jsonl` | Append-only judgments |
| `report.md` | Numbers, intervals, limits and what was decided |

## Decisions needed before a first run

These are proposals. The user decides them, and the choice goes in `plan.md`.

1. Which data backs the baseline. Prod has live frontier and more feedback, but a
   private backup of it holds real reading history.
2. Whether D92-13 is repaired first.
3. The intent count and the development and test split.
4. Whether authors and venue stay hidden by default.
5. The sentinel tolerance and the second-stratum sampling rate.
