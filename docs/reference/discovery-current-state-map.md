---
title: Discovery current-state map and ground-up plan
description: As-inspected data flow, measurements, boundaries, controls, gaps, and phased redesign for task 92.
---

# Discovery: current system and ground-up plan

**As inspected:** working tree on `feature/discovery-lenses-branches`, 2026-09-24.
This is a code map, not a claim that the proposed architecture is implemented.
The [earlier audit](discovery-scope-and-learning-audit.md) contains historical
defects and read-only database evidence; some source defects in that ledger have
since been edited in this working tree and remain unverified. The
[design and method decisions](discovery-component-architecture.md) is the
authoritative target, revised after this map. Its decisions supersede the
initial target formerly appended to this map, particularly generic quality
scores, mandatory contrast pairs, single-centroid interests and learned
acquisition allocation.
The as-inspected source map and historical counts remain unchanged.

## What the product must decide

For each lens, Discovery should answer four separate questions: **where can we
find plausible papers not yet in the Library; which structural region does each
paper belong near; is this paper useful to this user in that region; which
qualified papers should we show now?** An action then answers a fifth question:
**what explicit evidence did the user give about that exposed paper, and at what
scope?** These questions need different data, metrics, and owners. A good
ranker cannot repair a missing acquisition pool; a Like cannot safely label
every author or term on its paper.

Feed owns monitored arrivals. Library owns saved membership, collections,
ratings and reading state. Authors owns canonical identity and follows.
Discovery owns lenses, structural branches, external acquisition plans,
candidate retrieval, context-specific scores, suggestions, exposure, and
branch-local learning. The shared corpus owns paper identity, metadata,
embeddings and citation edges. Dismiss is lens visibility only; Dislike is a
negative preference; Read is workflow. The corpus partition is structural and
does not move after a preference action (D6, D24, D26, D27).

## Actual data flow

```mermaid
flowchart TD
  X[Library papers / collections / tags<br/>corpus metadata / vectors / citations] --> L[Lens scope SQL<br/>every member]
  L --> B[UMAP/HDBSCAN or lexical branch builder<br/>up to 6 displayed branches]
  B --> BC[(discovery_branches<br/>written on successful refresh)]
  X --> G[Periodic graph/frontier maintenance<br/>global Library seeds and top 6 topics]
  G --> F[(discovery_frontier + edges + vectors<br/>30-day expiry)]
  L --> R[Refresh job: profile prerequisite]
  B --> R
  F --> R
  R --> RL[Lexical: lens-wide]
  R --> RV[Vector: branch centroids]
  R --> RG[Graph: lens seeds + global PPR]
  R --> RE[External: local frontier<br/>first 4 active branches + taste queries]
  RL --> M[Canonical-key merge + family RRF]
  RV --> M
  RG --> M
  RE --> M
  M --> A[One branch attribution<br/>ambiguous/lens-wide may remain null]
  A --> S[10-family scorer + feature snapshot]
  S --> C[Top 200 staged; eligibility; MMR<br/>branch floor; up to 4 random exploration slots]
  C --> D[(suggestion_sets + recommendations<br/>ranking_candidates)]
  D --> U[Discovery page / card / explanation]
  U --> I[(Visible-card impressions)]
  U --> P[POST /papers/id/action]
  P --> E[(feedback_events + recommendation action<br/>Library state)]
  I --> V[Outcome evaluator / shadow fit]
  E --> V
  E --> GP[Global preference/projection/profile paths]
  GP --> R
  GP --> S
```

The ordinary lens refresh performs **local reads**, not live source search.
Source acquisition is the separate scheduled/manual maintenance job. Four
retrieval lanes run concurrently on separate SQLite read connections with a
configurable 60-second deadline. The refresh then does CPU work, two bounded
writer-gated phases (candidate materialization, deck swap), and schedules
hydration/outcome evaluation. One timeout yields a thinner deck and a recorded
lane failure. The background frontier sequence is citation coupling, external
source queries/S2 recommendations, then vector filling, committing between
network phases. Periodic lens refresh is opt-in; graph/frontier maintenance
defaults to a 24-hour interval; scoring calibration defaults to 12 hours.

### Other Discovery entry points and duplicate logic

The lens refresh is the active recommendation pipeline, but the API exposes
other Discovery paths. They matter because they can bypass its scope and
measurement model:

| Entry point | Actual path | Relationship to the lens lifecycle |
|---|---|---|
| `POST /discovery/similar` | `routes/discovery.py` → `discovery/engine.py::discover_similar_with_meta`; OpenAlex related/citing and S2 per-paper recommendations, then a local dense fallback if network yields nothing; cached by selected seed IDs. | Used from Discovery and Library. It can fetch live external papers, but has no lens/branch context, deck snapshot or exposed branch-local outcome. Treat as a distinct similarity tool or route its leads into the shared acquisition/evidence contract. |
| `POST /discovery/manual-search` and `/manual-search/add` | Background `openalex_manual.search_online_sources` with cross-source query fusion; selected result is added to Library. | Explicit user search, not automatic branch discovery. Frontend API functions exist but no current frontend caller was found. Reuse canonical identity, metadata and action boundaries; do not quietly train a branch from an unscoped manual addition. |
| `GET /discovery/recommendations`, `/stats`, `/recommendations/{id}/explain`, `/{id}/seen`, and `DELETE /recommendations` | Generic recommendation read/management beside `/lenses/{id}/recommendations`. | Audit which consumer still needs the generic list/clear/seen semantics; the lens route is the Discovery page's deck read. One deck policy and one explanation source should serve both, avoiding divergent visibility or score rules. |
| `discovery/engine.py::generate_recommendations` and `refresh_recommendations` | Legacy global generator; the legacy refresh deletes neutral recommendations and writes through its own connection. No production caller under `src/alma` was found by reference search. | Candidate for removal after caller/compatibility review. It is a separate writer/scorer path and conflicts with the intended single orchestration owner. Confirm external/scripts references before deleting. |
| `GET/PUT /discovery/settings`, branch preview, frontier rebuild, calibration/outcome and channel-yield diagnostics | Settings write through one route; preview and diagnostic reads have their own cached/background jobs. | Maintain a control-to-effect registry. A slider should identify the stage it changes and show the measured result; a diagnostic fit must never silently become an online branch weight. |

The currently exposed API still declares a global channel-yield adaptation
setting, while lens refresh deliberately leaves its multiplier empty to avoid
cross-branch learning. That is a truthful-UI/unused-setting cleanup candidate.
Likewise, manual search, generic recommendations and similarity need an
explicit product decision: a separate tool may remain separate, but any
candidate or verdict later promoted into Discovery must pass through its
canonical identity, provenance and scope contracts.

### Stage ledger: owner, actual inputs, output, boundary

| Stage | Current code and durable state | What happens now | Missing contract or measurement |
|---|---|---|---|
| Lens and controls | `routes/lenses.py`, `lens_crud.py`; `discovery_lenses` | Global Library, collection, text keyword, or tag context; channel weights and branch controls live on lens row. | Distinguish structural membership from liked examples and candidate eligibility; revision each. |
| Seed selection | `seed_profile._load_seed_papers_for_lens` | Every member, no cap (2026-10-30). Global/collection order by rating then citations; keyword/tag by citations. Text keyword scans title/abstract across the corpus, minus dismissed and removed papers. Zero seeds ends refresh. | Intentional cold-start acquisition. See [every Library paper counts](discovery-scope-and-learning-audit.md#every-library-paper-counts-2026-10-30). |
| Structure | `seed_profile.resolve_lens_branches`, `branch_identity.py`, `discovery_branches` | A refresh keeps the stored membership: a new seed joins the nearest stored branch, a seed with no vector joins by shared terms, a removed paper leaves. Only a rebuild clusters (reasons in `branch_identity.rebuild_reason`): vector-capable seeds are clustered with a lexical fallback, max six, and old identities are mapped with `successor_ids` and carried controls. Controls and the outcome lifecycle then apply. Preview runs the same plan in a separate cached job. | Complete scope, embedding coverage; per-revision history beyond the lens's current `branch_structure`; preview and refresh parity on the same revision. |
| Acquisition | `frontier.py`, `source_search.py`, scheduler; `discovery_frontier`, `_edges` | Six global Library topic queries × 40 results, fixed six-year query horizon; all saved papers as S2 seeds up to the API's 100 per list; six per-paper S2 seeds, four citing seeds; coupling 1,500 per pass. Each of these is a rotating window over the whole pool (`core/rotation.py`). Providers include OpenAlex, S2, Crossref, arXiv, bioRxiv, Europe PMC. Frontier entries expire at 30 days. | Per-branch query plan, source/page/seed provenance, coverage, exhaustion and budget. A branch query cannot find a paper absent from the prefetched frontier. Dismiss is still included in S2 negative seeds, violating D6. |
| Retrieval | `retrieval/{lexical,vector,graph,external}.py` | Lexical takes at most eight lens-wide query terms. Vector searches up to six resolved branch centroids. Graph traverses lens seed references/coupling/co-citation but PPR starts from global Library/Loved. External searches local frontier for first four active branches and lens/global taste variants. | Every active branch needs its own candidate allocation, graph/lexical context and per-lane recall. A bounded search budget should be explicit, not a hidden first-four branch cutoff. |
| Merge and identity | `retrieval/merge.py`, `branch_attribution.py` | Canonical-key dedup retains retrieval hits; family RRF then per-lens channel weights. A dominant branch may be assigned from hit evidence; overlap or no branch hit stays unattributed. | Canonical paper ID before selection, all candidate–branch contexts, attribution confidence, duplicate/identity-loss counts. |
| Features | `scoring_loop.py`, `features.py`; `discovery_ranking_candidates` | Candidate measurement, reward vs exposure features, availability flags, hit provenance, policy and schema versions are stored for staged candidates. | One versioned feature builder for retrieve/rank/reason/learn; stable author, venue, topic, phrase, method and embedding identities; no training on absent values. |
| Score | `ranker.py`, `discovery/scoring.py`, `signal_projection.py` | Ten related families plus a global profile/structured projection; missing families use a calibrated prior. Per-lens channel weights precede scoring. Shadow fit and signal tuning are diagnostic, not an active branch-local learner. | Separate branch relevance, paper quality and local taste; report incremental contribution and uncertainty; stop global learned effects crossing branches. |
| Selection | `__init__._refresh_lens_recommendations`, `exploration.py` | Full merged pool is scored, then the top `max(200, 4 × limit)` staged, which holds all four channels. Library/action/reading/min-score exclusions follow. MMR applies author/topic/venue caps, each active branch keeps two cards (a floor card may pass a cap), and up to four uniformly sampled tail cards are reserved in the requested deck, normally 50. `branch_coverage` in the summary states how far each branch got and why one has no card. | Eligibility before staging; per-branch paginated streams plus overview; qualified reach, novelty and propensity by branch. The 50-card deck must not define discoverable breadth. |
| Persist and hydrate | `__init__._materialize_slate/_persist_refresh`; `papers`, `suggestion_sets`, `recommendations`, candidate snapshots | Selected papers are promoted to tracked corpus and hydration queued; unacted old recs are swapped for a new deck; actioned rows remain. Branch identities are persisted with the deck. | Pre-serialize ranking rows outside the writer gate; bounded batches; atomic version/revision check so a stale job cannot publish over newer lens state. |
| Exposure and action | `DiscoveryPage.tsx`, `useDiscoveryImpressions`, `routes/publications.py`, `paper_actions.py`; `discovery_impressions`, `feedback_events` | A visible card records an impression. Canonical action updates paper/lens state; Save/Like/Love/Dislike give preference; Dismiss only hides; Read queues. Recommendation/action and event linkage is still uneven. | Freeze displayed branch/context and aspect options; return event ID; one idempotent action trail; distinguish viewed, skipped, saved and explicitly disliked. |
| Learning and audit | `observations.py`, `feedback_substrate.py`, `outcome_eval.py`, `channel_yield.py` | Outcome loader requires a selected v3 snapshot, visible impression, linked later event and known propensity; randomized-only by default. Other profile/projection paths are global. | Branch-local fit from attributable exposed events; honest negative construction, chronological replay, component ablations, and unsupported-value neutral fallback. |

### The measures and their actual role

| Measure | Produced from | Used for / limitation |
|---|---|
| Seed count, rating, citations, topics, embedding availability | Lens SQL + corpus | Branch inputs and priors. Membership is not a preference label. |
| Cluster membership, centroid, core/explore topics, branch score, outcome share | Seed vectors/text and 60-day branch action aggregation | Preview, vector search, frontier query strings and automatic controls. Structure and preference-driven allocation are partly mixed; small outcome samples can auto-mute. |
| Source/query/graph hits, rank, RRF, channel weights | Four retrieval lanes | Candidate inclusion and fused retrieval score. Multiple hits may be correlated; a hit is evidence of reach/relevance, not user taste. |
| Semantic/lexical similarity, topic, author, venue, recency, citation, feedback, preference | Candidate scorer and profile | Ten-family score. The current global learned profile is not branch-independent. Exact-value availability does not imply a learned coefficient. |
| Prior score, shadow score, reward/exposure features, propensity | Candidate snapshot and slate policy | Explanation and retrospective evaluation. Only recorded, exposed decisions can supply a causal outcome. |
| Impression, action, optional reason | Visible card and canonical action | Presently only impression + action exists. A paper verdict alone confounds all its authors/topics/phrases; optional aspect selection is proposed. |
| Qualified unseen yield, coverage, latency, source cost | **Not consistently recorded end-to-end** | These are required measures for deciding whether Discovery actually finds valuable external papers. |

The default score weights are retrieval **0.14**, semantic/lexical together
**0.51** (87% semantic), citation **0.12**, feedback **0.09**,
preference **0.09**, venue **0.05**, and topic/author/recency **0**.
The weights were fitted to separate historical Library papers from rejected
and random corpus papers, not to predict future branch-specific usefulness.
The `recommendation_mode` changes some family weights, while branch temperature
changes query composition and a few discrete choices (including nearest vs
farthest cluster at 0.5). Neither controls the slate's fixed four exploration
slots. Thus neither knob currently means a measured discovery distance or
exploration share. Per-lens channel weights, global score-family weights,
retrieval RRF, bounded citation bonuses and diversity selection all affect a
paper at different stages; their individual effects need separate ablations.

## Concrete failure chain

Suppose a user makes a methods lens from a large collection and wants a novel
paper at its edge. Since 2026-10-30 every member defines branches, so the
method is present. The maintenance job searches six global Library topics per
pass, three of them rotating, so that method waits for its turn. The refresh's
branch query only searches the already built frontier. Lexical and graph lanes
search lens-wide; vector can retrieve a local semantic neighbour of an included
branch; external searches only four branches. Even if the desired paper is in
the merged pool, scoring and top-200 staging can discard it before eligibility
and branch coverage are considered. The user sees at most 50 cards. A later
Like influences global projection/profile paths and does not by itself tell us
which of that paper's exact values the user liked. This is why tuning weights
alone cannot solve the discovery objective.

The measured copies reinforce the limit: production had 4,175 papers, one
lens and 50 recommendations, only four with branch IDs; dev had 12,148 papers,
two lenses and 264 recommendations, only ten with branch IDs. Neither copy
contains an exposed positive/negative pair in one branch. The dev copy has
5,289 candidate snapshots, but those are mainly feature coverage, not
branch-learning labels. See the [evidence audit](discovery-scope-and-learning-audit.md)
for exact counts and caveats. No claim of branch-local model accuracy follows
from these data.

## Public inventory (2026-10-30)

Extracted from source on `feature/discovery-g0`. Paths are relative to
`src/alma/` unless they start with `frontend/`. This supersedes the descriptive
tables above wherever they differ.

### Routes

Prefix `/api/v1/discovery`, file `api/routes/discovery.py`:

| Line | Method and path | Handler | Note |
|---:|---|---|---|
| 212 | GET `/status` | `get_discovery_status` | Read. |
| 233 | GET `/settings` | `get_discovery_settings` | Read. |
| 248 | GET `/outcome-evaluation` | `get_outcome_evaluation` | Read of a stored view. |
| 262 | GET `/channel-yield` | `get_channel_yield` | Diagnostic; no frontend caller of `getChannelYield`. |
| 281 | POST `/outcome-evaluation/refresh` | `refresh_outcome_evaluation` | Queues the view. |
| 298 | PUT `/settings` | `update_discovery_settings` | One writer of the KV settings. |
| 415 | POST `/settings/reset` | `reset_discovery_settings` | Writer. |
| 525 | POST `/similar` | `discover_similar` | Live network, no lens or branch context, no exposure row. |
| 670 | POST `/similarity-cache/clear` | `clear_similarity_cache` | Writer. |
| 721 | POST `/manual-search` | `manual_discovery_search` | Background network job. No frontend caller of `manualDiscoverySearch`. |
| 806 | POST `/manual-search/add` | `manual_discovery_add` | Writer. No frontend caller of `manualDiscoveryAdd`. |
| 863 | POST `/impressions` | `record_discovery_impressions` | Visible cards; all or nothing per batch. |
| 890 | GET `/recommendations` | `list_recommendations` | Generic list beside the lens route. |
| 916 | POST `/recommendations/{rec_id}/seen` | `mark_seen` | Stamps `seen`; never preference. |
| 951 | GET `/recommendations/{rec_id}/explain` | `explain_recommendation` | Read. |
| 998 | DELETE `/recommendations` | `clear_recommendations` | Deletes every row (`application/discovery/lens_crud.py:657`). |
| 1031 | POST `/frontier/rebuild` | `rebuild_frontier` | The manual acquisition trigger. |
| 1095 | GET `/stats` | `discovery_stats` | Read. |

Prefix `/api/v1/lenses`, file `api/routes/lenses.py`:

| Line | Method and path | Handler |
|---:|---|---|
| 34 | GET `` | `list_lenses` |
| 47 | POST `` | `create_lens` |
| 90 | PUT `/order` | `reorder_lenses` |
| 104 | GET `/{lens_id}` | `get_lens` |
| 120 | POST `/{lens_id}/seen` | `mark_lens_seen` |
| 146 | PUT `/{lens_id}` | `update_lens` |
| 189 | DELETE `/{lens_id}` | `delete_lens` |
| 220 | GET `/{lens_id}/signals` | `list_lens_signals` |
| 238 | GET `/{lens_id}/recommendations` | `list_lens_recommendations` |
| 263 | GET `/{lens_id}/branches` | `preview_lens_branches` |
| 289 | POST `/{lens_id}/branches/refresh` | `refresh_lens_branches` |
| 310 | POST `/{lens_id}/refresh` | `refresh_lens` |

Other routes that act on Discovery state:

| Location | Method and path | Note |
|---|---|---|
| `api/routes/publications.py:755` | POST `/api/v1/papers/{paper_id}/action` | The one action route. |
| `api/routes/publications.py:798` | PUT `/api/v1/papers/{paper_id}/rate` | Library papers; event has no recommendation context. |
| `api/routes/publications.py:840` | POST `/api/v1/papers/{paper_id}/undo-feedback` | Per-aspect undo; deletes events. |
| `api/routes/insights.py:1792` | GET `/api/v1/insights/recommendations` | Engagement read. |
| `api/routes/insights.py:2648` | POST `/api/v1/insights/discovery/branch-action` | No frontend caller found. |
| `api/routes/feedback.py:52` | POST `/api/v1/feedback/track` | Direct `feedback_events` writer. |
| `api/routes/feedback.py:104` | POST `/api/v1/feedback/reset` | Clears learning tables and `user_action`. |

### Tables and writers

| Table | DDL | Writers |
|---|---|---|
| `discovery_lenses` | `api/deps.py:732` | `application/discovery/lens_crud.py` (create, update, order, delete); `application/discovery/__init__.py` (`last_refreshed_at`) |
| `discovery_branches`, `discovery_lenses.branch_structure` | `api/deps.py:750`, `core/migrations.py:1609`, `core/migrations.py:1742` | `application/discovery/branch_identity.py` only |
| `suggestion_sets` | `api/deps.py:767` | `application/discovery/__init__.py` only |
| `recommendations` | `api/deps.py:786` | `application/discovery/__init__.py`, `application/discovery/lens_crud.py`, `application/library.py`, `services/feedback_substrate.py`, `api/routes/feedback.py`, `core/migrations.py`, and the legacy `discovery/engine.py` |
| `discovery_ranking_candidates` | `api/deps.py:826` | `application/discovery/observations.py` only |
| `discovery_impressions` | `api/deps.py:858` | `application/discovery/observations.py` only |
| `discovery_frontier` | `api/deps.py:879` | `application/discovery/frontier.py`, `core/migrations.py` |
| `discovery_frontier_edges` | `api/deps.py:909` | `application/discovery/frontier.py` only |
| `lens_signals` | `api/deps.py:927` | `application/discovery/lens_crud.py`, `services/feedback_substrate.py`, `application/library.py` (undo). Read only for a count and a list; no scorer reads it. |
| `feedback_events` | `api/deps.py:1318` | `services/feedback_substrate.py` (insert), `api/routes/feedback.py` (insert), `application/library.py` (undo deletes), `core/paper_groups.py` (merge), `api/routes/extension.py`, `core/migrations.py` |
| `preference_profiles` | `api/deps.py:1336` | `services/feedback_substrate.py`, `core/paper_groups.py` |
| `discovery_settings` | `api/deps.py:1389` | `application/discovery/lens_crud.py`, plus unrelated keys from `application/feed.py`, `application/walkthroughs.py`, `services/maintenance.py`, `core/secrets.py`, `api/routes/ai.py`, `core/migrations.py`, `api/deps.py` |
| `similarity_cache` | `api/deps.py:1479` | `api/routes/discovery.py`, `core/paper_groups.py` |

### Jobs

| Job or operation key | Source | Trigger |
|---|---|---|
| `lens_refresh_<id>`, `discovery.lens.refresh:<lens>` | `api/routes/lenses.py:358`, `api/routes/lenses.py:458` | User, or the opt-in periodic job. |
| `discovery.lens.refresh.lane.<lane>` | `application/discovery/__init__.py:446` | Inside a refresh. |
| `refresh_recommendations` | `api/scheduler.py:1683` | Interval, off by default (`schedule.refresh_enabled`). |
| `maintain_citation_graph` | `api/scheduler.py:1745`, `api/scheduler.py:2433` | Interval 24 h; refused unless `core/network_policy.py:59` admits it, which by default means prod only. |
| `scoring_calibration_refresh` | `api/scheduler.py:1727` | Interval. |
| Frontier rebuild | `api/routes/discovery.py:1086` | User. |
| Similar search, manual search | `api/routes/discovery.py:660`, `api/routes/discovery.py:797` | User. |
| `materialize.scoring.calibration` | `application/discovery/calibration.py:326` | View refresh. |
| `materialize.scoring.outcome_eval` | `application/discovery/outcome_eval.py:471` | View refresh. |
| `materialize.discovery.channel_yield` | `application/discovery/channel_yield.py:118` | View refresh; diagnostic. |
| `materialize.variant:discovery:branches:...` | `application/discovery/branch_preview.py:62` | Branch preview. |

### Cache keys

| Key | Source | Invalidated by |
|---|---|---|
| `scoring:calibration` | `application/discovery/calibration.py:53` | View fingerprint. |
| `scoring:outcome_eval` | `application/discovery/outcome_eval.py:446` | View fingerprint. |
| `discovery:channel_yield` | `application/discovery/channel_yield.py:39` | View fingerprint. |
| `discovery:branches:<lens>:v=<digest>` | `application/discovery/branch_preview.py:29` | Digest of version, options, controls, context and last set id, reused for 60 s. Seed and action revisions are absent (D92-05). |
| `positive_centroid`, `negative_centroid`, `positive_exemplars`, `negative_exemplars` in `scoring_cache` | `application/discovery/__init__.py:750` to `application/discovery/__init__.py:813` | Library fingerprint. |
| `similarity_cache` by seed ids | `api/routes/discovery.py:556` | `cache.similarity_ttl_hours`, manual clear, group merge. |
| Topic embeddings in process memory | `application/discovery/__init__.py:1085` | Process lifetime. |

### Public controls

Every key below is written by `PUT /discovery/settings` unless marked. "Lens
refresh" means the key changes the active pipeline.

| Control | Consumer in source | Effect on a lens refresh |
|---|---|---|
| Lens context, name, active flag | `application/discovery/lens_crud.py` | Yes |
| Lens channel weights | `frontend/src/components/discovery/LensWeightsPanel.tsx` | Yes |
| Branch temperature, resolution, pinned, boosted, muted (lens JSON) | `frontend/src/components/discovery/BranchExplorerPanel.tsx` | Yes |
| `branches.temperature` | `application/discovery/seed_profile.py:821` | Yes |
| `branches.max_clusters` | `application/discovery/__init__.py:357` | Yes, capped at 6 in code |
| `branches.max_active_for_retrieval`, `query_core_variants`, `query_explore_variants` | `application/discovery/retrieval/external.py:85` | Yes, external lane only |
| `recommendation_mode` | `application/discovery/ranker.py:645`, `application/discovery/seed_profile.py:814` | Yes |
| `weights.*` | `application/discovery/ranker.py:626` | Yes, global (D92-01) |
| `limits.min_score` | `application/discovery/__init__.py:1466` | Yes |
| `limits.lane_deadline_seconds` | `application/discovery/__init__.py:570` | Yes; absent from the settings route |
| `limits.recency_window_years` | `discovery/scoring.py:605` | Yes |
| `schedule.refresh_enabled`, `refresh_interval_hours`, `graph_maintenance_interval_hours` | `api/scheduler.py:1680`, `api/scheduler.py:1687`, `api/scheduler.py:1740` | Scheduling only |
| `cache.similarity_ttl_hours` | `api/routes/discovery.py:502` | Similar search only |
| `strategies.related_works`, `topic_search`, `followed_authors`, `coauthor_network`, `citation_chain`, `semantic_scholar`; `limits.max_results`, `limits.max_candidates_per_strategy` | `discovery/engine.py:654` to `discovery/engine.py:659`, `discovery/engine.py:649`, `discovery/engine.py:652` | **None.** Read only by the legacy generator |
| `strategies.branch_explorer`, `taste_topics`, `taste_authors`, `taste_venues`, `recent_wins` | Settings route only | **None** |
| `strategies.adaptive_channels` | `application/discovery/channel_yield.py:42` | **None**; `channel_multipliers` has no caller in the refresh |
| `limits.feedback_decay_days_full`, `limits.feedback_decay_days_half` | Settings route only | **None** |
| `strategies.s2_recommend`, `citation_fabric.*`, `limits.taste_*_queries`, `limits.recent_win_queries` | `discovery/defaults.py` only | **None**; defaults with no reader |

### Entry points outside the lens pipeline

| Entry | Writers it reaches | Consequence for measurement |
|---|---|---|
| Similar | `similarity_cache`; papers on a later action | No snapshot or impression; an action from it is `online_search` or `papers`. |
| Manual search | `application/openalex_manual.py` | Same; Dislike there also hides the paper globally (`application/openalex_manual.py:1212`). |
| Legacy generator | `discovery/engine.py:535`, `discovery/engine.py:1475` | Separate writer; reference search found no production caller. |

## Design and execution ownership

The target model and implementation sequence are maintained in
[Discovery design and method decisions](discovery-component-architecture.md).
That specification supersedes the initial target previously appended to this
code map. It defines reference scope versus candidate eligibility, multi-aspect
representations, structural acquisition, exact local learning, ordinal/aspect
supervision, slate policy, uncertainty, evaluation and runtime contracts.

The consolidated local task 92 owns gates G0–G6 and their completion evidence.
The [compute audit](discovery-compute-and-pi-audit.md) tracks proposed and active
paths for deferred task 94. Keeping this page focused on observed code prevents
an intended algorithm from being mistaken for an implemented capability.
