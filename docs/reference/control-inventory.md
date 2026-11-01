---
title: Control inventory
description: Every setting, environment variable, switch, schedule and tunable constant in ALMa, what reads it, what it changes, and how to calibrate it.
---

# Control inventory

This is the internal list of everything in ALMa that changes behaviour and that someone could tune: stored settings, per-lens controls, environment variables, plugin switches, job cadence, tunable constants and persisted frontend preferences. Hiding a control from the interface does not remove it from this page, so nothing becomes a setting nobody knows how to set.

The list is checked against the code. `tests/test_core_control_inventory_contract.py` parses the real sources and fails when a discovery setting, an app setting, an environment variable, a scheduled job, a maintenance task or a listed constant has no row, when a row names something that no longer exists, or when a default in a row no longer matches the code.

[Configuration](configuration.md) says how to set things. This page says what each one does.

## How to read a row

| Column | Meaning |
|---|---|
| Control | The key, field, variable or symbol. `<name>` marks a part filled in at runtime. |
| Defined in | The file that owns it. Symbols, not line numbers, because lines move. |
| Default | The value in the code. |
| Read by | The function that consumes it. |
| Controls | What it changes, and what you would see change. |
| Calibrate | What tells you it is wrong, and which way to move it. "no measure" means nothing in ALMa measures it. |
| Exposed | Where a person can reach it today: a Settings card, a page, env, API only (a route accepts it, no screen does), or nowhere (only a direct database or file edit reaches it). |
| Status | `live`: read and acted on. `dynamic`: live, read through a built key, so a text search for the full key finds no reader. `no-op`: nothing acts on it, even where a function reads it. |

## Measures used in the Calibrate column

| Measure | Where to read it | What it says |
|---|---|---|
| Outcome evaluation | The line under Signal Weights in Settings, `GET /api/v1/discovery/outcome-evaluation`; full payload in the `materialized_views` row `scoring:outcome_eval` | AUC of kept papers against rejected papers and against random corpus papers, per family `alone` and `loss_when_removed`, plus `fit`, the weights that would have done best out of sample. Retrieval and the citation-graph atoms cannot be measured offline. `feedback` and `preference` are leak-prone. |
| Signal impact report | `GET /api/v1/reports/signal-impact`, Insights | Marginal signal effect by outcome, and `tuning`: partial coefficients, their drift. It states its own sample size. |
| Refresh summary | `retrieval_summary` in `suggestion_sets`, and `last_retrieval_summary` on a lens | Per-channel counts, `lane_runs`, timings, `candidate_pool`, and `diversity`: `exploit_count`, `exploration_count`, `branch_floor_count`, `first_screen_lifted`, `first_screen_short`, `safety_caps`, `safety_filtered_count`. |
| Ranking snapshot | Table `discovery_ranking_candidates` | Every candidate of a refresh with its reward and exposure features, and why it was or was not shown. `scripts/discovery_evidence_audit.py` aggregates it from an offline copy without exposing records. |
| Branch panel | Discovery page, branch panel, `GET /api/v1/lenses/{id}/branches` | Per-branch outcomes, `auto_weight`, `auto_weight_reason`, coverage, and the drift numbers `turnover` and `misfit_share`. |
| Feed refresh result | Activity entry of a Feed refresh; `last_result_json` on a monitor | `raw_candidates`, `accepted_candidates`, `rejected_recency`, `rejected_match`, `drop_runs`. |
| Suggestion rows | `GET /api/v1/authors/suggestions` | Bucket, score and `bucket_calibration_multiplier` per suggested author. |
| Quota readout | `GET /api/v1/settings/openalex/usage`; `openalex_credits_remaining` in the Health operations payload | OpenAlex credits used, remaining, limit, retries and rate-limited events. |
| Job result | Activity, `GET /api/v1/activity` | Duration, counts and logged skip reasons of a background job. |
| Health page | `GET /api/v1/health/operations`, Health | Pending counts, ETA, daily usage and last outcome of each repair. |

## Which constants are listed

Constants appear here when changing one would change ranking, retrieval, rotation, the slate, learning, a budget, a retry or rate limit, a threshold a person sees the effect of, or how long something is kept. These are left out: timeouts on a single call, byte and length caps that exist for safety, write batch sizes and SQL chunk sizes, thread-pool widths, version stamps, display truncation and log thresholds. Literal numbers inside a function are listed under the function name when they meet the same rule.

## Discovery settings

Stored as text in the SQLite table `discovery_settings`, seeded from `DISCOVERY_SETTINGS_DEFAULTS` in `src/alma/discovery/defaults.py`. `PUT /api/v1/discovery/settings` writes the sections it models; `POST /api/v1/discovery/settings/reset` writes every default back.

### Ranking weights

<!-- inventory: discovery -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `weights.source_relevance` | `src/alma/discovery/defaults.py` | `0.14` | `ranker.resolve_family_weights` | Weight of the Retrieval family: how highly the channels ranked a paper and how many agreed. Every score on every surface moves. | Cannot be measured offline. Signal impact report; raise if papers found by several channels are kept more. | Settings: Discovery Settings, Signal Weights | live |
| `weights.topic_score` | `src/alma/discovery/defaults.py` | `0.0` | `ranker.resolve_family_weights` | Weight of the Topic family. Fitted to zero, so topic overlap moves no score until raised. | Outcome evaluation: raise only if `fit` gives it weight or `loss_when_removed` is positive. | Settings: Discovery Settings, Signal Weights | live |
| `weights.text_similarity` | `src/alma/discovery/defaults.py` | `0.51` | `ranker.resolve_family_weights` | One slider for two families: Semantic takes 87%, Lexical 13%. The largest driver of every score. | Outcome evaluation: `semantic` and `lexical` `alone` and `loss_when_removed`; follow `fit`. | Settings: Discovery Settings, Signal Weights | live |
| `weights.author_affinity` | `src/alma/discovery/defaults.py` | `0.0` | `ranker.resolve_family_weights` | Weight of the Author family. Fitted to zero because the corpus is mostly followed authors. Also the reason Explore and Exploit do nothing to authors at the defaults. | Outcome evaluation; the `author_zero` what-if sizes it. | Settings: Discovery Settings, Signal Weights | live |
| `weights.journal_affinity` | `src/alma/discovery/defaults.py` | `0.05` | `ranker.resolve_family_weights` | Weight of the Venue family: journals you read lift a paper. | Outcome evaluation: `venue`; lower if `loss_when_removed` is at or below 0. | Settings: Discovery Settings, Signal Weights | live |
| `weights.recency_boost` | `src/alma/discovery/defaults.py` | `0.0` | `ranker.resolve_family_weights` | Weight of the Recency family. Fitted to zero, so publication year moves no score until raised. | Outcome evaluation: it pointed the wrong way against rejected papers at the last fit. | Settings: Discovery Settings, Signal Weights | live |
| `weights.citation_quality` | `src/alma/discovery/defaults.py` | `0.12` | `ranker.resolve_family_weights` | Weight of the Citation family: citation count, field impact, shared references, co-citation and graph proximity. | Outcome evaluation for the count atoms; the graph atoms cannot be measured offline. | Settings: Discovery Settings, Signal Weights | live |
| `weights.feedback_adj` | `src/alma/discovery/defaults.py` | `0.09` | `ranker.resolve_family_weights` | Weight of the Feedback family: your verdicts on similar papers. | Leak-prone in the outcome evaluation, compare the `content_only` score. Signal impact report. | Settings: Discovery Settings, Signal Weights | live |
| `weights.preference_affinity` | `src/alma/discovery/defaults.py` | `0.09` | `ranker.resolve_family_weights` | Weight of the Preference family: the taste profile from ratings and history. | Leak-prone in the outcome evaluation. Signal impact report. | Settings: Discovery Settings, Signal Weights | live |
| `recommendation_mode` | `src/alma/discovery/defaults.py` | `balanced` | `ranker.resolve_family_weights` through `_MODE_MULTIPLIERS` | Explore or Exploit multiplies some family weights. At the fitted defaults author and recency are 0, so Explore only halves citation and venue, and Exploit only raises venue and preference and halves nothing. It does not change branch temperature, whatever the card says: see `branches.temperature`. | Outcome evaluation after switching; compare both bars. | Settings: Discovery Settings, Recommendation Mode | live |

### Limits

<!-- inventory: discovery -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `limits.min_score` | `src/alma/discovery/defaults.py` | `0` | `_refresh_lens_recommendations` | Drops candidates scoring below it before the slate is built, so a deck can come out shorter. | Refresh summary: raise if the tail of a deck is off-topic. 0 keeps everything. | Settings: Discovery Settings, Limits | live |
| `limits.max_candidates_per_strategy` | `src/alma/discovery/defaults.py` | `20` | `engine._discover_similar_with_meta_and_conn` | Candidates pulled per channel per seed by Discover Similar only. Lens refreshes do not read it. | Job result: channel `fetched` counts; raise if Similar returns too few. | Settings: Discovery Settings, Limits | live |
| `limits.recency_window_years` | `src/alma/discovery/defaults.py` | `10` | `scoring.measure_candidate` | Years over which the recency reading falls to zero. Reaches a score only when `weights.recency_boost` is above 0, which it is not by default. | Outcome evaluation once recency has weight. | Settings: Discovery Settings, Limits | live |
| `limits.lane_deadline_seconds` | `src/alma/discovery/defaults.py` | `60` | `_refresh_lens_recommendations`, clamped to 5 to 300 | Seconds one retrieval lane may run before the refresh builds the deck without it. | Refresh summary: raise if lanes are logged as cut. It is a backstop, it should never fire. | nowhere | live |

### Branches

<!-- inventory: discovery -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `branches.temperature` | `src/alma/discovery/defaults.py` | `0.28` | `seed_profile._resolve_branch_temperature` | Global temperature of a lens with no override. Hotter means fewer core topics, more explore topics, and at 0.5 or above explore terms come from the farthest branch. The mode fallback in that function (exploit 0.12, explore 0.55) never runs, because this key is always present. | Branch panel: core and explore topics; Refresh summary `lane_runs`. Lower if explore queries wander off the branch. | Settings: Discovery Settings, Branch Behavior | live |
| `branches.max_clusters` | `src/alma/discovery/defaults.py` | `6` | `seed_profile.lens_branch_cap` | Most branches a build keeps. `lens_branch_cap` caps it at 6, so the 7 to 12 the card allows do nothing. | Branch panel: raise only if distinct topics are merged, and only below 6. | Settings: Discovery Settings, Branch Behavior | live |
| `branches.max_active_for_retrieval` | `src/alma/discovery/defaults.py` | `4` | `_retrieve_external_channel` | How many active branches run frontier searches, in control order then score. Probing branches sit outside it. | Refresh summary: `lane_runs` by branch; Branch panel coverage. Raise if a branch gets no taste candidates. | Settings: Discovery Settings, Branch Behavior | live |
| `branches.query_core_variants` | `src/alma/discovery/defaults.py` | `2` | `_retrieve_external_channel` then `_plan_branch_queries_deterministic` | Core queries per branch. The planner writes at most 2, so 3 and 4 do nothing. | Refresh summary `lane_runs`. | Settings: Discovery Settings, Branch Behavior | live |
| `branches.query_explore_variants` | `src/alma/discovery/defaults.py` | `2` | `_retrieve_external_channel` then `_plan_branch_queries_deterministic` | Explore queries per branch. The planner writes at most 3, so 4 does nothing. | Refresh summary `lane_runs`. | Settings: Discovery Settings, Branch Behavior | live |

### Sources

The switches apply wherever `resolve_source_policy` is used: the frontier build, Feed topic and keyword monitors, and online search. Author and venue monitors do not go through it.

<!-- inventory: discovery -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `sources.openalex.enabled` | `src/alma/discovery/defaults.py` | `true` | `source_search.resolve_source_policy` | OpenAlex in free-text search. Off means fewer frontier and monitor candidates and no OpenAlex spend there. | Quota readout; turn off if credits run out and other sources suffice. | Settings: Discovery Settings, Source Control | dynamic |
| `sources.semantic_scholar.enabled` | `src/alma/discovery/defaults.py` | `true` | `source_search.resolve_source_policy`, `engine._discover_similar_with_meta_and_conn` | Semantic Scholar in search, and in Discover Similar. | Job result of a Feed refresh or frontier pass: timeouts and 429s. | Settings: Discovery Settings, Source Control | dynamic |
| `sources.crossref.enabled` | `src/alma/discovery/defaults.py` | `true` | `source_search.resolve_source_policy` | Crossref in search. | Feed refresh result by source. | Settings: Discovery Settings, Source Control | dynamic |
| `sources.arxiv.enabled` | `src/alma/discovery/defaults.py` | `true` | `source_search.resolve_source_policy`, `feed._read_monitor_refresh_settings` | arXiv in search. Preprint monitors force it on. | Feed refresh result by source. | Settings: Discovery Settings, Source Control | dynamic |
| `sources.biorxiv.enabled` | `src/alma/discovery/defaults.py` | `true` | `source_search.resolve_source_policy`, `feed._read_monitor_refresh_settings` | bioRxiv in search. Preprint monitors force it on. | Feed refresh result by source. | Settings: Discovery Settings, Source Control | dynamic |
| `sources.europe_pmc.enabled` | `src/alma/discovery/defaults.py` | `true` | `source_search.resolve_source_policy` | Europe PMC in search. | Feed refresh result by source. | Settings: Discovery Settings, Source Control | dynamic |
| `sources.semantic_scholar.fields_of_study` | `src/alma/discovery/defaults.py` | empty | `source_search._build_source_calls` | Comma-separated filter on Semantic Scholar bulk search only. Empty sends no filter. | Feed refresh result: `rejected_match` by a monitor. Set it if a monitor drowns in another field. | nowhere | live |
| `sources.semantic_scholar.publication_types` | `src/alma/discovery/defaults.py` | empty | `source_search._build_source_calls` | Comma-separated filter on Semantic Scholar bulk search only. | Feed refresh result. | nowhere | live |
| `sources.semantic_scholar.open_access_pdf` | `src/alma/discovery/defaults.py` | `false` | `source_search._build_source_calls` | Restricts Semantic Scholar bulk search to papers with an open PDF. | no measure | nowhere | live |

### Feed monitor defaults

Read on every refresh, so they apply to existing monitors as well as new ones.

<!-- inventory: discovery -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `monitor_defaults.author_per_refresh` | `src/alma/discovery/defaults.py` | `20` | `feed._read_monitor_refresh_settings` | Recent papers pulled per author monitor per refresh. | Feed refresh result: raise if a prolific author's new papers fall outside it. | Settings: Discovery Settings, Feed Monitor Defaults | live |
| `monitor_defaults.search_limit` | `src/alma/discovery/defaults.py` | `15` | `feed._read_monitor_refresh_settings` | Candidates per topic, keyword, venue, preprint or branch monitor. | Feed refresh result: `raw_candidates` against `accepted_candidates`. | Settings: Discovery Settings, Feed Monitor Defaults | live |
| `monitor_defaults.search_temperature` | `src/alma/discovery/defaults.py` | `0.22` | `feed._read_monitor_refresh_settings`, `feed._monitor_search_plan`, then `source_search._merge_candidates_from_sources` | Higher trusts provider rank over title match when merging a query's results. Keyword monitors cap it at 0.12, preprint and branch monitors floor it at 0.32 and 0.34. | Feed refresh result `rejected_match`; lower if matches miss the query. | Settings: Discovery Settings, Feed Monitor Defaults | live |
| `monitor_defaults.recency_years` | `src/alma/discovery/defaults.py` | `2` | `feed._resolve_feed_from_year` | Oldest publication year a monitor keeps. | Feed refresh result `rejected_recency`. | Settings: Discovery Settings, Feed Monitor Defaults | live |
| `monitor_defaults.include_preprints` | `src/alma/discovery/defaults.py` | `true` | `feed._read_monitor_refresh_settings` | Off forces arXiv and bioRxiv off for monitor search. | Feed refresh result by source. | Settings: Discovery Settings, Feed Monitor Defaults | live |
| `monitor_defaults.semantic_scholar_bulk` | `src/alma/discovery/defaults.py` | `true` | `feed._read_monitor_refresh_settings`, `feed._run_monitor_source_search` | Uses Semantic Scholar's bulk endpoint rather than interactive search. Bulk is the only mode with field filters. | Job result: Semantic Scholar timeouts. | Settings: Discovery Settings, Feed Monitor Defaults | live |

### Schedule, cache and embedding model

<!-- inventory: discovery -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `schedule.refresh_enabled` | `src/alma/discovery/defaults.py` | `false` | `setup_scheduler`, `update_discovery_settings` | Turns on periodic lens refresh. The reset route does not reschedule, so a reset only takes effect at the next restart. | Home Connections rail staleness. | Discovery page toggle; Settings: Discovery Settings, Refresh & Cache | live |
| `schedule.refresh_interval_hours` | `src/alma/discovery/defaults.py` | `6` | `setup_scheduler`, `update_discovery_settings` | Hours between periodic lens refreshes. 0 unregisters the job. | Job result of `refresh_recommendations`; lengthen if decks churn before you read them. | Settings: Discovery Settings, Refresh & Cache | live |
| `schedule.feed_refresh_enabled` | `src/alma/discovery/defaults.py` | `false` | `setup_scheduler`, `get_feed_settings`, `update_feed_settings` | Turns on periodic Feed refresh. | Home Connections rail. | Feed page, Tune monitors | live |
| `schedule.feed_refresh_interval_hours` | `src/alma/discovery/defaults.py` | `6` | `setup_scheduler`, `get_feed_settings`, `update_feed_settings` | Hours between periodic Feed refreshes. | Quota readout against Feed refresh result; lengthen if credits run short. | Feed page, Tune monitors | live |
| `schedule.graph_maintenance_interval_hours` | `src/alma/discovery/defaults.py` | `24` | `setup_scheduler`, `update_discovery_settings` | Hours between reference backfill and frontier rebuild. 0 stops the job, and with it new frontier leads. | Job result: frontier sizes and spend; Refresh summary `frontier_size`. | Settings: Discovery Settings, Refresh & Cache | live |
| `cache.similarity_ttl_hours` | `src/alma/discovery/defaults.py` | `24` | `routes.discovery._write_similarity_cache` | How long Discover Similar results stay cached. | no measure | Settings: Discovery Settings, Refresh & Cache | live |
| `embedding_model` | `src/alma/discovery/defaults.py` | `allenai/specter2_base` | `similarity.get_active_embedding_model`, `calibration.build_scoring_calibration`, `features.build_feature_snapshot`, `ai_status` | Which stored vectors count as active everywhere. `ai_configure` derives it from the provider, and with no provider it is SPECTER2. `PUT /discovery/settings` also accepts any string. A value no vector carries silently empties semantic retrieval. | Health embedding coverage; must equal the active provider's model. | Settings: AI, by choosing a provider | live |

### Author and paper signal weights

These three groups share the rule that a component with no data hands its weight to the components that have some. Nothing exposes them, and no measure isolates them.

<!-- inventory: discovery -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `paper_signal_weights.rating` | `src/alma/discovery/defaults.py` | `0.20` | `paper_signal._resolve_weights`, `score_papers_batch` | Rating share of a paper's global signal, which orders Library seeds, picks seeds for author suggestions and sorts Library by signal. | no measure | nowhere | dynamic |
| `paper_signal_weights.topic_alignment` | `src/alma/discovery/defaults.py` | `0.20` | `paper_signal._resolve_weights`, `score_papers_batch` | Topic share of that signal. | no measure | nowhere | dynamic |
| `paper_signal_weights.embedding_sim` | `src/alma/discovery/defaults.py` | `0.25` | `paper_signal._resolve_weights`, `score_papers_batch` | Embedding share of that signal. | no measure | nowhere | dynamic |
| `paper_signal_weights.author_alignment` | `src/alma/discovery/defaults.py` | `0.15` | `paper_signal._resolve_weights`, `score_papers_batch` | Author share of that signal. | no measure | nowhere | dynamic |
| `paper_signal_weights.feedback_events` | `src/alma/discovery/defaults.py` | `0.10` | `paper_signal._resolve_weights`, `score_papers_batch` | Feedback-event share of that signal. | no measure | nowhere | dynamic |
| `paper_signal_weights.recency` | `src/alma/discovery/defaults.py` | `0.10` | `paper_signal._resolve_weights`, `score_papers_batch` | Recency share of that signal. | no measure | nowhere | dynamic |
| `candidate_author_weights.seed_cooccurrence` | `src/alma/discovery/defaults.py` | `0.25` | `author_network._resolve_candidate_weights`, `_compute_composite` | Share of a network candidate's score from appearing with several seeds. | Suggestion rows: follow rate per bucket. | nowhere | dynamic |
| `candidate_author_weights.topic_overlap` | `src/alma/discovery/defaults.py` | `0.25` | `author_network._resolve_candidate_weights`, `_compute_composite` | Share from topic overlap with the Library. | Suggestion rows. | nowhere | dynamic |
| `candidate_author_weights.centroid_sim` | `src/alma/discovery/defaults.py` | `0.20` | `author_network._resolve_candidate_weights`, `_compute_composite` | Share from the author centroid's similarity to the Library. | Suggestion rows. | nowhere | dynamic |
| `candidate_author_weights.venue_overlap` | `src/alma/discovery/defaults.py` | `0.15` | `author_network._resolve_candidate_weights`, `_compute_composite` | Share from shared venues. | Suggestion rows. | nowhere | dynamic |
| `candidate_author_weights.recency_activity` | `src/alma/discovery/defaults.py` | `0.10` | `author_network._resolve_candidate_weights`, `_compute_composite` | Share from recent publishing. | Suggestion rows. | nowhere | dynamic |
| `candidate_author_weights.h_index_soft` | `src/alma/discovery/defaults.py` | `0.05` | `author_network._resolve_candidate_weights`, `_compute_composite` | Share from h-index. | Suggestion rows. | nowhere | dynamic |
| `author_suggestion_weights.library_core` | `src/alma/discovery/defaults.py` | `1.0` | `authors._load_author_suggestion_weights`, `list_author_suggestions` | Multiplier on the library_core bucket in the Authors rail. | Suggestion rows: `bucket_calibration_multiplier` persistently off 1.0 says the bucket is over- or under-weighted. | nowhere | dynamic |
| `author_suggestion_weights.cited_by_high_signal` | `src/alma/discovery/defaults.py` | `0.9` | `authors._load_author_suggestion_weights`, `list_author_suggestions` | Multiplier on the cited_by_high_signal bucket. | Suggestion rows. | nowhere | dynamic |
| `author_suggestion_weights.adjacent` | `src/alma/discovery/defaults.py` | `0.7` | `authors._load_author_suggestion_weights`, `list_author_suggestions` | Multiplier on the adjacent bucket. | Suggestion rows. | nowhere | dynamic |
| `author_suggestion_weights.semantic_similar` | `src/alma/discovery/defaults.py` | `0.8` | `authors._load_author_suggestion_weights`, `list_author_suggestions` | Multiplier on the semantic_similar bucket. | Suggestion rows. | nowhere | dynamic |
| `author_suggestion_weights.openalex_related` | `src/alma/discovery/defaults.py` | `0.9` | `authors._load_author_suggestion_weights`, `list_author_suggestions` | Multiplier on the openalex_related bucket. | Suggestion rows. | nowhere | dynamic |
| `author_suggestion_weights.s2_related` | `src/alma/discovery/defaults.py` | `0.9` | `authors._load_author_suggestion_weights`, `list_author_suggestions` | Multiplier on the s2_related bucket. | Suggestion rows. | nowhere | dynamic |
| `author_suggestion_cache_ttl_hours` | `src/alma/discovery/defaults.py` | `24` | `author_network._cache_ttl_hours`, `refresh_author_suggestions_periodic` | Age at which a network bucket is fetched again. | Quota readout; lengthen if suggestions cost too much. | nowhere | live |

### Other keys in `discovery_settings`

These live in the same table but not in `DISCOVERY_SETTINGS_DEFAULTS`. The last rows are state the app writes for itself, listed so nobody mistakes them for controls.

<!-- inventory: kv -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `addon.<id>.schema_version` | `src/alma/core/addons.py` | unset until first enable | `addons.ensure_schema` | Schema version of one add-on's tables. A value above the build's own blocks that add-on alone. Not for tuning; Remove deletes it. | no measure | nowhere | dynamic |
| `branches.resolution` | `src/alma/application/discovery/seed_profile.py` | unset, then `1.0` | `_resolve_branch_resolution` | Global cluster detail when a lens has no override. Not in the defaults, and no route writes it. | Branch panel. | nowhere | live |
| `schedule.scoring_calibration_interval_hours` | `src/alma/api/scheduler.py` | unset, then 12 | `setup_scheduler` | Hours between scoring calibration checks. 0 stops the job. Not in the defaults, and no route writes it. | Job result. | nowhere | live |
| `ai.provider` | `src/alma/api/deps.py` | `local` when the AI packages import, else `none` | `get_active_provider`, `ai_configure` | Who computes missing embeddings: `local` or `none`. | Health embedding coverage. | Settings: AI | live |
| `ai.local_model` | `src/alma/ai/providers.py` | `specter2-base` | `get_active_provider` | Which local embedding model runs. | Health embedding coverage. | Settings: AI | live |
| `ai.python_env_type` | `src/alma/api/deps.py` | detected at startup | `get_dependency_environment` | Kind of Python environment to import AI packages from. | AI status `dependency_environment`. | Settings: AI | live |
| `ai.python_env_path` | `src/alma/api/deps.py` | detected at startup | `get_dependency_environment` | Folder of that environment. | AI status `dependency_environment`. | Settings: AI | live |
| `maintenance.idle_wait_minutes` | `src/alma/services/background_settings.py` | 3 | `get_idle_wait_seconds`, then the maintenance admission gate | Idle minutes before a background sweep starts. 0 means as soon as nothing else runs. | Job result: sweeps that keep yielding. | Settings: Background Operations | live |
| `maintenance.reserved_api_calls` | `src/alma/services/background_settings.py` | 200 | `get_reserved_api_calls`, `_budget_refusal` | Provider calls background work leaves for you. | Quota readout: raise if your manual runs meet an exhausted quota. | Settings: Background Operations | live |
| `maintenance.<task>.auto_enabled` | `src/alma/services/maintenance.py` | off | `get_task_auto_enabled`, `maintenance_repair_periodic` | Opt-in of one repair to the idle healer. Destructive tasks can never be on. | Health page. | Health page, repair card | live |
| `maintenance.<task>.auto_daily_cap` | `src/alma/services/maintenance.py` | per task, below | `get_task_auto_daily_cap`, `maintenance_repair_periodic` | Units the healer may process per UTC day. | Health page: raise if the backlog never drains, lower if quota runs out. | Health page, repair card | live |
| `maintenance.<task>.remembered_manual_limit` | `src/alma/services/maintenance.py` | per task, below | `get_task_manual_limit` | The prefilled amount of Run now. | Health page. | Health page, repair card | live |
| `maintenance.<task>.request_batch_size` | `src/alma/services/maintenance.py` | per task | `get_task_request_batch_size` | Upstream payload size, for tasks that have a batch. | Job result: lower on response-size failures. | Health page, repair card | live |
| `maintenance.<task>.paused_by_user_until` | `src/alma/services/maintenance.py` | empty | `get_task_user_pause` | Set when you stop a run: the healer leaves the task alone until then. | Health page. | Activity, by stopping a job | live |
| `user.name` | `src/alma/api/routes/onboarding.py` | unset | `routes.onboarding`, `home.build_daily_brief` | The name Home greets you with. | no measure | Settings: You | live |
| `walkthrough.<id>` | `src/alma/application/walkthroughs.py` | unset | `walkthroughs` | Whether a page tour was seen. | no measure | Settings: First-run setup, by restarting | live |
| `rotation.<name>` | `src/alma/core/rotation.py` | unset | `load_cursor`, `save_cursors` | State: position of a rotation over a pool. | Job result carries `RotationWindow.record`. | nowhere | live |
| `feed.last_seen_at` | `src/alma/application/feed.py` | unset | `feed` | State: when you last opened the Feed. | no measure | nowhere | live |
| `feed.drop_runs` | `src/alma/application/feed.py` | unset | `feed_refresh_outcome` | State: refreshes in a row that lost yield, by `lost_yield`. | Feed refresh result. | nowhere | live |
| `discovery.lens_seen.<lens>` | `src/alma/application/discovery/lens_crud.py` | unset | `get_lens_last_seen` | State: when a lens was last opened. | no measure | nowhere | live |
| `hydration.post_hydration_chain_pending` | `src/alma/services/embedding_chain.py` | unset | `is_post_hydration_chain_pending` | State: an embedding chain is owed after hydration. | no measure | nowhere | live |

## Maintenance tasks

Defaults for the per-task keys above. Every task starts with `auto_enabled` off. The healer takes at most `HEALER_PER_TICK_LIMIT` items per tick from a task, and `auto_chunk_size` per run.

<!-- inventory: task -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `corpus_metadata` | `src/alma/services/maintenance.py` | daily 500, manual 500, max 5000 | `maintenance_repair_periodic`, `run_task_now` | Fills paper metadata from OpenAlex, Semantic Scholar, Crossref and landing pages. | Health page; Quota readout. | Health page, repair card | live |
| `s2_vector` | `src/alma/services/maintenance.py` | daily 500, manual 500, max 5000 | `maintenance_repair_periodic`, `run_task_now` | Fetches Semantic Scholar vectors. | Health embedding coverage. | Health page, repair card | live |
| `embedding` | `src/alma/services/maintenance.py` | daily 500, manual 500, max 5000 | `maintenance_repair_periodic`, `run_task_now` | Computes missing vectors locally. | Health embedding coverage. | Health page, repair card | live |
| `title_resolution` | `src/alma/services/maintenance.py` | daily 200, manual 200, max 5000 | `maintenance_repair_periodic`, `run_task_now` | Finds identifiers for papers that have none, by title. | Health page; Quota readout. | Health page, repair card | live |
| `author_metadata` | `src/alma/services/maintenance.py` | daily 200, manual 200, max 2000 | `maintenance_repair_periodic`, `run_task_now` | Rehydrates author profiles. | Health page. | Health page, repair card | live |
| `author_works` | `src/alma/services/maintenance.py` | daily 25, manual 25, max 500 | `maintenance_repair_periodic`, `run_task_now` | Expands works of authors with too few. | Health page; Quota readout. | Health page, repair card | live |
| `author_seed_thin` | `src/alma/services/maintenance.py` | daily 30, manual 30, max 30 | `maintenance_repair_periodic`, `run_task_now` | Seeds papers for under-covered suggested authors. | Health page. | Health page, repair card | live |
| `corpus_backfill_stale` | `src/alma/services/maintenance.py` | daily 5, manual 10, max 200 | `maintenance_repair_periodic`, `run_task_now` | Backfills authors gone stale. | Health page. | Health page, repair card | live |
| `gc_orphan_authors` | `src/alma/services/maintenance.py` | daily 500, manual 500, max 5000, destructive | `run_task_now` | Removes orphan authors. Manual only. | Health page. | Health page, repair card | live |
| `dedup_orcid` | `src/alma/services/maintenance.py` | daily 100, manual 100, max 500 | `maintenance_repair_periodic`, `run_task_now` | Scans authors for duplicates. | Health page. | Health page, repair card | live |
| `dedup_orcid_merge` | `src/alma/services/maintenance.py` | daily 100, manual 100, max 500, destructive | `run_task_now` | Merges duplicate authors. Manual only. | Health page. | Health page, repair card | live |
| `paper_group_reconcile` | `src/alma/services/maintenance.py` | daily 1000, manual 1000, max 20000 | `maintenance_repair_periodic`, `run_task_now` | Reconciles paper groups. | Health page. | Health page, repair card | live |
| `collapse_duplicate_identity` | `src/alma/services/maintenance.py` | daily 500, manual 500, max 5000 | `maintenance_repair_periodic`, `run_task_now` | Collapses duplicate-identity papers. | Health page. | Health page, repair card | live |
| `dedup_preprint_twins` | `src/alma/services/maintenance.py` | daily 500, manual 500, max 5000, destructive | `run_task_now` | Merges preprint and journal twins. Manual only. | Health page. | Health page, repair card | live |
| `library_dedup` | `src/alma/services/maintenance.py` | daily 1, manual 1, max 1, destructive | `run_task_now` | Deduplicates the Library. Manual only. | Health page. | Health page, repair card | live |
| `author_centroids` | `src/alma/services/maintenance.py` | daily 1000, manual 500, max 5000 | `maintenance_repair_periodic`, `run_task_now` | Recomputes author centroids. | Health page. | Health page, repair card | live |
| `reference_graph` | `src/alma/services/maintenance.py` | daily 250, manual 250, max 5000 | `maintenance_repair_periodic`, `run_task_now` | Backfills references. | Health page. | Health page, repair card | live |
| `topic_normalize` | `src/alma/services/maintenance.py` | daily 1, manual 1, max 1 | `maintenance_repair_periodic`, `run_task_now` | Normalizes topics. | Health page. | Health page, repair card | live |
| `housekeeping` | `src/alma/services/maintenance.py` | daily 1, manual 1, max 1 | `maintenance_repair_periodic`, `run_task_now` | Database housekeeping. | Health page. | Health page, repair card | live |

## Per-lens controls

Stored on each row of `discovery_lenses`. Written by `PUT /api/v1/lenses/{id}` and, for pin, boost and mute, by `apply_branch_control_action`.

<!-- inventory: lens -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `weights` | `src/alma/application/discovery/lens_crud.py` | `DEFAULT_CHANNEL_WEIGHTS` by context | `_refresh_lens_recommendations`, `_merge_channel_candidates` | Lexical, vector, graph and external shares. They set `source_relevance`, which is recorded and shown but is not a reward feature: the Retrieval family reads unweighted ranks, so these weights move no final score and no card. Tested 2026-10-31: swapping all weight between two channels moved the fused score from 100 to 0 and the final score not at all. | no measure | Discovery page, channel weights panel | no-op |
| `branch_controls.temperature` | `src/alma/application/discovery/lens_crud.py` | `None`, then `branches.temperature` | `_resolve_branch_temperature` | Per-lens temperature, same effect as the global key. The panel opens at 0.28 and saves that as an override. | Branch panel. | Discovery page, branch panel | live |
| `branch_controls.resolution` | `src/alma/application/discovery/lens_crud.py` | `None`, then `branches.resolution`, then 1.0 | `_resolve_branch_resolution`, `rebuild_reason` | Cluster detail, 0.5 to 3. Above 1 gives more branches. Changing it forces a rebuild, which costs branch identities. | Branch panel: branches too broad or too thin. | Discovery page, branch panel | live |
| `branch_controls.pinned` | `src/alma/application/discovery/lens_crud.py` | `[]` | `_apply_branch_controls`, `_apply_branch_auto_lifecycle` | Pinned branches sort first, so they pass the `branches.max_active_for_retrieval` cut, and are exempt from auto-rotate and auto-mute. | Branch panel coverage. | Discovery page, branch panel | live |
| `branch_controls.boosted` | `src/alma/application/discovery/lens_crud.py` | `[]` | `_apply_branch_controls`, `_apply_branch_auto_lifecycle` | Same as pinned, one sort place lower. Despite the name it adds no budget. | Branch panel coverage. | Discovery page, branch panel | live |
| `branch_controls.muted` | `src/alma/application/discovery/lens_crud.py` | `[]` | `_apply_branch_controls`, `branch_can_retrieve` | A muted branch stops retrieving and gets no card. Its seeds still shape ranking. | Branch panel. | Discovery page, branch panel | live |
| `branch_controls.custom_directions` | `src/alma/application/discovery/lens_crud.py` | `[]` | `_direction_centroids`, `_lexical_query_terms` | Adopted regions pull every centroid toward them and add lexical queries. Nothing in this line can create one: adoption came from the map, which left with D24. The panel can only remove them. | no measure | API only; removal on the Discovery page | live |
| `context_type` | `src/alma/application/discovery/lens_crud.py` | required | `_load_seed_papers_for_lens`, `default_channel_weights` | What the lens is seeded from: `library_global`, `collection`, `topic_keyword`, `tag`. | Refresh summary `seed_count`. | Discovery page, lens manager | live |
| `context_config` | `src/alma/application/discovery/lens_crud.py` | per context | `_load_seed_papers_for_lens`, `_lexical_query_terms` | The collection, keyword or tag of the lens. | Refresh summary `seed_count`. | Discovery page, lens manager | live |
| `is_active` | `src/alma/application/discovery/lens_crud.py` | `1` | `list_lenses`, `refresh_recommendations_periodic` | Inactive lenses leave the page and the periodic refresh. | no measure | Discovery page, lens manager | live |
| `position` | `src/alma/application/discovery/lens_crud.py` | `0` | `list_lenses` | Lens order on the page. | no measure | Discovery page, drag | live |
| `name` | `src/alma/application/discovery/lens_crud.py` | required | `list_lenses` | Label of the lens. | no measure | Discovery page, lens manager | live |
| `branch_structure` | `src/alma/application/discovery/lens_crud.py` | unset until the first build | `resolve_lens_branches`, `rebuild_reason` | State: the built branch membership. A refresh keeps it unless `rebuild_reason` names a reason. | Branch panel `turnover`. | Discovery page, Rebuild | live |
| `preference_profile` | `src/alma/api/deps.py` | unset | Nothing | Column in the lens table that no code reads or writes. | no measure | nowhere | no-op |

## Per-monitor and per-alert controls

<!-- inventory: row -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `enabled` (monitor) | `src/alma/application/feed_monitors.py` | `1` | `refresh_feed_inbox`, `refresh_feed_monitor` | Paused monitors stop fetching. | Feed refresh result. | Feed page, Tune monitors | live |
| `query` (monitor) | `src/alma/application/feed_monitors.py` | required | `feed._monitor_search_plan` | What a keyword, topic or venue monitor searches for. | Feed refresh result: `rejected_match`, and the zero-yield warning. | Feed page, Tune monitors | live |
| `filter_keywords` (monitor) | `src/alma/application/feed_monitors.py` | empty | `_filter_monitor_candidates` | Venue monitors keep only papers matching one keyword. | Feed refresh result `rejected_match`. | Feed page, Tune monitors | live |
| `source_id` (monitor) | `src/alma/application/feed_monitors.py` | required for venue | `feed._run_monitor_source_search` | The OpenAlex source of a venue monitor. | Feed refresh result. | Feed page, venue picker | live |
| `semantic_scholar_enabled` (monitor) | `src/alma/application/feed.py` | false | `feed._monitor_search_plan` | Adds Semantic Scholar to a preprint monitor. No screen writes it. | no measure | API only | live |
| `temperature` (monitor) | `src/alma/application/feed.py` | 0.34 | `feed._monitor_search_plan` | Floor on a branch monitor's search temperature. No screen writes it. | no measure | API only | live |
| `schedule` (alert) | `src/alma/addons/alerts/service.py` | `manual` | `evaluate_scheduled_alerts` | When an alert fires. | Alert history. | Alerts page | live |
| `schedule_config` (alert) | `src/alma/addons/alerts/service.py` | unset | `next_slot`, `reference_slot` | The day and time slot of that schedule. | Alert history. | Alerts page | live |
| `rule_config` (alert) | `src/alma/addons/alerts/service.py` | required | alert rule evaluation | What a rule matches. | Alert history. | Alerts page | live |

## Add-ons

An add-on is a folder under `src/alma/addons/` listed in `ADDON_MODULES` (`src/alma/core/addons.py`). Each one has a switch, read on every request, so a change needs no restart. While it is off its routes answer 404, its jobs are not scheduled, and its nav entry, pills and figures are gone. Its data stays until an explicit Remove. If the switch is missing from `settings.json`, startup writes it once as `default_enabled or has_user_data(conn)`; a switch that exists is never changed. `GET /api/v1/addons` lists them.

<!-- inventory: addon -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `plugins.alerts.enabled` | `src/alma/addons/alerts/__init__.py` | `False`, or `True` when `alerts` or `alert_rules` has rows | `addons.is_enabled`, `require_addon` | Everything Alerts does: routes, the sweep, the sidebar entry, Home figure and pill, the Health check, and the Email plugin. | no measure | Settings: Add-ons | live |
| `plugins.auto_tagger.enabled` | `src/alma/addons/auto_tagger/__init__.py` | `False`, or `True` when `tag_suggestions` has rows | `addons.is_enabled`, `require_addon` | Tag suggestions: `/tags/suggestions/*`, and the suggestion panel under Library, Tags, Tune tags. Merging tags stays core. | no measure | Settings: Add-ons | live |

## App settings

Stored in `settings.json`, through `alma.config`. `PUT /api/v1/settings` writes the fields `SettingsModel` declares, `DEFAULT_SETTINGS` holds the defaults, and startup checks the plugin and network keys are the right type.

<!-- inventory: app -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `settings_schema_version` | `src/alma/config.py` | `3` | `migrate_settings_schema`, `validate_settings_schema` | Migration marker. Not for tuning. | no measure | nowhere | live |
| `network_access_enabled` | `src/alma/config.py` | `True` | `core.network_policy.network_access_enabled`, `require_network_access` | Master switch for all outbound traffic. Off fails each call before it leaves. | Health network status. | Settings: External APIs; Health | live |
| `backend` | `src/alma/config.py` | `openalex` | `get_backend`, `get_fetch_year`, `core.backend` | Where author fetches go. `scholar` is the legacy scraping backend and turns `fetch_full_history` off. | no measure | Settings: Backend | live |
| `openalex_email` | `src/alma/config.py` | `None` | `get_openalex_email`, `get_contact_email` | Contact address sent to OpenAlex and used for User-Agent, Crossref and Unpaywall. | Crossref drops to 1 request a second with no contact address. | Settings: External APIs, OpenAlex | live |
| `fetch_full_history` | `src/alma/config.py` | `False` | `get_fetch_full_history`, `get_fetch_year` | OpenAlex author fetches take all years. The Feed still applies `monitor_defaults.recency_years`. | Quota readout. | API only | live |
| `from_year` | `src/alma/config.py` | `None` | `get_from_year`, `get_fetch_year` | First year of author fetches. Empty means the current year. | no measure | API only | live |
| `api_call_delay` | `src/alma/config.py` | `1.0` | `get_api_call_delay`, `enrich_all_unenriched` | Delay between calls of the post-import enrichment batch. | no measure | API only | live |
| `database` | `src/alma/config.py` | computed | `get_db_path` | Relative path of the database, below `DB_PATH` in priority. | no measure | API only | live |
| `id_resolution_semantic_scholar_enabled` | `src/alma/config.py` | `True` | `routes.authors` resolver options | Semantic Scholar in author identifier resolution. | Authors needing review on Health. | Settings: Identifier Resolution | live |
| `id_resolution_orcid_enabled` | `src/alma/config.py` | `True` | `routes.authors` resolver options | ORCID in author identifier resolution. | Authors needing review on Health. | Settings: Identifier Resolution | live |
| `id_resolution_scholar_scrape_auto_enabled` | `src/alma/config.py` | `False` | `routes.authors` resolver options | Google Scholar scraping inside pipelines. | Authors needing review on Health. | Settings: Identifier Resolution | live |
| `id_resolution_scholar_scrape_manual_enabled` | `src/alma/config.py` | `False` | `routes.authors` resolver options | Google Scholar scraping from the Authors page. | Authors needing review on Health. | Settings: Identifier Resolution | live |
| `slack_channel` | `src/alma/config.py` | `None` | `get_slack_channel`, `plugins.slack` | Channel of Slack alerts. | Alert history. | Settings: Plugins, Slack | live |
| `slack_inbox_channel` | `src/alma/config.py` | `None` | `SlackInboxChannel`, `plugins.slack` | Channel ALMa reads for Inbox capture. Empty means capture off. | Inbox ledger. | Settings: Plugins, Slack | live |
| `smtp_host` | `src/alma/addons/alerts/__init__.py` | `None` | `get_smtp_host`, `plugins.email` | Mail server for alert digests. | Alert history. | Settings: Plugins, Email, while Alerts is on | live |
| `smtp_port` | `src/alma/addons/alerts/__init__.py` | `587` | `get_smtp_port`, `plugins.email` | Mail server port. | Alert history. | Settings: Plugins, Email, while Alerts is on | live |
| `smtp_username` | `src/alma/addons/alerts/__init__.py` | `None` | `get_smtp_username`, `plugins.email` | Mail login name, and the sender when `smtp_from` is empty. | Alert history. | Settings: Plugins, Email, while Alerts is on | live |
| `smtp_from` | `src/alma/addons/alerts/__init__.py` | `None` | `get_smtp_from`, `plugins.email` | Sender address. | Alert history. | Settings: Plugins, Email, while Alerts is on | live |
| `smtp_to` | `src/alma/addons/alerts/__init__.py` | `None` | `get_smtp_recipients`, `plugins.email` | Digest recipients. | Alert history. | Settings: Plugins, Email, while Alerts is on | live |
| `smtp_use_tls` | `src/alma/addons/alerts/__init__.py` | `True` | `get_smtp_use_tls`, `plugins.email` | STARTTLS. | Alert history. | Settings: Plugins, Email, while Alerts is on | live |
| `plugins.slack.enabled` | `src/alma/config.py` | `False` | `PluginManifest.is_enabled` | Activates Slack delivery and capture. Saved credentials alone do nothing. | Plugin status. | Settings: Plugins | live |
| `plugins.email.enabled` | `src/alma/addons/alerts/__init__.py` | `False` | `PluginManifest.is_enabled` | Activates email delivery. A switch inside Alerts: it does nothing while Alerts is off. | Plugin status. | Settings: Plugins, while Alerts is on | live |
| `plugins.open_access.enabled` | `src/alma/config.py` | `False` | `PluginManifest.is_enabled` | Activates open-access PDF finding. | PDF attempts in Activity. | Settings: Plugins | live |
| `plugins.open_access.use_openalex_content` | `src/alma/config.py` | `False` | `plugins.open_access._sources` | Adds OpenAlex cached PDFs as a last resort, at 100 credits a PDF. | Quota readout. | Settings: Plugins, advanced | live |
| `plugins.shadow_libraries.enabled` | `src/alma/config.py` | `False` | `PluginManifest.is_enabled` | Activates shadow-library PDF finding. | PDF attempts in Activity. | Settings: Plugins | live |
| `plugins.shadow_libraries.scihub_mirrors` | `src/alma/config.py` | empty | `plugins.shadow_libraries._settings` | Mirrors tried in order. | Plugin Test button. | Settings: Plugins | live |
| `plugins.shadow_libraries.annas_archive_domains` | `src/alma/config.py` | empty | `plugins.shadow_libraries._settings` | Addresses tried in order. | Plugin Test button. | Settings: Plugins | live |
| `contact_email` | `src/alma/config.py` | unset | `get_contact_email` | Contact address. Read, but no default and no route. | no measure | nowhere | live |
| `crossref_mailto` | `src/alma/config.py` | unset | `get_crossref_mailto`, `get_contact_email` | Crossref polite-pool address. Read, but no default and no route. | no measure | nowhere | live |
| `user_agent` | `src/alma/config.py` | unset | `get_app_user_agent` | User-Agent override. Read, but no default and no route. | no measure | nowhere | live |

### Secrets

Names and purposes only. They live in the secret store and can also come from the environment.

<!-- inventory: secret -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `SECRET_OPENALEX_API_KEY` | `src/alma/core/secrets.py` | unset | `get_openalex_api_key` | OpenAlex API key. Without it, 100 credits a day. | Quota readout. | Settings: External APIs | live |
| `SECRET_SEMANTIC_SCHOLAR_API_KEY` | `src/alma/core/secrets.py` | unset | `get_semantic_scholar_api_key` | Semantic Scholar API key. | Settings status check. | Settings: External APIs | live |
| `SECRET_SLACK_BOT_TOKEN` | `src/alma/core/secrets.py` | unset | `get_slack_token` | Slack bot token. | Plugin Test button. | Settings: Plugins, Slack | live |
| `SECRET_SMTP_PASSWORD` | `src/alma/core/secrets.py` | unset | `get_smtp_password` | Mail password. | Plugin Test button. | Settings: Plugins, Email, while Alerts is on | live |
| `SECRET_ANNAS_ARCHIVE_KEY` | `src/alma/core/secrets.py` | unset | `plugins.shadow_libraries._settings` | Anna's Archive member key. | Plugin Test button. | Settings: Plugins, advanced | live |

## Environment variables

### Location and profile

<!-- inventory: env -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `ALMA_ENV` | `src/alma/config.py` | `prod` | `get_env_profile` | Profile. Anything but prod uses its own data and config folders and runs no unattended network work. | no measure | env | live |
| `DATA_DIR` | `src/alma/config.py` | OS data folder | `get_data_dir` | Where data lives. The image pins it to `/app/data`. | no measure | env; Dockerfile | live |
| `ALMA_CONFIG_DIR` | `src/alma/config.py` | OS config folder | `get_config_dir` | Where `.env` and settings live. | no measure | env | live |
| `ALMA_SETTINGS_PATH` | `src/alma/config.py` | `settings.json` in the config folder | `get_settings_path` | Path of `settings.json`. The image pins it to the data volume. | no measure | env; Dockerfile | live |
| `DB_PATH` | `src/alma/config.py` | `scholar.db` in the data folder | `get_db_path` | Database file. Also moves the PDF store. | no measure | env | live |
| `ALMA_SECRETS_PATH` | `src/alma/core/secrets.py` | `secrets.json` by the data | `_resolve_store_path` | Path of the secret store. | no measure | env | live |
| `ALMA_DB_MIGRATION` | `src/alma/core/storage_migration.py` | unset | `_decide` | `migrate` or `fresh` for a legacy database when nobody can answer the prompt. | no measure | env | live |
| `ALMA_CONFIG_MIGRATION` | `src/alma/core/storage_migration.py` | unset | `_decide` | The same for legacy config. | no measure | env | live |

### Network, access and scheduling

<!-- inventory: env -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `ALMA_DISABLE_NETWORK` | `src/alma/core/network_policy.py` | unset | `_env_disables_network` | Forces all outbound traffic off. Cannot force it on. | no measure | env | live |
| `ALMA_UNATTENDED_NETWORK` | `src/alma/core/network_policy.py` | on for prod, off otherwise | `unattended_network_enabled` | Whether the scheduler may start network work on its own. | Quota readout: every profile shares one key. | env | live |
| `ALMA_ALLOWED_ORIGINS` | `src/alma/api/origin_guard.py` | empty | `allowed_origins` | Addresses beyond localhost and IPs the app answers on. A malformed entry stops startup. | Refused-address message in the browser. | env; `.env` | live |
| `FRONTEND_URL` | `src/alma/api/origin_guard.py` | unset | `allowed_origins` | One more allowed address. | Refused-address message. | env | live |
| `API_KEY` | `src/alma/api/deps.py` | empty | `verify_api_key` | Requires a key on most routes. Secret. The web app sends no key, so setting it breaks the browser UI. | no measure | env; `.env` | live |
| `API_HOST` | `src/alma/api/app.py` | `0.0.0.0` | `__main__` block | Bind address when run as a script. The image and dev script use uvicorn directly. | no measure | env | live |
| `API_PORT` | `src/alma/api/app.py` | `8000` | `__main__` block | Port when run as a script. | no measure | env | live |
| `DEBUG` | `src/alma/api/app.py` | `false` | `__main__` block | Reload when run as a script. | no measure | env | live |
| `SCHEDULER_ENABLED` | `src/alma/api/scheduler.py` | `true` | `_scheduler_enabled` | Off registers no periodic job at all. | no measure | env | live |
| `ALMA_SCHEDULER_WORKERS` | `src/alma/api/scheduler.py` | `5` | `_scheduler_max_workers`, clamped 1 to 16 | Background jobs running at once. | Writes failing with lock errors means lower; queues that never drain mean raise. | env | live |
| `ALERT_CHECK_INTERVAL_HOURS` | `src/alma/addons/alerts/jobs.py` | `1` | `alert_check_interval_minutes` | Hours between alert evaluations. | Alert history lateness. | env | live |
| `INBOX_SWEEP_INTERVAL_MINUTES` | `src/alma/api/scheduler.py` | `5` | `_inbox_sweep_interval_minutes` | Minutes between Inbox capture polls. 0 disables. | Inbox ledger latency. | env; `.env` | live |
| `ALMA_AUTHOR_SUGGESTION_REFRESH_INTERVAL_HOURS` | `src/alma/api/scheduler.py` | `6` | `setup_scheduler` | Hours between checks for stale suggestion caches. | Job result. | env | live |
| `ALMA_IDLE_MAINTENANCE_INTERVAL_HOURS` | `src/alma/api/scheduler.py` | `1` | `setup_scheduler` | Hours between idle healer ticks. | Health page backlog. | env | live |
| `ALMA_DISABLE_IDLE_MAINTENANCE` | `src/alma/services/maintenance.py` | unset | `maintenance_repair_periodic` | Hard kill of the idle healer. | no measure | env | live |
| `ALMA_HYDRATION_DRAIN_INTERVAL_MINUTES` | `src/alma/api/scheduler.py` | `15` | `setup_scheduler` | Minutes between checks for pending hydration. | Health page backlog. | env | live |
| `ALMA_OPERATION_LOG_RETENTION_DAYS` | `src/alma/api/scheduler.py` | `30` | `_operation_log_retention_days` | Days of `operation_logs` kept. | Database size. | env | live |
| `ALMA_BACKUP_RETAIN` | `src/alma/api/routes/library_mgmt.py` | `5` | `_backup_retain_count` | Backups kept on disk. | no measure | env | live |
| `ALMA_DEEP_REFRESH_WORKERS` | `src/alma/api/routes/authors.py` | `4` | deep-refresh worker count, clamped 1 to 16 | Parallel authors in a deep refresh. | Job result: lock errors mean lower. | env | live |
| `ALMA_DEFAULT_AI_PROVIDER` | `src/alma/api/deps.py` | follows the environment | `default_ai_provider`, migration | Forces the first-run embedding provider. | Health embedding coverage. | env | live |
| `SCHOLAR_RETRY_DELAYS` | `src/alma/core/fetcher.py` | `20,40,60` | module load of `core.fetcher` | Retry delays of the legacy Scholar scraper. | no measure | env | live |

### Credentials and contact

Secrets are listed by name and purpose only.

<!-- inventory: env -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `OPENALEX_API_KEY` | `src/alma/config.py` | unset | `get_openalex_api_key`, `openalex.http` | OpenAlex API key. Secret. Wins over the secret store. | Quota readout. | env; `.env`; Settings writes it back | live |
| `SEMANTIC_SCHOLAR_API_KEY` | `src/alma/config.py` | unset | `get_semantic_scholar_api_key` | Semantic Scholar API key. Secret. | Settings status check. | env; `.env`; Settings writes it back | live |
| `OPENALEX_EMAIL` | `src/alma/config.py` | unset | `get_openalex_email`, `get_contact_email` | Contact address, wins over `openalex_email`. | no measure | env; `.env` | live |
| `ALMA_CONTACT_EMAIL` | `src/alma/config.py` | unset | `get_contact_email` | First choice of contact address. | no measure | env | live |
| `CONTACT_EMAIL` | `src/alma/config.py` | unset | `get_contact_email` | Second choice of contact address. | no measure | env | live |
| `CROSSREF_MAILTO` | `src/alma/config.py` | unset | `get_crossref_mailto`, `get_contact_email` | Crossref polite-pool address. Any contact address raises Crossref pace from 1 to 3 requests a second. | Job result: Crossref 429s. | env; `.env` | live |
| `ALMA_USER_AGENT` | `src/alma/config.py` | `ALMa/3.0` | `get_app_user_agent` | User-Agent of upstream calls. | no measure | env | live |
| `SLACK_TOKEN` | `src/alma/config.py` | unset | `get_slack_token`, `slack.client` | Slack bot token. Secret. | Plugin Test button. | env; `.env` | live |
| `SLACK_API_TOKEN` | `src/alma/config.py` | unset | `get_slack_token` | Alias of `SLACK_TOKEN`. Secret. | no measure | env | live |
| `SLACK_CHANNEL` | `src/alma/config.py` | unset | `get_slack_channel`, `slack.client` | Alert channel, wins over the setting. | Alert history. | env; `.env` | live |
| `SLACK_DEFAULT_CHANNEL` | `src/alma/config.py` | unset | `get_slack_channel` | Alias of `SLACK_CHANNEL`. | no measure | env | live |
| `SMTP_HOST` | `src/alma/addons/alerts/mailer.py` | unset | `get_smtp_host` | Mail server, wins over the setting. | Alert history. | env | live |
| `SMTP_PORT` | `src/alma/addons/alerts/mailer.py` | `587` | `get_smtp_port` | Mail port. | Alert history. | env | live |
| `SMTP_USERNAME` | `src/alma/addons/alerts/mailer.py` | unset | `get_smtp_username` | Mail login. | Alert history. | env | live |
| `SMTP_PASSWORD` | `src/alma/addons/alerts/mailer.py` | unset | `get_smtp_password` | Mail password. Secret. | Alert history. | env | live |
| `SMTP_FROM` | `src/alma/addons/alerts/mailer.py` | unset | `get_smtp_from` | Sender address. | Alert history. | env | live |
| `SMTP_TO` | `src/alma/addons/alerts/mailer.py` | unset | `get_smtp_recipients` | Recipients. | Alert history. | env | live |

### Deployment files

Read by Docker Compose or the image build, not by Python.

<!-- inventory: deploy -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `ALMA_IMAGE_TAG` | `docker-compose.yml` | `stable` | Compose `image` | Image tag. | no measure | `.env` | live |
| `BIND_ADDR` | `docker-compose.yml` | `127.0.0.1` | Compose `ports` | Host address of the port. ALMa has no login, keep it private. | no measure | `.env` | live |
| `ALMA_CPUS` | `docker-compose.yml` | `0` | Compose `deploy.resources` | CPU ceiling. 0 means none. | Host load during a refresh. | `.env` | live |
| `ALMA_MEMORY` | `docker-compose.yml` | `4G` | Compose `deploy.resources` | Memory ceiling. | Container restarts while embedding. | `.env` | live |
| `ALMA_VARIANT` | `docker-compose.build.yml` | `normal` | Compose build arg | Image variant for a local build. | no measure | `.env` | live |
| `ALMA_TORCH_VARIANT` | `docker-compose.build.yml` | `cpu` | Compose build arg | Torch build for a local build. | no measure | `.env` | live |
| `VARIANT` | `Dockerfile` | `normal` | Build stage | Same, seen by the Dockerfile. | no measure | build arg | live |
| `TORCH_VARIANT` | `Dockerfile` | `cpu` | Build stage | Same, seen by the Dockerfile. | no measure | build arg | live |
| `NUMBA_DISABLE_JITCACHE` | `docker-compose.yml` | `1` | numba | Avoids JIT cache trouble on the container filesystem. | no measure | compose | live |
| `NUMBA_CACHE_DIR` | `Dockerfile` | `/app/data/.numba-cache` | numba | Cache folder on the data volume. | no measure | Dockerfile | live |
| `PYTHONDONTWRITEBYTECODE` | `docker-compose.yml` | `1` | Python | No bytecode files on a read-only image. | no measure | compose; Dockerfile | live |
| `PYTHONUNBUFFERED` | `docker-compose.yml` | `1` | Python | Unbuffered logs. | no measure | compose; Dockerfile | live |
| `HF_HOME` | `Dockerfile` | `/app/data/.hf-cache` | Hugging Face libraries | Model cache on the data volume. | no measure | Dockerfile | live |
| `HF_HUB_CACHE` | `Dockerfile` | `/app/data/.hf-cache` | Hugging Face libraries | Same cache, under the name newer versions read. | no measure | Dockerfile | live |
| `TRANSFORMERS_CACHE` | `Dockerfile` | `/app/data/.hf-cache` | Hugging Face libraries | Same cache, under the older name. | no measure | Dockerfile | live |
| `ENVIRONMENT` | `Dockerfile` | `production` | Nothing in `src/` | Set in the image, read by nobody. | no measure | Dockerfile | no-op |
| `PYTHONHASHSEED` | `Dockerfile` | `random` | Python | Hash randomization. | no measure | Dockerfile | live |
| `PYTHONPYCACHEPREFIX` | `Dockerfile` | `/tmp/pycache` | Python | Moves bytecode off the read-only image. | no measure | Dockerfile | live |
| `PIP_DISABLE_PIP_VERSION_CHECK` | `Dockerfile` | `1` | pip, build stage | Quieter build. | no measure | Dockerfile | live |
| `PIP_NO_CACHE_DIR` | `Dockerfile` | `1` | pip, build stage | Smaller image. | no measure | Dockerfile | live |

### Platform and test

Set by the platform or the test runner. Nobody tunes these.

<!-- inventory: env -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `PATH` | `src/alma/application/pdfs/sandbox.py` | platform | `_environment` | Passed to the PDF sandbox, which gets a minimal environment. | no measure | env | live |
| `SYSTEMROOT` | `src/alma/application/pdfs/sandbox.py` | platform | `_environment` | The same on Windows. | no measure | env | live |
| `PYTEST_CURRENT_TEST` | `src/alma/api/scheduler.py` | set by pytest | `schedule_immediate`, `library`, `fetcher` | Runs immediate jobs inline under pytest. | no measure | test runner | live |
| `ALMA_TEST_DEFER_JOBS` | `src/alma/api/scheduler.py` | unset | `schedule_immediate` | Leaves jobs queued under pytest. | no measure | test runner | live |

## Scheduled jobs

Registered in `setup_scheduler`. Network jobs also pass `scheduled_network_refusal`: the profile must allow it, the network must be on, and the provider must hold the reserve.

<!-- inventory: job -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `health_snapshot_periodic` | `src/alma/api/scheduler.py` | every 5 min | `rebuild_health_snapshots` | Rebuilds stored Health payloads. | Staleness on Health. | nowhere | live |
| `reap_orphan_jobs` | `src/alma/api/scheduler.py` | every `_ORPHAN_REAP_INTERVAL_MINUTES` | `reap_orphan_jobs` | Closes Activity rows whose worker died. | Ghost rows in Activity. | nowhere | live |
| `evaluate_alerts` | `src/alma/addons/alerts/__init__.py` | hourly, `ALERT_CHECK_INTERVAL_HOURS` | `evaluate_scheduled_alerts` | Checks non-manual alerts. | Alert history lateness. | env | live |
| `refresh_author_suggestions` | `src/alma/api/scheduler.py` | 6 h, `ALMA_AUTHOR_SUGGESTION_REFRESH_INTERVAL_HOURS` | `refresh_author_suggestions_periodic` | Refreshes stale suggestion caches while idle. | Job result; Quota readout. | env | live |
| `refresh_recommendations` | `src/alma/api/scheduler.py` | off, 6 h when on | `refresh_recommendations_periodic` | Refreshes active lenses. | Home Connections rail. | Discovery page; Settings | live |
| `refresh_feed_inbox` | `src/alma/api/scheduler.py` | off, 6 h when on | `refresh_feed_inbox_periodic` | Refreshes the Feed. | Feed refresh result. | Feed page | live |
| `inbox_capture_sweep` | `src/alma/api/scheduler.py` | every 5 min, `INBOX_SWEEP_INTERVAL_MINUTES` | `inbox_capture_sweep_periodic` | Polls capture channels. | Inbox ledger. | env | live |
| `scoring_calibration_refresh` | `src/alma/api/scheduler.py` | 12 h | `scoring_calibration_refresh_periodic` | Rebuilds scoring calibration when inputs moved, and asks for the outcome evaluation. | `calibration` in a score explanation. | nowhere | live |
| `maintain_citation_graph` | `src/alma/api/scheduler.py` | 24 h, `schedule.graph_maintenance_interval_hours` | `maintain_citation_graph_periodic` | Backfills 500 references, then runs the frontier passes. | Job result. | Settings | live |
| `db_maintenance` | `src/alma/api/scheduler.py` | cron 04:30 UTC | `db_maintenance_periodic` | Vacuum and log retention. | Database size. | nowhere | live |
| `maintenance_repair` | `src/alma/api/scheduler.py` | hourly, `ALMA_IDLE_MAINTENANCE_INTERVAL_HOURS` | `maintenance_repair_periodic` | The idle healer. | Health page. | env; Health page | live |
| `hydration_drain` | `src/alma/api/scheduler.py` | every 15 min, `ALMA_HYDRATION_DRAIN_INTERVAL_MINUTES` | `drain_pending_hydration_periodic` | Resumes hydration work left pending. | Health page. | env | live |

## Hard-coded constants

### Ranking

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `TEXT_SIMILARITY_SEMANTIC_SHARE` | `src/alma/discovery/defaults.py` | `0.87` | `ranker.FAMILY_SPECS` | Split of the one similarity slider between Semantic and Lexical. | Outcome evaluation `fit`. | nowhere | live |
| `_MODE_MULTIPLIERS` | `src/alma/application/discovery/ranker.py` | `{'explore': {'recency': 1.5, 'citation': 0.5, 'author': 0.5, 'venue': 0.5}, 'exploit': {'author': 1.5, 'venue': 1.5, 'preference': 1.5, 'recency': 0.5}, 'balanced': {}}` | `resolve_family_weights` | What Explore and Exploit do to family weights. | Outcome evaluation per mode. | nowhere | live |
| `_RETRACTION_PENALTY` | `src/alma/application/discovery/ranker.py` | `0.45` | `repaired_prior_score` | Points taken off a retracted paper, as a share of 100. | no measure | nowhere | live |
| `_PROJECTED_FEEDBACK_CAP` | `src/alma/discovery/scoring.py` | `0.6` | `measure_candidate` | Bound on the structured feedback adjustment. | Signal impact report. | nowhere | live |
| `_FEEDBACK_SEMANTIC_SHARE` | `src/alma/discovery/scoring.py` | `0.6` | `measure_candidate` | Split of Feedback between the semantic and structured estimates. | Signal impact report. | nowhere | live |
| `TITLE_FALLBACK_TOPIC_SCORE` | `src/alma/discovery/scoring.py` | `0.3` | `resolve_candidate_topics` | Weight of title words when a paper has no topics. | no measure | nowhere | live |
| `TOPIC_SIGNAL_MIN_SCORE` | `src/alma/core/topics.py` | `0.2` | `topic_confidence_sql`, `topic_signal_relevance` | Weakest topic row allowed to affect ranking. | no measure | nowhere | live |
| `DEFAULT_TOPIC_SCORE` | `src/alma/core/topics.py` | `0.5` | `compute_preference_profile`, `topic_relevance` | Relevance of a topic row with no stored score. | no measure | nowhere | live |
| `RRF_K` | `src/alma/core/scoring_math.py` | `60` | `rrf_weight`, `rrf_score_normalized` | Rank fusion smoothing. Higher flattens the gap between ranks. | no measure | nowhere | live |
| `MIN_INTERACTIONS_FOR_SCORING` | `src/alma/services/feedback_substrate.py` | `2` | `preload_preference_profile_maps`, `get_preference_affinity_signal` | Interactions on a topic, author or venue before its preference counts. | Signal impact report. | nowhere | live |
| `DEFAULT_DECAY_HALF_LIFE_DAYS` | `src/alma/services/feedback_substrate.py` | `30` | `_apply_decay` | Half-life of preference affinity. | Signal impact report `tuning` drift. | nowhere | live |

<!-- inventory: inline -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `FAMILY_SPECS` atoms and `prior_mean` | `src/alma/application/discovery/ranker.py` | see source | `_family_reading`, `repaired_prior_score` | Inside-family atom weights, and the value imputed for an unmeasured family. `calibration.prior` replaces the priors once built. The script the comment names, `scripts/measure_ranking_priors.py`, is not in the repo. | Outcome evaluation for atoms. | nowhere | live |
| `measure_candidate` projected axes | `src/alma/discovery/scoring.py` | paper 0.65, neighbours 0.55 and 0.45, venue 0.35, topic 0.45, keyword 0.30, tag 0.30, author 0.40, author name 0.30 | `measure_candidate` | How much each kind of past feedback counts toward a candidate. | Signal impact report. | nowhere | live |
| `measure_candidate` text blend | `src/alma/discovery/scoring.py` | semantic 0.68 and lexical 0.32, adjusted within 0.56 to 0.80 | `measure_candidate` | Blend inside the recorded `text_similarity` signal. The score reads the atoms instead. | no measure | nowhere | live |
| `compute_preference_profile` background priors | `src/alma/discovery/scoring.py` | topic 0.22, venue 0.18, collection 0.5 | `compute_preference_profile` | How much the monitored corpus and collections feed the taste profile. | no measure | nowhere | live |

### Slate

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `EXPLORATION_SLOTS` | `src/alma/application/discovery/exploration.py` | `4` | `build_slate` | Cards per deck drawn uniformly from the tail. This is the randomized evidence learning relies on. | Refresh summary `exploration_count`, `exploration_pool_size`; Signal impact report sample size. | nowhere | live |
| `BRANCH_FLOOR_SLOTS` | `src/alma/application/discovery/exploration.py` | `2` | `_floor_deficit` | Cards each active branch keeps in a deck. | Refresh summary `branch_floor_count`; Branch panel coverage. | nowhere | live |
| `FIRST_SCREEN_CARDS` | `src/alma/application/discovery/exploration.py` | `20` | `build_slate` | Each branch keeps a card within this many. Pinned to `DEFAULT_VISIBLE_RECS`. | Refresh summary `first_screen_short`. | nowhere | live |

<!-- inventory: inline -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_mmr(diversity)` | `src/alma/application/discovery/exploration.py` | 0.24 | `_exploit_with_floor` | Trade-off between score and similarity to cards already picked. Higher gives a more varied deck. | Refresh summary `final_mix`: `max_author_count`, `max_venue_count`, `max_topic_count`. | nowhere | live |
| `_safety_caps` | `src/alma/application/discovery/exploration.py` | author 16%, venue 22%, topic 30% of the deck, at least 3, 4, 5 | `_would_exceed_caps` | Most cards one author, venue or topic may take. Floor cards ignore them. | Refresh summary `safety_caps`, `safety_filtered_count`. | nowhere | live |
| `_refresh_lens_recommendations` staging limit | `src/alma/application/discovery/__init__.py` | `max(200, 4 * limit)` | `_refresh_lens_recommendations` | Candidates kept for the slate and the counterfactual record. | Refresh summary `staged_pool`. | nowhere | live |

### Retrieval and frontier

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `DEFAULT_CHANNEL_WEIGHTS` | `src/alma/application/discovery/lens_crud.py` | see source | `default_channel_weights` | Starting lens channel weights by context. They move no score: see `weights` under per-lens controls. | no measure | nowhere | no-op |
| `_MAX_TOPIC_QUERIES` | `src/alma/application/discovery/retrieval/lexical.py` | `8` | `_lexical_query_terms` | Lexical searches per refresh. | Refresh summary. | nowhere | live |
| `_PER_QUERY_LIMIT` | `src/alma/application/discovery/retrieval/lexical.py` | `25` | `_retrieve_lexical_channel` | Candidates per lexical search. | Refresh summary. | nowhere | live |
| `_DIRECTION_BLEND_WEIGHT` | `src/alma/application/discovery/retrieval/vector.py` | `{'pin': 0.5, 'boost': 0.3}` | `_direction_centroids` | Pull of an adopted direction. | no measure | nowhere | live |
| `_PROBE_SHARE_DIVISOR` | `src/alma/application/discovery/retrieval/vector.py` | `4` | `_retrieve_vector_channel`, `_share_the_cut` | Share of a normal branch a probing branch gets. | Branch panel: whether auto-muted branches recover. | nowhere | live |
| `DEFAULT_BUILD_LIMIT` | `src/alma/application/discovery/frontier.py` | `1500` | `build_frontier`, `fill_frontier_vectors` | Frontier rows gathered and vectorized per pass. | Refresh summary `frontier_size`. | nowhere | live |
| `DEFAULT_EXPIRY_DAYS` | `src/alma/application/discovery/frontier.py` | `30` | `upsert_frontier_candidates` | How long a lead stays usable. | Refresh summary `frontier_size`. | nowhere | live |
| `_DISCOVERY_QUERY_LIMIT` | `src/alma/application/discovery/frontier.py` | `6` | `build_discovery_frontier` | Library topics searched per pass, from a rotating window. | Job result `frontier_topics` lap; Quota readout. | nowhere | live |
| `_DISCOVERY_RESULTS_PER_QUERY` | `src/alma/application/discovery/frontier.py` | `40` | `build_discovery_frontier` | Results per topic search. | Job result. | nowhere | live |
| `_S2_RELATED_SEEDS` | `src/alma/application/discovery/frontier.py` | `6` | `build_discovery_frontier` | Papers whose Semantic Scholar neighbours are fetched per pass. | Job result rotation lap. | nowhere | live |
| `_OPENALEX_CITING_SEEDS` | `src/alma/application/discovery/frontier.py` | `4` | `build_discovery_frontier` | Works whose citers are fetched per pass. | Job result rotation lap; Quota readout. | nowhere | live |
| `RECOMMEND_MAX_SEEDS` | `src/alma/discovery/semantic_scholar.py` | `100` | `recommend_from_seeds`, `_s2_seed_windows` | Provider limit on seeds per list. Sets rotation laps. | Job result rotation lap. | nowhere | live |
| `DEFAULT_LANE_DEADLINE_S` | `src/alma/discovery/source_search.py` | `8.0` | `search_across_sources`, `stream_across_sources` | Seconds a multi-source search waits for its sources. Feed monitors use this. | no measure | nowhere | live |
| `FINDADD_LANE_DEADLINE_S` | `src/alma/discovery/source_search.py` | `15.0` | `stream_online_sources` | The same for Find & Add. | Source chips showing timeout. | nowhere | live |

<!-- inventory: inline -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_retrieve_external_channel` lane budgets | `src/alma/application/discovery/retrieval/external.py` | 4 topics, 3 recent wins, 4 authors, 3 venues, at least 8 hits each, venue weight at least 0.14 | `_retrieve_external_channel` | How many taste searches a refresh runs over the frontier. | Refresh summary `lane_runs`. | nowhere | live |
| `_merge_channel_candidates` hits per retriever | `src/alma/application/discovery/retrieval/merge.py` | 4 | `_merge_channel_candidates` | How many hits of one retriever count, so a fan-out cannot win by asking more. | no measure | nowhere | live |
| `_retrieve_graph_channel` pools | `src/alma/application/discovery/retrieval/graph.py` | `max(limit * 2, 50)`, PPR `max(limit * 3, 100)` | `_retrieve_graph_channel` | Candidates per citation lane. | Refresh summary. | nowhere | live |
| `build_discovery_frontier` request | `src/alma/application/discovery/frontier.py` | last 6 years, temperature 0.28, deadline 20 s | `build_discovery_frontier` | Age and shape of frontier searches. | Job result. | nowhere | live |
| `_refresh_lens_recommendations` exemplars | `src/alma/application/discovery/__init__.py` | 12 positive, 8 negative | `_refresh_lens_recommendations` | Representative papers compared to each candidate. | no measure | nowhere | live |
| `search_across_sources` per-source limit | `src/alma/discovery/source_search.py` | `max(3, min(25, limit))` | `search_across_sources` | Results asked of each source. | Feed refresh result. | nowhere | live |

### Branches

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `BRANCH_RESOLUTION_DEFAULT` | `src/alma/application/discovery/seed_profile.py` | `1.0` | `_resolve_branch_resolution` | Cluster detail with no override. | Branch panel. | nowhere | live |
| `BRANCH_RESOLUTION_MIN` | `src/alma/application/discovery/seed_profile.py` | `0.5` | `_resolve_branch_resolution` | Lowest cluster detail. | no measure | nowhere | live |
| `BRANCH_RESOLUTION_MAX` | `src/alma/application/discovery/seed_profile.py` | `3.0` | `_resolve_branch_resolution` | Highest cluster detail. | no measure | nowhere | live |
| `TURNOVER_REBUILD` | `src/alma/application/discovery/branch_identity.py` | `1.0` | `rebuild_reason` | Turnover at which a refresh reclusters by itself. | Branch panel `turnover`. | nowhere | live |
| `TURNOVER_RECOMMEND` | `src/alma/application/discovery/branch_identity.py` | `0.5` | `_measure_drift` | Turnover at which the panel recommends a rebuild. | Branch panel. | nowhere | live |
| `MISFIT_RECOMMEND` | `src/alma/application/discovery/branch_identity.py` | `0.03` | `_measure_drift` | Share of ill-fitting joiners at which the panel recommends a rebuild. | Branch panel `misfit_share`. | nowhere | live |
| `FIT_FLOOR_QUANTILE` | `src/alma/application/discovery/branch_identity.py` | `0.1` | `_measure_drift` | How poorly a joiner must fit to count as a misfit. | Branch panel. | nowhere | live |
| `_MIN_SEED_OVERLAP` | `src/alma/application/discovery/branch_identity.py` | `0.7` | `resolve_branch_identities` | Overlap a rebuilt branch needs to keep an old identity and its learning. | Branch panel: identities lost at rebuild. | nowhere | live |
| `_MIN_BEST_MARGIN` | `src/alma/application/discovery/branch_identity.py` | `0.1` | `resolve_branch_identities` | Lead the best match needs over the next. | Branch panel. | nowhere | live |
| `_SUCCESSOR_SHARE` | `src/alma/application/discovery/branch_identity.py` | `0.1` | `plan_branch_lineage` | Share of an old branch that makes a new one its successor. | no measure | nowhere | live |
| `_CONTROL_MAJORITY` | `src/alma/application/discovery/branch_identity.py` | `0.5` | `plan_branch_lineage` | Pins and mutes follow papers only into a branch holding this share. | no measure | nowhere | live |
| `_AUTO_WEIGHT_PRIOR_STRENGTH` | `src/alma/application/discovery/lens_crud.py` | `6.0` | `_compute_branch_auto_weight` | Pseudo-verdicts before a branch's weight moves. | Branch panel `auto_weight_reason`. | nowhere | live |
| `_AUTO_WEIGHT_FLOOR` | `src/alma/application/discovery/lens_crud.py` | `0.3` | `_compute_branch_auto_weight` | Lowest branch auto-weight. | Branch panel. | nowhere | live |
| `_AUTO_WEIGHT_CEIL` | `src/alma/application/discovery/lens_crud.py` | `1.8` | `_compute_branch_auto_weight` | Highest branch auto-weight. It has no effect above 1: the weight only drives the rotate and mute thresholds, no budget reads it. | no measure | nowhere | live |
| `_AUTO_WEIGHT_HALF_LIFE_DAYS` | `src/alma/application/discovery/lens_crud.py` | `30.0` | `_aggregate_branch_outcomes` through `_decay_factor` | How fast a branch's old verdicts fade. | Branch panel. | nowhere | live |
| `_AUTO_WEIGHT_ROTATE_THRESHOLD` | `src/alma/application/discovery/lens_crud.py` | `0.65` | `_apply_branch_auto_lifecycle` | Weight at which a branch swaps core and explore topics. | Branch panel: rotations that do not recover mean raise. | nowhere | live |
| `_AUTO_WEIGHT_MUTE_THRESHOLD` | `src/alma/application/discovery/lens_crud.py` | `0.55` | `_apply_branch_auto_lifecycle` | Weight at which a branch is auto-muted and only probes. | Branch panel. | nowhere | live |

<!-- inventory: inline -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_describe_seed_branches` topic counts | `src/alma/application/discovery/seed_profile.py` | core `4 - 2.4 t` within 2 to 4, explore `1 + 4 t` within 1 to 5, far neighbour at `t >= 0.5` | `_describe_seed_branches` | How temperature turns into topics. | Branch panel. | nowhere | live |
| `_plan_branch_queries_deterministic` temperature cut | `src/alma/application/discovery/seed_profile.py` | 0.4 | `_plan_branch_queries_deterministic` | Word added when a branch has no explore topic. | no measure | nowhere | live |
| `_resolve_branch_temperature` mode fallback | `src/alma/application/discovery/seed_profile.py` | exploit 0.12, balanced 0.28, explore 0.55 | `_resolve_branch_temperature` | Dead on the live path, because `branches.temperature` is always present. | no measure | nowhere | no-op |
| `lens_branch_cap` ceiling | `src/alma/application/discovery/seed_profile.py` | 6 | `lens_branch_cap` | Hard ceiling over `branches.max_clusters`. | Branch panel. | nowhere | live |
| `_compute_branch_auto_weight` new-branch lift | `src/alma/application/discovery/lens_crud.py` | 1.15 | `_compute_branch_auto_weight` | Starting weight of a branch with no verdicts. | no measure | nowhere | live |
| `resolve_lens_branches` outcome window | `src/alma/application/discovery/seed_profile.py` | 60 days | `resolve_lens_branches` | Verdicts counted toward a branch. | Branch panel. | nowhere | live |

### Learning from verdicts

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_EVENT_HALF_LIFE_DAYS` | `src/alma/application/signal_projection.py` | `180.0` | `compute_paper_signal_map`, `load_projected_paper_signals` | Half-life of feedback. The real decay behind the two no-op decay settings. | Signal impact report `tuning` drift. | nowhere | live |
| `_EVENT_MAX_AGE_DAYS` | `src/alma/application/signal_projection.py` | `730.0` | `compute_paper_signal_map`, `load_projected_paper_signals` | Oldest feedback counted. | Signal impact report. | nowhere | live |
| `_RATING_SIGNAL_WEIGHT` | `src/alma/application/signal_projection.py` | `0.6` | `_add_rating_signals` | Weight of a rating among verdicts. | Signal impact report. | nowhere | live |
| `_RECOMMENDATION_SIGNAL_WEIGHT` | `src/alma/application/signal_projection.py` | `0.5` | `_add_recommendation_signals` | Weight of a recommendation action. | Signal impact report. | nowhere | live |
| `_POSITION_WEIGHTS` | `src/alma/application/signal_projection.py` | `{'first': 1.25, 'last': 1.25, 'corresponding': 1.35}` | `_project_authors` | Extra credit to lead authors. | no measure | nowhere | live |
| `ACTION_RATINGS` | `src/alma/application/paper_actions.py` | `{'add': 3, 'like': 4, 'love': 5, 'dislike': 1}` | `apply_valence` | Rating each action writes. | no measure | nowhere | live |
| `_PAPER_DISMISS_SIGNAL_SOFT` | `src/alma/application/discovery/lens_crud.py` | `-0.38` | `_paper_dismissal_scores` | Suppression of one dismissal. | no measure | nowhere | live |
| `_PAPER_DISMISS_SIGNAL_HARD` | `src/alma/application/discovery/lens_crud.py` | `-0.82` | `_paper_dismissal_scores` | Extra suppression after repeated dismissals. | no measure | nowhere | live |
| `_PAPER_DISMISS_DECAY_HALF_LIFE_DAYS` | `src/alma/application/discovery/lens_crud.py` | `180.0` | `_paper_dismissal_scores` | How long one dismissal hides a paper. | no measure | nowhere | live |
| `_PAPER_DISMISS_HARD_HALF_LIFE_DAYS` | `src/alma/application/discovery/lens_crud.py` | `365.0` | `_paper_dismissal_scores` | How long repeated dismissals hide it. | no measure | nowhere | live |
| `_PAPER_DISMISS_HARD_THRESHOLD` | `src/alma/application/discovery/lens_crud.py` | `3` | `_paper_dismissal_scores` | Dismissals that count as repeated. | no measure | nowhere | live |
| `_PAPER_DISMISS_SUPPRESSION_THRESHOLD` | `src/alma/application/discovery/lens_crud.py` | `-0.18` | `_refresh_lens_recommendations` | Suppression below which a paper stays hidden. | no measure | nowhere | live |
| `_NEUTRAL_DEADBAND` | `src/alma/application/recommendation_outcomes.py` | `0.05` | `_classify` | Net signal counted as no verdict. | no measure | nowhere | live |
| `_LIFECYCLE_POSITIVE` | `src/alma/application/recommendation_outcomes.py` | `0.5` | `build_paper_outcome_map` | Credit of a saved paper. | no measure | nowhere | live |
| `_LIFECYCLE_NEGATIVE` | `src/alma/application/recommendation_outcomes.py` | `-0.5` | `build_paper_outcome_map` | Debit of a removed paper. | no measure | nowhere | live |
| `_PRIOR_ALPHA` | `src/alma/application/outcome_calibration.py` | `2.0` | `_finalize_calibration` | Positive prior of a suggestion bucket. | Suggestion rows. | nowhere | live |
| `_PRIOR_BETA` | `src/alma/application/outcome_calibration.py` | `2.0` | `_finalize_calibration` | Negative prior of a suggestion bucket. | Suggestion rows. | nowhere | live |
| `_DEFAULT_WINDOW_DAYS` | `src/alma/application/outcome_calibration.py` | `180.0` | `compute_author_bucket_calibration` | Outcome window. | Suggestion rows. | nowhere | live |
| `_DEFAULT_HALF_LIFE_DAYS` | `src/alma/application/outcome_calibration.py` | `60.0` | `compute_author_bucket_calibration` | Outcome half-life. | Suggestion rows. | nowhere | live |
| `_DEFAULT_MULTIPLIER_LO` | `src/alma/application/outcome_calibration.py` | `0.5` | `compute_author_bucket_calibration` | Lowest bucket multiplier. | Suggestion rows. | nowhere | live |
| `_DEFAULT_MULTIPLIER_HI` | `src/alma/application/outcome_calibration.py` | `1.5` | `compute_author_bucket_calibration` | Highest bucket multiplier. | Suggestion rows. | nowhere | live |

### Calibration and evaluation

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `SAMPLE_SIZE` | `src/alma/application/discovery/calibration.py` | `600` | `build_scoring_calibration` | Corpus papers scored to build percentile tables. | no measure | nowhere | live |
| `_MIN_LIBRARY` | `src/alma/application/discovery/calibration.py` | `3` | `build_scoring_calibration` | Library size before calibration. | no measure | nowhere | live |
| `_MIN_SAMPLE` | `src/alma/application/discovery/calibration.py` | `30` | `build_scoring_calibration` | Corpus size before calibration. | no measure | nowhere | live |
| `_QUANTILES` | `src/alma/application/discovery/calibration.py` | `(0.5, 1, 5, 10, 25, 50, 75, 90, 95, 99, 99.5, 99.9)` | `quantile_table` | Grid of the percentile tables. | no measure | nowhere | live |
| `MIN_PROFILE` | `src/alma/application/discovery/outcome_eval.py` | `20` | `evaluate_ranker_outcomes` | Profile papers before the evaluation runs. | Outcome evaluation `not_ready`. | nowhere | live |
| `MIN_GROUP` | `src/alma/application/discovery/outcome_eval.py` | `15` | `auc_with_interval`, `fit_family_weights` | Papers per side before a result is stated. | Outcome evaluation `inconclusive`. | nowhere | live |
| `RANDOM_SAMPLE` | `src/alma/application/discovery/outcome_eval.py` | `600` | `evaluate_ranker_outcomes` | Random corpus papers in the evaluation. | Width of the intervals. | nowhere | live |
| `BOOTSTRAP_DRAWS` | `src/alma/application/discovery/outcome_eval.py` | `1000` | `auc_with_interval` | Bootstrap draws. | no measure | nowhere | live |
| `FIT_FOLDS` | `src/alma/application/discovery/outcome_eval.py` | `5` | `fit_family_weights` | Cross-validation folds. | no measure | nowhere | live |
| `FIT_L2` | `src/alma/application/discovery/outcome_eval.py` | `(0.001, 0.01, 0.1, 1.0)` | `fit_family_weights` | Regularization grid. | no measure | nowhere | live |
| `MIN_ROWS_TO_FIT` | `src/alma/application/discovery/signal_tuning.py` | `30` | `_fit` | Rows before partial coefficients are fitted. | Signal impact report. | nowhere | live |
| `MIN_PER_CLASS` | `src/alma/application/discovery/signal_tuning.py` | `8` | `_fit` | The same per class. | Signal impact report. | nowhere | live |
| `RIDGE_STRENGTH` | `src/alma/application/discovery/signal_tuning.py` | `1.0` | `_fit` | L2 penalty of the tuning fit. | no measure | nowhere | live |
| `EVENTS_PER_VARIABLE_FLOOR` | `src/alma/application/discovery/signal_tuning.py` | `10.0` | `signal_tuning` | Events needed per coefficient. | Signal impact report. | nowhere | live |
| `ROLLING_WINDOWS` | `src/alma/application/discovery/signal_tuning.py` | `3` | `_rolling` | Slices of the drift view. | no measure | nowhere | live |
| `MIN_RATE_SAMPLE` | `src/alma/application/diagnostics_stats.py` | `8` | `is_sufficient` | Outcomes before a rate is stated. | no measure | nowhere | live |
| `MIN_GROUP_SAMPLE` | `src/alma/application/diagnostics_stats.py` | `5` | `sufficient`, `signal_impact` | Per-group floor of a comparison. | no measure | nowhere | live |
| `_MIN_TOPIC_SUPPORT` | `src/alma/application/reports.py` | `2` | `topic_drift` | Papers a topic needs to count in drift. | no measure | nowhere | live |
| `_MIN_WINDOW_PAPERS` | `src/alma/application/reports.py` | `5` | `topic_drift` | Papers a window needs. | no measure | nowhere | live |
| `_MIN_DRIFT_EFFECT` | `src/alma/application/reports.py` | `0.05` | `topic_drift` | Smallest change called drift. | no measure | nowhere | live |

### Authors

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_WEIGHTS` | `src/alma/application/author_signal.py` | `{'library': 0.3, 'rating': 0.2, 'interaction': 0.2, 'similarity': 0.2, 'neighborhood': 0.1}` | `_make_component` | Mix of the canonical author signal shown on author cards. | no measure | nowhere | live |
| `_AUTHOR_SIMILARITY_FLOOR` | `src/alma/application/author_signal.py` | `0.35` | `_scale_author_similarity` | Cosine read as zero similarity for an author. | no measure | nowhere | live |
| `_NEIGHBORHOOD_COAUTHOR_SCALE` | `src/alma/application/author_signal.py` | `6.0` | `_load_neighborhood` | Saturation of shared co-authorship. | no measure | nowhere | live |
| `_NEIGHBORHOOD_CITED_SCALE` | `src/alma/application/author_signal.py` | `4.0` | `_load_neighborhood` | Saturation of shared citation. | no measure | nowhere | live |
| `_SEED_AUTHOR_SIGNAL_MIN` | `src/alma/application/author_network.py` | `0.45` | `_rank_seed_authors` | Paper signal that makes its authors strong seeds. | no measure | nowhere | live |
| `_MAX_SEED_AUTHORS` | `src/alma/application/author_network.py` | `20` | `_select_seed_authors` | Seed authors per pass, half fixed and half rotating. | Job result rotation lap; Quota readout. | nowhere | live |
| `_MAX_SEED_DOIS` | `src/alma/application/author_network.py` | `20` | `_select_seed_dois` | Seed papers per pass. | Job result rotation lap. | nowhere | live |
| `_MAX_WORKS_PER_SEED` | `src/alma/application/author_network.py` | `25` | `refresh_openalex_related_network` | Works read per seed author. | Suggestion rows. | nowhere | live |
| `_MAX_RECS_PER_DOI` | `src/alma/application/author_network.py` | `25` | `refresh_s2_related_network` | Recommendations read per seed paper. | Suggestion rows. | nowhere | live |
| `_MAX_STORED_CANDIDATES` | `src/alma/application/author_network.py` | `2000` | `_accumulate_candidates` | Candidates kept in the cache row. | no measure | nowhere | live |
| `_CONSENSUS_BONUS_FRACTION` | `src/alma/application/authors.py` | `0.12` | `list_author_suggestions` | Bonus for appearing in several buckets. | Suggestion rows. | nowhere | live |
| `_PROJECTED_AUTHOR_DIRECT_SCALE` | `src/alma/application/authors.py` | `18.0` | `_projected_author_signal_adjustment` | Points from direct author verdicts. | Suggestion rows. | nowhere | live |
| `_PROJECTED_AUTHOR_TOPIC_SCALE` | `src/alma/application/authors.py` | `5.0` | `_projected_author_signal_adjustment` | Points from topic verdicts. | Suggestion rows. | nowhere | live |
| `_PROJECTED_AUTHOR_VENUE_SCALE` | `src/alma/application/authors.py` | `4.0` | `_projected_author_signal_adjustment` | Points from venue verdicts. | Suggestion rows. | nowhere | live |
| `_PROJECTED_AUTHOR_KEYWORD_SCALE` | `src/alma/application/authors.py` | `3.0` | `_projected_author_signal_adjustment` | Points from keyword verdicts. | Suggestion rows. | nowhere | live |
| `_PROJECTED_AUTHOR_TAG_SCALE` | `src/alma/application/authors.py` | `4.0` | `_projected_author_signal_adjustment` | Points from tag verdicts. | Suggestion rows. | nowhere | live |
| `_PROJECTED_AUTHOR_ADJUSTMENT_CAP` | `src/alma/application/authors.py` | `24.0` | `_projected_author_signal_adjustment` | Most points those can move. | no measure | nowhere | live |
| `_DISMISSAL_TOPIC_PENALTY_PER_HIT` | `src/alma/application/authors.py` | `0.02 * _MAX_SUGGESTION_SCORE` | `_dismissal_overlap_penalty` | Penalty per shared topic with a dismissed author. | Suggestion rows. | nowhere | live |
| `_DISMISSAL_VENUE_PENALTY_PER_HIT` | `src/alma/application/authors.py` | `0.015 * _MAX_SUGGESTION_SCORE` | `_dismissal_overlap_penalty` | Per shared venue. | Suggestion rows. | nowhere | live |
| `_DISMISSAL_COAUTHOR_PENALTY_PER_HIT` | `src/alma/application/authors.py` | `0.008 * _MAX_SUGGESTION_SCORE` | `_dismissal_overlap_penalty` | Per shared co-author. | Suggestion rows. | nowhere | live |
| `_DISMISSAL_INSTITUTION_PENALTY_PER_HIT` | `src/alma/application/authors.py` | `0.01 * _MAX_SUGGESTION_SCORE` | `_dismissal_overlap_penalty` | Per shared institution. | Suggestion rows. | nowhere | live |
| `_DISMISSAL_PENALTY_CAP` | `src/alma/application/authors.py` | `0.3 * _MAX_SUGGESTION_SCORE` | `_dismissal_overlap_penalty` | Most those can take off. | Suggestion rows. | nowhere | live |
| `_REMOVE_SIGNAL_SOFT` | `src/alma/application/gap_radar.py` | `-0.38` | `record_missing_author_remove` | Suppression of one author dismissal. | no measure | nowhere | live |
| `_REMOVE_SIGNAL_HARD` | `src/alma/application/gap_radar.py` | `-0.82` | `get_missing_author_feedback_state` | Suppression of repeated dismissals or a deletion. | no measure | nowhere | live |
| `_REMOVE_DECAY_HALF_LIFE_DAYS` | `src/alma/application/gap_radar.py` | `180.0` | `get_missing_author_feedback_state` | How long one dismissal lasts. | no measure | nowhere | live |
| `_HARD_REMOVE_HALF_LIFE_DAYS` | `src/alma/application/gap_radar.py` | `365.0` | `get_missing_author_feedback_state` | How long repeated ones last. | no measure | nowhere | live |
| `_HARD_REMOVE_THRESHOLD` | `src/alma/application/gap_radar.py` | `3` | `get_missing_author_feedback_state` | Dismissals that count as repeated. | no measure | nowhere | live |
| `_SUPPRESSION_THRESHOLD` | `src/alma/application/gap_radar.py` | `-0.18` | `get_missing_author_feedback_state` | Suppression below which an author stays hidden. | no measure | nowhere | live |
| `ACCEPTED_CONFIDENCE` | `src/alma/application/author_identity.py` | `0.7` | `resolve_identity_hierarchical` | Match confidence accepted without review. | Authors needing review on Health. | nowhere | live |
| `SEED_TARGET_PAPERS` | `src/alma/application/author_backfill.py` | `2` | `seed_papers_for_author` | Papers a suggested author needs before a centroid exists. | Health: thin suggested authors. | nowhere | live |
| `SUGGESTION_REVIEW_WINDOW` | `src/alma/application/authors.py` | `30` | `seed_thin_suggestion_authors` | Top suggestions the seed repair looks at. | Health: thin suggested authors. | nowhere | live |
| `_AUTHOR_BACKFILL_STALE_AFTER_DAYS` | `src/alma/application/followed_authors.py` | `120` | `_backfill_status_from_inputs` | Age at which an author counts as stale. | Health backfill counts. | nowhere | live |
| `_AUTHOR_BACKFILL_MIN_EXPECTED` | `src/alma/application/followed_authors.py` | `5` | `_backfill_status_from_inputs` | Fewest works expected in the corpus. | Health backfill counts. | nowhere | live |
| `_AUTHOR_BACKFILL_EXPECTED_RATIO` | `src/alma/application/followed_authors.py` | `0.2` | `_backfill_status_from_inputs` | Share of an author's works expected. | Health backfill counts. | nowhere | live |

### Feed and Home

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `FEED_NEW_LOOKBACK_HOURS` | `src/alma/application/feed.py` | `24` | `feed_new_window` | What counts as new in the Feed the first time. | no measure | nowhere | live |
| `ZERO_YIELD_WARNING_RUNS` | `src/alma/application/feed_monitors.py` | `3` | `_monitor_health`, `feed_refresh_outcome` | Empty runs before a monitor warns. | Feed refresh result. | nowhere | live |
| `FEED_INBOX_DAYS` | `src/alma/application/home.py` | `60` | `home._window` | Feed window of the Home counts. Also the default `since_days` of the Feed, written separately in `feed.py`. | no measure | nowhere | live |
| `RECENT_DAYS` | `src/alma/application/home.py` | `7` | `home._window` | Window of the Home recent counts. | no measure | nowhere | live |
| `HIGHLIGHT_LIMIT` | `src/alma/application/home.py` | `4` | `_select_highlights` | Highlights on Home. | no measure | nowhere | live |
| `READING_PREVIEW_LIMIT` | `src/alma/application/home.py` | `10` | `build_daily_brief` | Reading items loaded for Home. | no measure | nowhere | live |
| `INBOX_PREVIEW_LIMIT` | `src/alma/application/home.py` | `10` | `build_daily_brief` | Inbox items loaded for Home. | no measure | nowhere | live |
| `TREND_DAYS` | `src/alma/application/home.py` | `7` | `_local_days` | Days of the inflow strip. | no measure | nowhere | live |
| `FEED_STALE_AFTER` | `src/alma/application/home_status.py` | `timedelta(days=2)` | `_feed` | Age at which the Feed pill warns. | Home pill. | nowhere | live |
| `LENS_STALE_AFTER` | `src/alma/application/home_status.py` | `timedelta(days=7)` | `_discovery` | Age at which the Discovery pill warns. | Home pill. | nowhere | live |
| `EMBEDDING_READY_PCT` | `src/alma/application/home_status.py` | `80.0` | `_embeddings` | Coverage at which the pill is calm. Written twice, see the next row. | Health embedding coverage. | nowhere | live |
| `EMBEDDINGS_READY_THRESHOLD` | `src/alma/services/health.py` | `80.0` | `assess_corpus`, `embedding_coverage` | Coverage at which Health calls embeddings ready. | Health embedding coverage. | nowhere | live |
| `MAP_GAP_TOLERANCE_PCT` | `src/alma/application/home_status.py` | `5.0` | Nothing | Leftover from the map. | no measure | nowhere | no-op |
| `_QUEUED_STATE_THRESHOLD` | `src/alma/services/health.py` | `0.9` | `assess_corpus` | Share of a gap never attempted for it to read as queued instead of warning. | Health page. | nowhere | live |
| `LOW_CONFIDENCE_IMPORT_TITLE_THRESHOLD` | `src/alma/library/importer.py` | `0.7` | `_should_stage_import` | Title confidence below which an import stays staged. | Imports staged for review. | nowhere | live |
| `DEFAULT_LIBRARY_RATING` | `src/alma/application/library.py` | `3` | `add_to_library` | Rating of a plain save. | no measure | nowhere | live |
| `ALERT_HISTORY_RETENTION_DAYS` | `src/alma/addons/alerts/service.py` | `180` | `prune_alert_history` | How long alert history stays. Floor 90. | no measure | nowhere | live |
| `_FAILURE_WINDOW_DAYS` | `src/alma/addons/alerts/surfaces.py` | `30` | `delivery_check` | How far back a failed digest run still reads as a current problem on Health. | Alert history. | nowhere | live |
| `_MAX_PAPERS_PER_EMAIL` | `src/alma/addons/alerts/mailer.py` | `50` | email digest sender in `alerts.mailer` | Papers per email. | no measure | nowhere | live |
| `_MAX_PAPERS_PER_MESSAGE` | `src/alma/slack/client.py` | `15` | Slack digest sender in `slack.client` | Papers per Slack message. | no measure | nowhere | live |
| `FETCH_LIMIT` | `src/alma/services/inbox_channels/slack.py` | `100` | `fetch` | Messages read per sweep. The rest waits for the next. | Inbox ledger. | nowhere | live |

<!-- inventory: inline -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_assess_gap` tiers (`_IMPACT_TIERS`) | `src/alma/services/health.py` | integrity 0 and 2%, high 5 and 25%, medium 15 and 50%, low 40 and 80% | `_assess_gap` | Share of papers at which a gap warns and turns critical. | Health page: a page that is never quiet means raise. | nowhere | live |

### Budgets, retries and rate limits

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `RESERVED_USER_CALLS` | `src/alma/core/http_sources.py` | `200` | `background_settings`, `provider_budget_ok` | Default of `maintenance.reserved_api_calls`. | Quota readout. | Settings: Background Operations | live |
| `IDLE_THRESHOLD_SECONDS` | `src/alma/core/user_activity.py` | `180.0` | `app_is_idle`, `background_settings` | Default of `maintenance.idle_wait_minutes`. | Job result. | Settings: Background Operations | live |
| `MAX_RETRY_AFTER_SECONDS` | `src/alma/core/http_sources.py` | `60.0` | `_retry_wait` | Longest honoured provider wait. | Job result. | nowhere | live |
| `_TITLE_SEARCH_BUDGET_RESERVE` | `src/alma/core/resolution.py` | `100` | `resolve_paper_openalex_work` | Credits at which title lookup switches to Semantic Scholar first. | Quota readout. | nowhere | live |
| `_SINGLETON_FALLBACK_RESERVE` | `src/alma/openalex/client.py` | `25` | `batch_fetch_works_by_dois`, `batch_fetch_works_by_openalex_ids` | Credits at which batch fetches switch to the free path. | Quota readout. | nowhere | live |
| `SEARCH_COST_CREDITS` | `src/alma/openalex/http.py` | `10` | `provider_can_start`, `forecast_operation_quota` | Credits of one search. Mirrors OpenAlex pricing. | Quota readout. | nowhere | live |
| `CONTENT_COST_CREDITS` | `src/alma/openalex/http.py` | `100` | `_request_with_retry` | Credits of one cached PDF. Mirrors OpenAlex pricing. | Quota readout. | nowhere | live |
| `_MAX_RETRIES` | `src/alma/openalex/http.py` | `5` | `_request_with_retry` | Retries of an OpenAlex call. | Quota readout `retry_count`. | nowhere | live |
| `_BACKOFF_BASE` | `src/alma/openalex/http.py` | `1.0` | `_backoff_wait` | First OpenAlex backoff. | Quota readout. | nowhere | live |
| `_BACKOFF_MAX` | `src/alma/openalex/http.py` | `60.0` | `_backoff_wait` | Longest OpenAlex backoff. | Quota readout. | nowhere | live |
| `_CACHE_TTL_SECONDS` | `src/alma/openalex/http.py` | `300` | `_store_cached_response` | In-process OpenAlex cache age. | Quota readout `calls_saved_by_cache`. | nowhere | live |
| `_CREDIT_WARN_FRACTION` | `src/alma/openalex/http.py` | `0.1` | `_update_rate_limits` | Remaining share at which a warning is logged. | no measure | nowhere | live |
| `_NETWORK_CACHE_TTL_HOURS` | `src/alma/api/routes/publications.py` | `24` | `_network_cache_write` | Age of the cached network view of a paper. | no measure | nowhere | live |
| `HEALER_PER_TICK_LIMIT` | `src/alma/services/maintenance.py` | `50` | `maintenance_repair_periodic` | Items per task per tick. | Health page. | nowhere | live |
| `USER_PAUSE_HOURS` | `src/alma/services/maintenance.py` | `24` | `set_task_user_pause` | How long stopping a run pauses automation. | no measure | nowhere | live |
| `_RESUME_SESSION_CAP` | `src/alma/services/maintenance.py` | `5000` | `resume_orphaned_sweeps`, `run_onboarding_convergence` | Budget of one resume after a restart. | Job result. | nowhere | live |
| `_CONVERGE_MAX_STEPS` | `src/alma/services/maintenance.py` | `30` | `run_onboarding_convergence` | Steps of the onboarding repair chain. | no measure | nowhere | live |
| `_PER_RUN_PAPERS` | `src/alma/services/s2_vectors.py` | `1500` | `run_s2_vector_backfill` | Papers per run of the vector fetch. | Job result. | nowhere | live |
| `DEFAULT_VECTOR_CHUNK_PAPERS` | `src/alma/services/s2_vectors.py` | `250` | `run_s2_vector_backfill` | Papers per request. | Job result. | nowhere | live |
| `_AUTO_S2_FETCH_LIMIT` | `src/alma/services/embedding_chain.py` | `5000` | `schedule_post_hydration_chain` | Vectors fetched automatically after hydration. | Health embedding coverage. | nowhere | live |
| `_AUTO_PAPER_INSERT_HYDRATION_LIMIT` | `src/alma/services/corpus_rehydrate.py` | `25` | `schedule_pending_hydration_sweep` | Papers hydrated right after insert. | Health page. | nowhere | live |
| `_DATACITE_LOOKUP_LIMIT` | `src/alma/services/corpus_rehydrate.py` | `50` | `_link_datacite_components` | DataCite lookups per sweep. | Job result. | nowhere | live |
| `TITLE_RESOLUTION_JACCARD_THRESHOLD` | `src/alma/services/title_resolution.py` | `0.92` | `_accept_match` | Title similarity accepted as the same work. | Health unresolved-identity count: many left unmatched means lower. Wrong matches have no measure. | nowhere | live |
| `TITLE_RESOLUTION_YEAR_DELTA` | `src/alma/services/title_resolution.py` | `1` | `_accept_match` | Year difference allowed in a match. | Health unresolved-identity count. | nowhere | live |
| `TITLE_RESOLUTION_MAX_RESULTS` | `src/alma/services/title_resolution.py` | `3` | `_fetch_openalex_title_match` | Results considered per title. | Health unresolved-identity count. | nowhere | live |
| `TITLE_RESOLUTION_PER_RUN_BUDGET` | `src/alma/services/title_resolution.py` | `500` | `run_title_resolution_sweep` | Papers attempted per run. | Job result. | nowhere | live |
| `OPENALEX_FALLBACK_PER_RUN_BUDGET` | `src/alma/services/title_resolution.py` | `100` | `run_title_resolution_sweep` | Paid OpenAlex fallbacks per run. | Quota readout. | nowhere | live |
| `_PER_RUN_SECONDS` | `src/alma/services/title_resolution.py` | `75.0` | `run_title_resolution_sweep` | Time budget of one run. | Job result. | nowhere | live |

<!-- inventory: inline -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_POLICIES` | `src/alma/core/http_sources.py` | seconds between calls: Semantic Scholar 1.05, Crossref 0.34 with a contact else 1.05, DataCite 0.25, arXiv 3.1, bioRxiv 0.35, Unpaywall 0.12, Europe PMC 0.2, PMC 0.2, publisher 0.5, ORCID 0.04 | `_current_min_interval`, `_send` | Pace, concurrency and retries per source. They follow each provider's published limits. | Job result: 429s mean slower. | nowhere | live |
| `maintain_citation_graph_periodic` reference budget | `src/alma/api/scheduler.py` | 500 | `maintain_citation_graph_periodic` | References fetched per graph pass. | Job result. | nowhere | live |
| `_is_stale_active_status` | `src/alma/api/scheduler.py` | 300 s | `_reap_orphan_jobs_now` | Age at which a dead job counts as orphaned. | no measure | nowhere | live |

### Paper PDFs

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `DOWNLOAD_DEADLINE` | `src/alma/application/pdfs/download.py` | `180.0` | `download_candidate` | Time budget of one candidate. | PDF attempts in Activity. | nowhere | live |
| `HTML_HOP_LINKS` | `src/alma/application/pdfs/download.py` | `3` | `_follow_landing_page` | PDF links followed from a landing page. | PDF attempts in Activity. | nowhere | live |
| `MIN_PDF_BYTES` | `src/alma/application/pdfs/verify.py` | `10000` | `download_candidate` | Files below this are rejected. | PDF attempts in Activity. | nowhere | live |
| `VERIFY_PAGES` | `src/alma/application/pdfs/verify.py` | `2` | `read_facts` | Pages read to identify a PDF. | PDF attempts in Activity. | nowhere | live |
| `_TOKEN_CONTAINMENT` | `src/alma/application/pdfs/verify.py` | `0.8` | `title_matches` | Share of title words a PDF must contain. | Right PDFs rejected means lower. | nowhere | live |
| `ORPHAN_GRACE_SECONDS` | `src/alma/application/pdfs/store.py` | `3600` | `prune_orphan_files` | Age before an unreferenced file may be pruned. | no measure | nowhere | live |
| `STAGING_TTL_SECONDS` | `src/alma/application/pdfs/store.py` | `24 * 3600` | `prune_orphan_files` | How long a staged upload waits. | no measure | nowhere | live |

### Embeddings

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `DEFAULT_LOCAL_MODEL` | `src/alma/ai/providers.py` | `specter2-base` | `get_active_provider` | Default of `ai.local_model`. | Health embedding coverage. | Settings: AI | live |
| `_CLUSTER_REDUCED_DIM` | `src/alma/ai/clustering.py` | `5` | `reduce_for_clustering` | Dimensions before density clustering of branches. | Branch panel. | nowhere | live |
| `_UMAP_MIN_POINTS` | `src/alma/ai/clustering.py` | `15` | `reduce_for_clustering` | Points below which clustering skips reduction. | Branch panel. | nowhere | live |

### Leftovers

Constants nothing reads. They do nothing and can go.

<!-- inventory: constant -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `_GRAPH_WARMUP_DELAY_SECONDS` | `src/alma/api/scheduler.py` | `90` | Nothing | Delay of the removed map warm-up. | no measure | nowhere | no-op |

## Frontend preferences

Kept per browser. Clearing site data resets them. None reaches the backend except as a request parameter.

<!-- inventory: frontend -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `alma.sidebar.collapsed` | `frontend/src/components/layout/AppShell.tsx` | off | `AppShell` | Collapsed sidebar. | no measure | Sidebar | live |
| `alma.feed.hideLibrary` | `frontend/src/pages/FeedPage.tsx` | off | `FeedPage` | Hides papers already in the Library from the Feed. | no measure | Feed page | live |
| `alma.discovery.hideLibrary` | `frontend/src/pages/DiscoveryPage.tsx` | off | `DiscoveryPage` | Hides papers already in the Library from a lens. | no measure | Discovery page | live |
| `alma.library.saved.viewMode` | `frontend/src/components/library/SavedTab.tsx` | `compact` | `SavedTab` | Cards or table. | no measure | Library page | live |
| `alma.library.saved.cardSort` | `frontend/src/components/library/SavedTab.tsx` | `date` | `SavedTab` | Card sort. | no measure | Library page | live |
| `alma.library.saved.compactSortKey` | `frontend/src/components/library/SavedTab.tsx` | `year` | `SavedTab` | Table sort column. | no measure | Library page | live |
| `alma.library.saved.compactSortDir` | `frontend/src/components/library/SavedTab.tsx` | `desc` | `SavedTab` | Table sort direction. | no measure | Library page | live |
| `alma.library.saved.prefsVersion` | `frontend/src/components/library/SavedTab.tsx` | `2026-03-07.list-default.v1` | `SavedTab` | Resets the four keys above when it changes. | no measure | nowhere | live |
| `alma.datatable.` | `frontend/src/components/ui/data-table/DataTable.tsx` | per table | `DataTable` | Column visibility, order, size and sort of each table, under one key per `storageKey`. | no measure | Each table | live |
| `alma-connector-notice-dismissed` | `frontend/src/hooks/useConnectorStatus.ts` | unset | `useConnectorStatus` | Which browser connector notice was dismissed. | no measure | Notice | live |
| `alma.pending-author-follows.v1` | `frontend/src/components/authors/SuggestedAuthorsRail.tsx` | empty | `SuggestedAuthorsRail` | State: follows not yet acknowledged. | no measure | nowhere | live |
| `alma.onboarding.state` | `frontend/src/components/onboarding/useOnboardingState.ts` | unset | `useOnboardingState` | State: wizard progress, per session. | no measure | nowhere | live |

### Frontend constants that set behaviour

<!-- inventory: frontend -->
| Control | Defined in | Default | Read by | Controls | Calibrate | Exposed | Status |
|---|---|---|---|---|---|---|---|
| `LENS_REFRESH_LIMIT` | `frontend/src/pages/DiscoveryPage.tsx` | `50` | `refreshLensMutation` | Deck size of a refresh. The scheduler uses the function default, also 50. | Refresh summary `requested_slate_size`. | nowhere | live |
| `DEFAULT_VISIBLE_RECS` | `frontend/src/pages/DiscoveryPage.tsx` | `20` | `DiscoveryPage` | Cards before Show all. Pinned to `FIRST_SCREEN_CARDS`. | no measure | nowhere | live |
| `SAVED_PAGE_SIZE` | `frontend/src/components/library/SavedTab.tsx` | `50` | `SavedTab` | Library page size. | no measure | nowhere | live |
