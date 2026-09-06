# Discovery, learning and maps: scope review

**Decision update, 2026-09-06:** the user confirmed Feed, Discovery and Library as
the core. Maps will be decoupled, preserved on a secondary development branch for
future plugins, and temporarily removed from main. Learning correctness and
calibration work proceed in parallel. PRODUCT_DECISIONS D24 / Task 67 §13 govern;
the earlier open question and optional-module proposals below are historical.

Progress (2026-09-06): the super-region build is coordinate-free and the
retained learning path (Signal Lab policy, scoring terms, fit, simulate,
summary, eval) reads membership, vectors and the assignment rule from
`application/semantic_partition.py`, never from the map substrate — guarded by
`tests/test_semantic_partition_coordinate_free.py`. Tables from Task 67 §14.3,
the freshness owner and consumer migration remain open.

Status: proposal and source audit, 2026-09-06. Not an edition decision or a claim
that recommendation quality has been measured. Active implementation plan:
`tasks/67_LEARNING_DISCOVERY_MAPS_2026-09-05.md`, section 12.

## One product outcome

Proposed promise: **help me find research worth my attention, understand why it
matters to me, and keep what I choose.**

Feed monitors explicit sources. Library holds the user's selected papers.
Discovery expands beyond that monitoring. Supporting features should improve
this loop without becoming a second product the user must maintain.

The open product choice is whether a useful paper shortlist leads, or whether
understanding/exploring a field is equally central. The proposal below assumes
the shortlist leads; confirm this before removing surfaces or assigning editions.

## Boundaries

| Part | User job | What it should not own |
| --- | --- | --- |
| Discovery | Offer plausible new papers with useful reasons and clear actions | Maintaining maps; requiring training sessions; silently changing Library membership |
| Preference learning | Use deliberate feedback to improve retrieval/ranking | Treating a dismissal as dislike; treating the engine's own scores as user approval |
| Signal Lab | Let a willing user teach preferences through explicit comparisons | Being mandatory upkeep; owning another scoring formula; promising improvement without evidence |
| Semantic representation | Supply reusable vectors and, where needed, groups/relationships | Display controls; moving geometry because the user likes something |
| Map | Help explore a direction and reach useful papers/authors | Being a prerequisite for ordinary suggestions; fitting a separate taste model merely to colour a page |

Learning is not synonymous with Signal Lab. Normal saves, likes, dislikes and
follows already provide evidence. A lightweight edition should not lose all
personalization merely because it cannot afford map or Lab computations.

Likewise, a map is not the underlying semantic representation. Hiding a map
does not save its computation if other features still schedule the same jobs.
That dependency must be made explicit before adding a toggle or package split.

## Simplification direction, pending confirmation

- Lead Discovery with papers, reasons and actions. Put detailed score diagnostics
  and tuning behind disclosure; do not make users manage every internal model.
- Give each optional interaction one purpose. A map should help choose a research
  direction; Lab should collect feedback the normal workflow does not supply.
- Keep one evidence contract and one final ranking/explanation path. An evaluator
  must exercise that same path, not approximate it with another score formula.
- Prefer an internal optional module boundary first. A public plugin system adds
  lifecycle, compatibility and support costs; there is no demonstrated need for
  third-party map implementations yet.
- Assign Standard/Max only after identifying indispensable jobs and measuring
  their costs. The earlier Max-only partition/Lab proposal remains provisional.

Non-goals for this slice: new mini-games, new retrieval providers, more tuning
knobs, a generic plugin framework, a new clustering algorithm, or a page redesign
before the primary user job is confirmed.

## What the current source demonstrates

1. **Two Lab ranking terms are disconnected.** `discovery/scoring.py` emits
   `lab_region_offset_raw` and `lab_utility_raw`, but
   `application/discovery/features.py` and `ranker.py` omit them from final scoring.
   A local pure-function probe with otherwise identical candidates, raw values
   −1/0/+1 and both settings at 0 or 5 returned **56.887273 in all six cases**.
   This proves an integration gap, not that the learned terms would improve quality.
   Author/venue Lab terms have separate active curated-affinity consumers; it would
   be incorrect to call all Lab learning inert.
2. **Evaluation does not exercise the deployed ranking.**
   `application/signal_lab/eval.py` adds hypothetical 2.5-point terms directly to
   stored scores, assumes utility confidence 1.0 and pools rows across lenses.
   Runtime uses an evidence-based confidence damper. Hypothetical churn is neither
   actual ranking behaviour nor evidence of better suggestions.
3. **Model freshness omits inputs.** `signal_lab/fit.py` fingerprints round
   count/max-id but its builder also reads vectors, regions, Library membership,
   authors, venues and tuning. This is a source-confirmed dependency mismatch;
   mutation-by-mutation stale-cache reproductions remain to do.
4. **Lab currently depends on map-produced groups.** `signal_lab/policy.py`
   directs users to Rebuild map layouts when the super-region substrate is absent.
   The fit consumes cluster→region assignments, not the drawn map itself. A real
   optional-map boundary therefore involves job ownership, not just navigation.
5. **Map reads can still do substantial computation.** `/graphs/signal-field`
   calls `terrain.build_terrain_field` on each request. Layout coordinates are
   stored, but the taste field is fitted at read time. This is not yet a measured
   bottleneck; collect latency and memory before choosing caching or removal.

## Acceptance gates

- Correctness: nonzero enabled terms have an observable, explained effect; off
  means no effect; ordinary feedback and Lab respect the same action semantics.
  Retries, undo, purge and changed model inputs must not leak or strand evidence.
- Honesty: one score/explanation, actual-runtime evaluation, degraded source/model
  state shown explicitly. A passing formula test is not recommendation quality.
- Usefulness: compare a fixed simple baseline with the proposed learning using
  temporally held-out evidence; report sample size and inconclusive results.
  Check novelty as well as relevance so personalization does not mean repetition.
- Cost: measure cold/warm refresh, page reads, peak memory and background writes.
  Verify which jobs stop when an optional surface is off. Pi support is unverified.
- Scope: removing an optional interaction must leave the primary paper-selection
  workflow useful. A capability remains only if its contribution warrants its
  code, computation and user attention costs.

## Next work

### Learning correctness and calibration are required

Explicit user requirement, 2026-09-06: learning must be correct and well calibrated.
This is not satisfied by making previously disconnected terms change a score.

Current source: `signal_lab/map_terms.utility_confidence` is
`min(1, train_prefs / 60)`, an evidence-volume multiplier, not measured predictive
reliability. `fit._holdout_metrics` reports pairwise accuracy, not probability
calibration. Neither establishes that confidence 0.8 means 80% correct.

Audit and implementation gates:

1. **Define the predicted event first.** Pairwise preference, saving a suggestion,
   and similarity judgments are different targets. A 0–100 ranking score is not
   a probability. Document each confidence channel's meaning; do not rescale a
   score and call it calibrated.
2. **Audit evidence before fitting.** No duplicate feedback on retry; skip/dismiss
   is not dislike; no training on engine predictions. Check conflicting answers,
   undo/purge, merged identities and the same signal entering several features.
3. **Keep evaluation genuinely held out.** Split before building learned priors
   and preprocessing; prevent future Library feedback entering historical test
   predictions. Group dependent comparisons from the same round/query, account
   for repeated papers, and preserve a final test set when fitting a calibrator.
4. **Measure useful ordering and reliability separately.** Compare against a
   simple baseline, test one learned component at a time, and report held-out
   ranking quality. For outputs explicitly defined as probabilities, report
   Brier/log loss and reliability bins with counts and uncertainty. Do not apply
   probability metrics to arbitrary relevance scores or geometric similarity.
5. **Respect limited and biased evidence.** Report effective independent sample
   size, not three pairwise constraints as three independent user judgments.
   Lab's actively selected comparisons and displayed recommendation outcomes
   are selected samples; results do not establish whole-corpus usefulness.
   With insufficient data, report inconclusive and keep influence conservative.
6. **Verify deployment parity.** Evaluation must use the actual ranking weights,
   evidence dampers and enabled state. Recheck on a later time window and after
   meaningful input/model changes. A large refit should not silently make weak
   evidence look certain.

No quality/calibration improvement has been claimed or implemented yet. The
confidence multiplier may be a useful safeguard, but its name and downstream
interpretation must not promise validated reliability.

Confirm the primary job, then settle whether disconnected Lab terms should be
restored as explained ranking inputs or retired together with their controls.
Either choice must remove the evaluation/runtime mismatch. Reproduce freshness
failures and audit evidence eligibility next; profile map jobs before extraction.
