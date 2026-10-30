# Health and maintenance surface audit

Audited 2026-09-21. Contract map for Health assessment, maintenance planning,
automatic repair, insert-time convergence, and the Health UI.

## One truth path

| Concern | Owner |
|---|---|
| Dimension meaning, severity, actions, drilldown capability | services/health.py |
| Durable corpus/author/operation snapshots | services/health_snapshots.py |
| Task order, dependencies, safety, cost, limits, routine classification | services/maintenance.py registry |
| Typed task/run contracts | services/maintenance_contracts.py |
| Transient and unchanged retry clocks | services/retry_policy.py |
| Health UI grouping/order/recommendation/attention | backend operation payload |
| Affected-paper popup availability | backend dimension has_drilldown field |
| Runtime dependency blockers and owner/fix route | backend availability_blocker field |
| Run history and logs | operation_status + operation_logs, shown in Activity |

The frontend keeps no task-key order array, drilldown allowlist, or independent
pending-means-unhealthy rule.

## Retry and freshness policy

| Outcome | Recheck |
|---|---|
| Transient paper/author/provider failure | 6 hours |
| Successful author works coverage | 14 days |
| Unchanged paper/author metadata | 30 days |
| Successful ORCID duplicate scan | 30 days |
| Transient ORCID duplicate-scan failure | 6 hours |
| Exact terminal no-match | No automatic retry while lookup identity is unchanged |

Terminal no-match resets when its lookup input changes. High-priority author
actions such as Follow also re-arm terminal author hydration. Health reports
terminal gaps as observed/exhausted work instead of claiming a repair can clear
them.

## Ordered operation graph

Routine means normal pipeline/freshness work. A routine backlog remains
available under **Healthy & routine tools** and does not become a Health alarm.
Structural candidates or warning/critical dimensions become full repair cards.

| Order | Task | Kind | Required predecessor |
|---:|---|---|---|
| 10 | Author metadata | routine | — |
| 20 | ORCID duplicate scan | routine | author metadata |
| 21 | Apply author merges | structural, destructive/manual | — |
| 29 | Seed thin suggested authors | optional | — |
| 30 | Expand author works | routine, optional | ORCID scan |
| 31 | Backfill stale author corpora | routine, optional | ORCID scan |
| 40 | Resolve paper identity by title | repair | — |
| 50 | Rehydrate paper metadata | repair | title resolution |
| 56 | Reconcile paper groups | repair | — |
| 58 | Collapse duplicate identity | structural | metadata |
| 60 | Dedup preprint/journal twins | structural, destructive/manual | metadata |
| 62 | Dedup library | structural, destructive/manual | metadata |
| 70 | Fetch S2 vectors | repair | preprint review is advisory |
| 80 | Compute local embeddings | repair | S2 vectors |
| 85 | Recompute author centroids | routine | embeddings |
| 90 | Backfill reference graph | repair | metadata |
| 94 | Normalize topics | routine | metadata |
| 100 | Garbage-collect orphan authors | routine, destructive/manual | embeddings |
| 110 | Database housekeeping | routine | — |

Manual, destructive, and optional tasks are never one-click recommendations.
Required predecessors block automatic scheduling. Manual runs show advisory
predecessors. Paper-group reconciliation remains independently reachable: a
provider outage must not freeze an already-observable group defect.

The local-embedding repair declares its provider requirement in the task
registry. With no usable provider it is `blocked_external`, never recommended
or auto-run, and links to its owner at Settings → AI.

## Recommendation and card policy

- Recommended next requires measured pending work, no required blocker,
  sufficient quota, safe execution, and attention_required=true.
- Routine pending work is not recommended.
- Informational dimensions remain visible but do not create emergency cards.
- Assessment failure is attention; unknown is never rendered as zero.
- Structural dimensionless operations use their measured candidate pool as the
  defect.
- A routine queued chip uses a clock and shows its queue count; a clear task uses
  a check.
- Dimension rows open a paper popup only when has_drilldown=true. Author rows
  link to the Authors owner surface.
- Destructive repairs open the review/confirmation dialog.
- Every scheduled repair has an Activity row and structured logs. Failed rows
  expose their logs in Activity.

## Corpus metadata convergence

The operation count is the distinct union of the same four selectors the runner
uses: OpenAlex metadata, Semantic Scholar metadata, Crossref abstracts, and
landing-page/preprint abstract recovery. One shared run limit spans all phases.

- Provider exceptions are retryable for 6 hours and leave the actionable queue
  during cooldown. They are not treated as empty provider results.
- A Crossref batch outage cannot stamp its entire DOI batch as permanent
  `terminal_no_match`.
- An abstract-recovery worker failure writes a retry ledger row instead of
  hot-looping on every run.
- Abstract recovery fingerprints local DOI/URL inputs. Changing an input rearms
  immediately; unchanged no-result work sleeps for 30 days.
- Genuine no-match/no-content outcomes become terminal or long-cooldown rows,
  so a repair pass can reach zero actionable items while Health still reports
  the truthful exhausted floor.

## Insert-time convergence

- Library, Feed, Discovery, imports, OpenAlex ingestion, author backfill, and
  legacy Google Scholar writes enter the durable paper hydration ledger.
- Scheduling happens after commit and coalesces onto one fixed hydration lane.
- New followed/resolved authors enter the author hydration ledger.
- Bulk identifier resolution schedules the author hydration drain after writes.
- Imported structured coauthors count as linked to their publication; resolution
  no longer selects only the single papers.author_id owner.
- Import postprocessing reports its real stage total. Vector work is queued by
  enrichment instead of reported as a nonexistent import stage.

## Restart and snapshot truth

An orphan marker is a one-shot handoff. After startup relaunches an operation,
the marker is consumed; later restarts cannot replay it. User-cancelled jobs and
destructive operations are excluded.

Corpus, author, then operation-plan snapshots rebuild in dependency order.
Logic versions invalidate stored payloads when contracts change. The author
fingerprint includes paper/authorship/embedding/seed inputs used by suggested
author coverage. Planning combines corpus and author dimensions before assigning
attention.

## Verification

- Maintenance contracts and failure assessment: 56 passed (48 service + 8 API).
- Author retry/freshness/dedup: 53 passed.
- Background governance/cooldown: 28 passed.
- Paper-group focused suites: 24 passed; broader sweep reached 41 passing tests
  before an unrelated shared scheduler teardown hang.
- Corpus/hydration focused suite: 40 existing tests passed plus 3 new failure
  regressions; one combined-process rerun exposed only shared scheduler teardown.
- Frontend: 142 tests; TypeScript passed.
- Python Ruff and diff whitespace checks: passed.
- Health component browser smoke: passed.
- Real dev API: snapshots fresh; routine backlogs calm; structural duplicates
  loud/manual; no unsafe recommendation.
- Real cold-start browser test: both Health endpoints repolled from pending state
  and rendered the published snapshot.
