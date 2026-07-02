/**
 * HealthPage — the single front door for "is my data healthy, and what do I do
 * about it?" One scrollable surface, no tabs: status and the operation that
 * repairs it live in the SAME card (a `RepairCard`), so nothing is scattered.
 *
 * Top → bottom:
 *   - persistent vitals ribbon + scoreboard (at-a-glance triage)
 *   - "Corpus & embeddings" repair group  — op cards, worst-first
 *   - "Authors" repair group               — op cards, worst-first
 *   - "Observed — no automatic repair"      — dimensions with no repair op
 *   - "System status"                       — operational subsystems
 *
 * The card unit is the maintenance OPERATION (not the dimension) because the
 * mapping is many-to-many — `corpus_metadata` alone repairs seven dimensions.
 * Each op lists the gaps it heals as status rows (drilldown to the papers) and
 * carries its run / auto-repair / cap / scope / batch controls once. Every
 * number reads the canonical endpoints (/insights/health + /health/operations).
 */
import { useEffect, useRef, useState } from 'react'
import { AlertTriangle, RefreshCw } from 'lucide-react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  getHealthOperations,
  getHealthSnapshot,
  getJobStatus,
  runMaintenanceOperation,
  setMaintenanceConfig,
  type HealthDimension,
  type MaintenanceOperation,
  type MaintenanceRunRequest,
} from '@/api/client'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { Button } from '@/components/ui/button'
import { JargonHint } from '@/components/shared/JargonHint'
import { HealthVitals } from '@/components/health/HealthVitals'
import { RepairGroup } from '@/components/health/RepairGroup'
import { DiagnosticsSection } from '@/components/health/DiagnosticsSection'
import { SystemStatusCards } from '@/components/health/SystemStatusCards'
import { ApiBudgetCard } from '@/components/health/ApiBudgetCard'
import { SectionLabel } from '@/components/health/SectionLabel'
import { HealthDimensionDrilldown } from '@/components/health/HealthDimensionDrilldown'
import { invalidateQueries } from '@/lib/queryHelpers'
import { freshnessNote } from '@/components/health/healthFormat'
import { formatRelativeShort } from '@/lib/utils'
import { useToast, errorToast } from '@/hooks/useToast'

const SNAPSHOT_KEY = ['health', 'snapshot']
const OPERATIONS_KEY = ['health', 'operations']

export function HealthPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [openDim, setOpenDim] = useState<HealthDimension | null>(null)
  // "Run recommended sequence" state (H-4). The sequence is driven by the DURABLE
  // terminal state of the launched background job — never by a blind poll of
  // `recommended_next`. `watchedJob` is the step we fired and are waiting on; we
  // advance only after it COMPLETES, stop on failed/cancelled, and stop (instead
  // of looping forever) if the same op is still recommended after it finished.
  const [sequenceActive, setSequenceActive] = useState(false)
  // `pendingBefore` (42.5) is the op's pending count at launch — the sequence
  // loops the SAME op while its pending strictly decreases, and stops truthfully
  // when it doesn't. A ref mirrors `sequenceActive` so a mid-poll "Stop" is
  // honoured even inside the async post-repair freshness wait (42.4).
  const [watchedJob, setWatchedJob] = useState<{ jobId: string; opKey: string; pendingBefore: number } | null>(null)
  const sequenceActiveRef = useRef(false)
  useEffect(() => {
    sequenceActiveRef.current = sequenceActive
  }, [sequenceActive])

  const snapshotQuery = useQuery({
    queryKey: SNAPSHOT_KEY,
    queryFn: getHealthSnapshot,
    staleTime: 30_000,
    retry: 1,
  })
  const operationsQuery = useQuery({
    queryKey: OPERATIONS_KEY,
    queryFn: getHealthOperations,
    staleTime: 30_000,
    retry: 1,
  })

  // Poll the launched step's Activity status until it reaches a terminal state.
  // refetch stops the moment it's terminal, so this isn't a perpetual loop.
  const jobStatusQuery = useQuery({
    queryKey: ['activity', watchedJob?.jobId],
    queryFn: () => getJobStatus(watchedJob!.jobId),
    enabled: !!watchedJob,
    refetchInterval: (query) => {
      const st = query.state.data?.status
      return st && ['completed', 'failed', 'cancelled'].includes(st) ? false : 1500
    },
  })

  // Stop the auto-advance sequence and clear the watched job. NOTE: this never
  // cancels an already-running background job — it only stops launching the NEXT
  // step (H-4: "Stop auto-advance" ≠ cancel). The control is labelled to match.
  const stopSequence = (message?: string, tone: 'info' | 'error' = 'info') => {
    setSequenceActive(false)
    sequenceActiveRef.current = false
    setWatchedJob(null)
    if (message) {
      if (tone === 'error') errorToast('Recommended sequence stopped', message)
      else toast({ title: 'Recommended sequence', description: message })
    }
  }

  const runMutation = useMutation({
    // The atomic Run spec travels with the click (RepairCard builds it from its
    // visible controls). A bare `{ key }` — e.g. a drilldown "fix all" — sends no
    // max_items, so the backend applies the task's remembered manual limit.
    // `sequence` marks a step fired by the auto-advance runner (H-4): on success
    // we WATCH the job to terminal state instead of toasting and forgetting it.
    mutationFn: ({ key, request }: { key: string; request?: MaintenanceRunRequest; sequence?: boolean }) =>
      runMaintenanceOperation(key, request ?? {}),
    onSuccess: async (result, variables) => {
      await invalidateQueries(queryClient, OPERATIONS_KEY, SNAPSHOT_KEY)
      const launched = result.status !== 'noop' && !!result.job_id
      if (variables.sequence) {
        // Enqueue succeeded, but a background FAILURE is not this mutation's
        // onError — so we can't trust "started" as "will finish". If nothing was
        // eligible, the step can't make progress → stop truthfully. Otherwise
        // hand off to the job watcher, which advances only on COMPLETED.
        if (!launched) {
          stopSequence('The recommended step had nothing eligible to run — auto-advance stopped.')
        } else {
          const before = (operationsQuery.data?.operations ?? []).find((o) => o.key === result.key)
          setWatchedJob({
            jobId: result.job_id as string,
            opKey: result.key,
            pendingBefore: Number(before?.candidates_pending ?? Number.POSITIVE_INFINITY),
          })
        }
        return
      }
      if (!launched) {
        toast({ title: 'Nothing to run', description: 'No provider or no eligible items.' })
      } else {
        toast({
          title: 'Maintenance started',
          description: `${result.key} queued (${result.job_id}). Track it in Activity.`,
        })
      }
    },
    onError: (err, variables) => {
      // A failed enqueue stops the sequence (don't loop on a broken step).
      if (variables?.sequence) stopSequence(String(err), 'error')
      else errorToast('Could not start maintenance', String(err))
    },
  })

  const configMutation = useMutation({
    mutationFn: ({
      key,
      body,
    }: {
      key: string
      body: {
        auto_enabled?: boolean
        auto_daily_cap?: number
        remembered_manual_limit?: number
        request_batch_size?: number
      }
    }) => setMaintenanceConfig(key, body),
    onSuccess: async () => {
      await invalidateQueries(queryClient, OPERATIONS_KEY)
    },
    onError: (err) => errorToast('Could not update setting', String(err)),
  })

  const refresh = () => invalidateQueries(queryClient, SNAPSHOT_KEY, OPERATIONS_KEY)
  const runningKey = runMutation.isPending ? (runMutation.variables?.key ?? null) : null
  // H-11: the op whose auto-config write is in flight (shows a "Saving…" hint so
  // the post-save snap to server truth doesn't look like a glitch).
  const configSavingKey = configMutation.isPending ? (configMutation.variables?.key ?? null) : null

  const snapshot = snapshotQuery.data
  const operations = operationsQuery.data?.operations ?? []

  // Resolve the dimensions one op repairs (op.repairs ∩ snapshot.dimensions).
  const dimByKey = new Map(
    (snapshot?.dimensions ?? []).map((d): [string, HealthDimension] => [d.key, d]),
  )
  const dimsOf = (op: MaintenanceOperation): HealthDimension[] =>
    op.repairs.map((k) => dimByKey.get(k)).filter((d): d is HealthDimension => !!d)

  // Dimensions with no repair op at all → the "Observed" diagnostics section.
  const repairedKeys = new Set(operations.flatMap((op) => op.repairs))
  const orphanDims = (snapshot?.dimensions ?? []).filter((d) => !repairedKeys.has(d.key))

  // Grouping + order come ONLY from the backend plan (Checkpoint G): render each
  // stage in registry order with its label, populated by its operation_keys. No
  // hard-coded task-key arrays — a registry addition appears in its stage
  // automatically. Empty stages (nothing pending/applicable) are dropped.
  const opByKey = new Map(operations.map((op): [string, MaintenanceOperation] => [op.key, op]))
  const stageGroups = (operationsQuery.data?.stages ?? [])
    .map((stage) => ({
      key: stage.key,
      label: stage.label,
      ops: stage.operation_keys
        .map((k) => opByKey.get(k))
        .filter((o): o is MaintenanceOperation => !!o),
    }))
    .filter((group) => group.ops.length > 0)
  const recommended = operationsQuery.data?.recommended_next ?? null

  // H-4: advance the sequence off the watched job's DURABLE terminal state.
  //   - failed / cancelled → stop and surface it (a background failure never
  //     reached the mutation's onError, so this is the only place it's caught);
  //   - completed → re-plan, then either fire the NEXT op (progress) or stop if
  //     the SAME op is still recommended (a bounded batch reduced but didn't
  //     clear the backlog, or only a manual gate remains) — never an infinite loop.
  useEffect(() => {
    if (!watchedJob) return
    const status = jobStatusQuery.data?.status
    if (status === 'failed' || status === 'cancelled') {
      stopSequence(`Step "${watchedJob.opKey}" ${status} — see Activity. Auto-advance stopped.`, 'error')
      return
    }
    if (status !== 'completed') return

    const completedKey = watchedJob.opKey
    const pendingBefore = watchedJob.pendingBefore
    setWatchedJob(null) // consume this completion exactly once
    if (!sequenceActive) return // user stopped while it ran — leave the job's results in place
    void advanceSequence(completedKey, pendingBefore)
    // eslint-disable-next-line react-hooks/exhaustive-deps -- driven by the job status + identity
  }, [jobStatusQuery.data?.status, watchedJob])

  // Advance the recommended sequence off FRESH counts (42.4 + 42.5). A repair
  // job's terminal state fires this; the materialized health view rebuilds
  // asynchronously (SWR), so evaluating immediately reads yesterday's counts and
  // stops a working repair. We (1) refetch until the snapshot is no longer
  // stale/rebuilding — bounded so a stuck rebuild can't hang the sequence — then
  // (2) LOOP the same op while its pending strictly decreased and is > 0, else
  // advance to the next recommended op, else stop truthfully.
  const advanceSequence = async (completedKey: string, pendingBefore: number) => {
    let opsData = operationsQuery.data
    let fresh = false
    for (let i = 0; i < 10; i++) {
      if (!sequenceActiveRef.current) return // user stopped during the wait
      const [snap, ops] = await Promise.all([snapshotQuery.refetch(), operationsQuery.refetch()])
      opsData = ops.data
      const s = snap.data
      if (s && s.stale !== true && s.rebuilding !== true) {
        fresh = true
        break
      }
      await new Promise((r) => setTimeout(r, 3000))
    }
    if (!sequenceActiveRef.current) return
    if (!fresh) {
      toast({ title: 'Health', description: 'Diagnostics are still refreshing — counts may lag briefly.' })
    }
    const opNow = (opsData?.operations ?? []).find((o) => o.key === completedKey)
    const pendingNow = Number(opNow?.candidates_pending ?? 0)
    // 42.5: keep running the same op while it is genuinely draining the backlog.
    if (pendingNow > 0 && pendingNow < pendingBefore) {
      runMutation.mutate({ key: completedKey, sequence: true })
      return
    }
    const next = opsData?.recommended_next ?? null
    if (!next) {
      stopSequence('Recommended sequence finished — no further safe steps (any manual-review gates remain for you).')
    } else if (next.key === completedKey) {
      // Same op still recommended but pending did NOT drop → truthful no-progress stop.
      stopSequence(
        `"${next.label}" made no progress this run (${pendingNow.toLocaleString()} still pending) — ` +
          'it may be throttled or blocked; retry later or review. Auto-advance stopped.',
      )
    } else {
      runMutation.mutate({ key: next.key, sequence: true })
    }
  }

  const onRun = (key: string, request: MaintenanceRunRequest) =>
    runMutation.mutate({ key, request })
  const onConfig = (
    key: string,
    body: {
      auto_enabled?: boolean
      auto_daily_cap?: number
      remembered_manual_limit?: number
      request_batch_size?: number
    },
  ) => configMutation.mutate({ key, body })

  const groupProps = { dimsOf, onRun, onConfig, onOpenDim: setOpenDim, runningKey, configSavingKey }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h1 className="font-brand text-2xl font-semibold text-alma-900">Health</h1>
          <p className="mt-1 text-sm text-slate-500">
            Is my data healthy — and what do I do about it?
          </p>
        </div>
        <div className="flex items-center gap-3">
          {snapshot ? (
            <span className="text-xs text-slate-400">
              Last assessed {formatRelativeShort(snapshot.generated_at)}
              {freshnessNote(snapshot) ? ` · ${freshnessNote(snapshot)}` : ''}
            </span>
          ) : null}
          <Button
            size="sm"
            variant="ghost"
            onClick={refresh}
            disabled={snapshotQuery.isFetching || operationsQuery.isFetching}
          >
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
        </div>
      </div>

      <ConceptCallout
        eyebrow="What is Health?"
        summary="ALMa watches your corpus for fixable gaps and repairs them — one-click or automatically."
      >
        <p>
          Each card is one <strong>repair operation</strong>. It shows the gaps it fixes — missing
          abstracts, embedding{' '}
          <JargonHint
            title="Coverage"
            description="The share of papers that have an embedding vector for the active model. Discovery's semantic ranking depends on it."
            className="inline-flex"
          />{' '}
          coverage, unresolved identities — each with a severity and the affected papers, and the
          controls to act: <strong>Run now</strong> processes a batch, <strong>Auto-repair</strong>{' '}
          is opt-in within a daily cap. Network tasks show an <strong>ETA</strong> for how long they
          take at the source API's rate limit. Cost tags: <em>local</em> (your database),{' '}
          <em>network</em> (OpenAlex / Crossref / Semantic Scholar), or <em>compute</em> (local
          SPECTER2).
        </p>
        <p>
          <strong>Observed — no automatic repair</strong> lists gaps that have no one-click fix.{' '}
          <strong>System status</strong> is the operational health of the running system — what's
          degraded or failing right now (monitors, sources, plugins, background jobs). Subsystem{' '}
          <em>trends and analytics</em> live under <strong>Insights → Activity</strong>.
        </p>
      </ConceptCallout>

      {/* Vitals + System status — ONE panel (the bright forefront band): the
          colored data-health ribbon up top, then a one-line strip of clickable
          system-component chips. They share a panel because together they ARE
          the at-a-glance "is everything OK?" — clicking a chip opens its detail. */}
      <section className="space-y-4 rounded-sm border border-[var(--color-border)] bg-surface-1 p-4 shadow-paper-sm sm:p-5">
        {snapshotQuery.isError ? (
          <div className="flex items-center justify-between gap-3 rounded-sm border border-critical-100 bg-critical-50 p-3">
            <div className="flex items-center gap-3">
              <AlertTriangle className="h-5 w-5 shrink-0 text-critical-600" />
              <p className="text-sm text-critical-700">Couldn't load the health snapshot.</p>
            </div>
            <Button size="sm" variant="outline" onClick={() => snapshotQuery.refetch()}>
              Retry
            </Button>
          </div>
        ) : snapshotQuery.isLoading ? (
          <div className="h-24 animate-pulse rounded-sm bg-surface-2" />
        ) : snapshot ? (
          <HealthVitals snapshot={snapshot} />
        ) : null}

        <div className="border-t border-[var(--color-border)] pt-4">
          <SectionLabel>System status</SectionLabel>
          <div className="mt-2">
            <SystemStatusCards />
          </div>
          {/* External-API budget + last credit-limit abort (task 37 B/C). */}
          <div className="mt-3">
            <ApiBudgetCard budget={operationsQuery.data?.api_budget} />
          </div>
        </div>
      </section>

      {/* The affected-papers drilldown, opened by any status row on the page. */}
      <HealthDimensionDrilldown
        dim={openDim}
        open={openDim != null}
        onOpenChange={(o) => {
          if (!o) setOpenDim(null)
        }}
        onRun={(key) => runMutation.mutate({ key })}
        runningKey={runningKey}
      />

      {/* Recommended next — the safe one-click sequence driver. The backend points
          this at the first actionable, non-blocked, non-destructive, non-manual op
          in dependency order. "Run sequence" walks the steps, advancing only when
          each launched job actually COMPLETES (H-4), and stops at a manual gate, a
          failure, or a step that can't make further progress. Shown whenever there
          IS a recommended op OR a sequence is mid-flight (so its controls/status
          stay visible even as the recommended op changes underneath it). */}
      {recommended || sequenceActive ? (
        <section className="flex flex-wrap items-center justify-between gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 p-4">
          <div>
            <SectionLabel>Recommended next</SectionLabel>
            {recommended ? (
              <p className="mt-1 text-sm text-alma-900">
                <strong>{recommended.label}</strong>
                <span className="text-slate-500"> — {recommended.reason}</span>
              </p>
            ) : (
              <p className="mt-1 text-sm text-slate-500">Re-planning…</p>
            )}
            {sequenceActive ? (
              <p className="mt-1 text-xs text-slate-500">
                {watchedJob
                  ? `Running "${watchedJob.opKey}" (${jobStatusQuery.data?.status ?? 'starting'}) — the next step waits for it to finish.`
                  : 'Running the recommended sequence — advancing as each step completes, stopping at any manual-review gate.'}
              </p>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            {sequenceActive ? (
              <Button size="sm" variant="outline" onClick={() => stopSequence()}>
                Stop auto-advance
              </Button>
            ) : recommended ? (
              <>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => runMutation.mutate({ key: recommended.key })}
                  disabled={runningKey === recommended.key}
                >
                  {runningKey === recommended.key ? 'Starting…' : 'Run step'}
                </Button>
                <Button
                  size="sm"
                  onClick={() => {
                    setSequenceActive(true)
                    runMutation.mutate({ key: recommended.key, sequence: true })
                  }}
                >
                  Run sequence
                </Button>
              </>
            ) : null}
          </div>
        </section>
      ) : null}

      {/* Repair groups — status + action in one card, in backend stage order.
          H-5: a FAILED operations plan must be distinct from "loading" and from
          "healthy / no repairs". The read-only diagnostics above still render —
          a broken repair plan never blocks the snapshot. */}
      {operationsQuery.isError ? (
        <section className="flex flex-wrap items-center justify-between gap-3 rounded-sm border border-critical-100 bg-critical-50 p-4">
          <div className="flex items-center gap-3">
            <AlertTriangle className="h-5 w-5 shrink-0 text-critical-600" />
            <div>
              <p className="text-sm font-medium text-critical-700">
                Couldn't load the repair plan.
              </p>
              <p className="text-xs text-critical-600">
                Diagnostics above are still current — only the repair operations failed to load.
              </p>
            </div>
          </div>
          <Button size="sm" variant="outline" onClick={() => operationsQuery.refetch()}>
            Retry
          </Button>
        </section>
      ) : operations.length > 0 ? (
        stageGroups.map((group) => (
          <RepairGroup key={group.key} title={group.label} ops={group.ops} {...groupProps} />
        ))
      ) : operationsQuery.isLoading ? (
        <p className="text-sm text-slate-500">Loading repair operations…</p>
      ) : (
        <p className="text-sm text-success-700">No repair operations pending — everything's clear.</p>
      )}

      {/* Observed dimensions with no repair op. */}
      <DiagnosticsSection dims={orphanDims} onOpenDim={setOpenDim} />
    </div>
  )
}
