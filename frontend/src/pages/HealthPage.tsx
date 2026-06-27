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
  // "Run recommended sequence" auto-advance state. `sequenceActive` turns on a
  // refetch loop; `lastFiredRef` records the op we last launched so we never
  // re-fire the same step while it is still running (the backend only advances
  // `recommended_next` once a step's pending drops, i.e. it completed).
  const [sequenceActive, setSequenceActive] = useState(false)
  const lastFiredRef = useRef<string | null>(null)

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
    // While a sequence runs, re-plan on a cadence so `recommended_next` advances
    // as each background step finishes.
    refetchInterval: sequenceActive ? 3000 : false,
  })

  const runMutation = useMutation({
    // The atomic Run spec travels with the click (RepairCard builds it from its
    // visible controls). A bare `{ key }` — e.g. a drilldown "fix all" — sends no
    // max_items, so the backend applies the task's remembered manual limit.
    mutationFn: ({ key, request }: { key: string; request?: MaintenanceRunRequest }) =>
      runMaintenanceOperation(key, request ?? {}),
    onSuccess: async (result) => {
      await invalidateQueries(queryClient, OPERATIONS_KEY, SNAPSHOT_KEY)
      if (result.status === 'noop' || !result.job_id) {
        toast({ title: 'Nothing to run', description: 'No provider or no eligible items.' })
      } else {
        toast({
          title: 'Maintenance started',
          description: `${result.key} queued (${result.job_id}). Track it in Activity.`,
        })
      }
    },
    onError: (err) => {
      // A failed step stops the sequence (don't loop on a broken step).
      setSequenceActive(false)
      lastFiredRef.current = null
      errorToast('Could not start maintenance', String(err))
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

  // Auto-advance the recommended sequence (Checkpoint G). No per-job polling: the
  // backend only moves `recommended_next` to the NEXT op once the current one's
  // pending drops (it finished), so firing whenever the recommended key CHANGES
  // walks one safe step at a time and naturally stops when `recommended_next`
  // becomes null (everything healthy, or only manual-review gates remain).
  useEffect(() => {
    if (!sequenceActive || runMutation.isPending) return
    if (!recommended) {
      setSequenceActive(false)
      lastFiredRef.current = null
      toast({
        title: 'Recommended sequence finished',
        description: 'No further safe steps — stopped (any manual-review gates remain for you).',
      })
      return
    }
    if (recommended.key !== lastFiredRef.current) {
      lastFiredRef.current = recommended.key
      runMutation.mutate({ key: recommended.key })
    }
  }, [sequenceActive, recommended, runMutation.isPending])

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

      {/* Recommended next — the safe one-click sequence driver. The backend only
          ever points this at the first actionable, non-blocked, non-destructive,
          non-manual op in dependency order, so running it repeatedly walks the
          repair sequence and stops at a manual-review gate (re-planned on each
          refetch). */}
      {recommended ? (
        <section className="flex flex-wrap items-center justify-between gap-3 rounded-sm border border-[var(--color-border)] bg-surface-2 p-4">
          <div>
            <SectionLabel>Recommended next</SectionLabel>
            <p className="mt-1 text-sm text-alma-900">
              <strong>{recommended.label}</strong>
              <span className="text-slate-500"> — {recommended.reason}</span>
            </p>
            {sequenceActive ? (
              <p className="mt-1 text-xs text-slate-500">
                Running the recommended sequence — advancing as each step completes, stopping at any
                manual-review gate.
              </p>
            ) : null}
          </div>
          <div className="flex items-center gap-2">
            {sequenceActive ? (
              <Button
                size="sm"
                variant="outline"
                onClick={() => {
                  setSequenceActive(false)
                  lastFiredRef.current = null
                }}
              >
                Stop
              </Button>
            ) : (
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
                    lastFiredRef.current = null
                    setSequenceActive(true)
                  }}
                >
                  Run sequence
                </Button>
              </>
            )}
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
