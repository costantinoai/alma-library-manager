/**
 * HealthPage — the front door for "is my data healthy, and what do I do about
 * it?" Consolidates the canonical health layer (task 24) into one surface:
 * a vitals hero, the Data Health dimension cards, and the Maintenance
 * Operations controls. Every number reads the canonical endpoints
 * (/insights/health + /health/operations) — one source of truth.
 */
import { AlertTriangle, RefreshCw } from 'lucide-react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  getHealthOperations,
  getHealthSnapshot,
  runMaintenanceOperation,
  setMaintenanceConfig,
} from '@/api/client'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { Button } from '@/components/ui/button'
import { JargonHint } from '@/components/shared/JargonHint'
import { HealthVitals } from '@/components/health/HealthVitals'
import { DataHealthSection } from '@/components/health/DataHealthSection'
import { MaintenanceOperations } from '@/components/health/MaintenanceOperations'
import { invalidateQueries } from '@/lib/queryHelpers'
import { formatRelativeShort } from '@/lib/utils'
import { useToast, errorToast } from '@/hooks/useToast'

const SNAPSHOT_KEY = ['health', 'snapshot']
const OPERATIONS_KEY = ['health', 'operations']

export function HealthPage() {
  const queryClient = useQueryClient()
  const { toast } = useToast()

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

  const runMutation = useMutation({
    mutationFn: (key: string) => runMaintenanceOperation(key),
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
    onError: (err) => errorToast('Could not start maintenance', String(err)),
  })

  const configMutation = useMutation({
    mutationFn: ({ key, body }: { key: string; body: { enabled?: boolean; daily_cap?: number } }) =>
      setMaintenanceConfig(key, body),
    onSuccess: async () => {
      await invalidateQueries(queryClient, OPERATIONS_KEY)
    },
    onError: (err) => errorToast('Could not update setting', String(err)),
  })

  const refresh = () => invalidateQueries(queryClient, SNAPSHOT_KEY, OPERATIONS_KEY)

  const runningKey = runMutation.isPending ? (runMutation.variables ?? null) : null
  const snapshot = snapshotQuery.data
  const operations = operationsQuery.data?.operations ?? []

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
              {snapshot.rebuilding ? ' · updating…' : ''}
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
        summary="ALMa watches your corpus for fixable gaps and offers one-click or automatic repairs."
      >
        <p>
          A <strong>dimension</strong> is one measurable aspect of corpus health — missing
          abstracts, embedding{' '}
          <JargonHint
            title="Coverage"
            description="The share of papers that have an embedding vector for the active model. Discovery's semantic ranking depends on it."
            className="inline-flex"
          />{' '}
          coverage, unresolved identities, and so on. Each carries a severity, a plain-language
          explanation, and the actions that fix it.
        </p>
        <p>
          <strong>Enrichment</strong> fills missing metadata from OpenAlex / Crossref;{' '}
          <strong>maintenance operations</strong> are the bounded background jobs that do the
          fixing. They can run on demand or — opt-in — automatically within a daily cap.
        </p>
      </ConceptCallout>

      {/* Error */}
      {snapshotQuery.isError ? (
        <div className="flex items-center justify-between gap-3 rounded-sm border border-rose-200 bg-rose-50 p-4">
          <div className="flex items-center gap-3">
            <AlertTriangle className="h-5 w-5 shrink-0 text-rose-600" />
            <p className="text-sm text-rose-800">Couldn't load the health snapshot.</p>
          </div>
          <Button size="sm" variant="outline" onClick={() => snapshotQuery.refetch()}>
            Retry
          </Button>
        </div>
      ) : null}

      {/* Loading skeleton */}
      {snapshotQuery.isLoading ? (
        <div className="space-y-6">
          <div className="h-40 animate-pulse rounded-sm bg-alma-chrome-elev" />
          <div className="grid gap-3 md:grid-cols-2">
            <div className="h-36 animate-pulse rounded-sm bg-alma-chrome-elev" />
            <div className="h-36 animate-pulse rounded-sm bg-alma-chrome-elev" />
          </div>
        </div>
      ) : null}

      {/* Content */}
      {snapshot ? <HealthVitals snapshot={snapshot} /> : null}
      {snapshot ? (
        <DataHealthSection
          dimensions={snapshot.dimensions}
          onRun={(key) => runMutation.mutate(key)}
          runningKey={runningKey}
        />
      ) : null}
      {operations.length > 0 ? (
        <MaintenanceOperations
          operations={operations}
          onRun={(key) => runMutation.mutate(key)}
          onConfig={(key, body) => configMutation.mutate({ key, body })}
          runningKey={runningKey}
        />
      ) : null}
    </div>
  )
}
