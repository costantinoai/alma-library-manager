/**
 * MaintenanceOperations — the list of maintenance tasks (run-now + auto-repair
 * + daily cap). Reads GET /health/operations; mutations are owned by the page.
 * Rendered inside the Health page's "Maintenance" tab (which provides the
 * heading), with a short inline helper instead of a ConceptCallout so it never
 * stacks with the page-level "What is Health?" explainer.
 */
import type { MaintenanceOperation } from '@/api/client'
import { MaintenanceOperationCard } from './MaintenanceOperationCard'

interface MaintenanceOperationsProps {
  operations: MaintenanceOperation[]
  onRun: (key: string) => void
  onConfig: (key: string, body: { enabled?: boolean; daily_cap?: number }) => void
  runningKey: string | null
}

export function MaintenanceOperations({
  operations,
  onRun,
  onConfig,
  runningKey,
}: MaintenanceOperationsProps) {
  return (
    <section className="space-y-3">
      <p className="text-sm text-slate-500">
        Each task fixes a health gap via a bounded background job.{' '}
        <strong className="font-medium text-slate-600">Run now</strong> processes one batch (sized
        by the daily cap); <strong className="font-medium text-slate-600">Auto-repair</strong> is
        opt-in and off by default — when on, the idle healer runs it periodically without exceeding
        its cap. Cost tags show what a task uses: <em>local</em> (your database),{' '}
        <em>network</em> (OpenAlex / Crossref / Semantic Scholar), or <em>compute</em> (local
        SPECTER2).
      </p>

      <div className="space-y-3">
        {operations.map((op) => (
          <MaintenanceOperationCard
            key={op.key}
            op={op}
            onRun={onRun}
            onConfig={onConfig}
            running={runningKey === op.key}
          />
        ))}
      </div>
    </section>
  )
}
