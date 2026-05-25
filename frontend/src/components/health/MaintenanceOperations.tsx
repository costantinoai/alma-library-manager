/**
 * MaintenanceOperations — the list of maintenance tasks (run-now + auto-repair
 * + daily cap). Reads GET /health/operations; mutations are owned by the page.
 */
import { Wrench } from 'lucide-react'

import { ConceptCallout } from '@/components/ui/concept-callout'
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
      <div className="flex items-center gap-2">
        <Wrench className="h-4 w-4 text-slate-500" />
        <h2 className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500">
          Maintenance operations
        </h2>
      </div>

      <ConceptCallout
        eyebrow="How does repair work?"
        summary="Each task fixes a health gap. Run it now, or let it run automatically within a daily cap."
      >
        <p>
          Every task maps to a bounded background runner. <strong>Run now</strong> processes one
          batch immediately (sized by the daily cap). <strong>Auto-repair</strong> is opt-in and
          off by default — when enabled, the idle healer runs the task periodically without
          exceeding its daily cap, so it never floods the upstream APIs or your machine.
        </p>
        <p>
          Cost tags tell you what a task uses: <em>local</em> (your database only),{' '}
          <em>network</em> (OpenAlex / Crossref / Semantic Scholar), or <em>compute</em> (local
          CPU/GPU for SPECTER2 embeddings).
        </p>
      </ConceptCallout>

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
