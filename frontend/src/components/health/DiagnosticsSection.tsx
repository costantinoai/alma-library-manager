/**
 * DiagnosticsSection — the health dimensions that have NO repair operation
 * (e.g. canonical orphans, retry-ledger waits, authors needing review). They're
 * surfaced for visibility only: no Run button, because there's no backend op to
 * fire (Truthful UI). Attention dimensions render as status rows (a few are
 * drilldown-backed, like the retry ledger); healthy ones collapse into a quiet
 * chip strip. The whole section hides when there are no observed dimensions.
 */
import { CheckCircle2 } from 'lucide-react'

import type { HealthDimension } from '@/api/client'
import { sortBySeverity } from './healthFormat'
import { DimensionStatusRow } from './DimensionStatusRow'
import { SectionLabel } from './SectionLabel'

interface DiagnosticsSectionProps {
  dims: HealthDimension[]
  onOpenDim: (dim: HealthDimension) => void
}

export function DiagnosticsSection({ dims, onOpenDim }: DiagnosticsSectionProps) {
  if (dims.length === 0) return null
  const attention = sortBySeverity(dims.filter((d) => d.severity !== 'ok'))
  const healthy = dims.filter((d) => d.severity === 'ok')

  return (
    <section className="space-y-3">
      <SectionLabel>Observed — no automatic repair</SectionLabel>
      <p className="text-sm text-slate-500">
        Surfaced for visibility. These have no one-click repair — they clear on their own or are
        handled elsewhere (author tools, the retry ledger).
      </p>

      {attention.length > 0 ? (
        <div className="space-y-1.5">
          {attention.map((dim) => (
            <DimensionStatusRow key={dim.key} dim={dim} onOpen={() => onOpenDim(dim)} />
          ))}
        </div>
      ) : null}

      {healthy.length > 0 ? (
        <div className="flex flex-wrap items-center gap-2 rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-3 shadow-paper-sm">
          <span className="text-[11px] font-medium uppercase tracking-wide text-slate-400">
            All clear
          </span>
          {healthy.map((dim) => (
            <span
              key={dim.key}
              className="inline-flex items-center gap-1.5 rounded-full border border-[var(--color-border)] bg-alma-content-elev px-2.5 py-1 text-xs text-alma-700"
            >
              <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600" />
              {dim.label}
            </span>
          ))}
        </div>
      ) : null}
    </section>
  )
}
