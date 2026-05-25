/**
 * DataHealthSection — splits the canonical dimensions into two zones:
 *  - "Needs attention" (severity != ok): full HealthDimensionCards on the
 *    cool alma-50 ramp, sorted worst-first.
 *  - "All clear" (severity == ok): collapsed into one quiet warm chrome-elev
 *    card listing the healthy dimensions as chips, so the page stays focused
 *    on what actually needs action.
 */
import { CheckCircle2, ShieldCheck } from 'lucide-react'

import { StatusBadge } from '@/components/ui/status-badge'
import type { HealthDimension } from '@/api/client'
import { HealthDimensionCard } from './HealthDimensionCard'
import { isAttention, sortBySeverity } from './healthFormat'

interface DataHealthSectionProps {
  dimensions: HealthDimension[]
  onRun: (operationKey: string) => void
  runningKey: string | null
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <h2 className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500">{children}</h2>
  )
}

export function DataHealthSection({ dimensions, onRun, runningKey }: DataHealthSectionProps) {
  const attention = sortBySeverity(dimensions.filter(isAttention))
  const healthy = dimensions.filter((d) => !isAttention(d))

  return (
    <div className="space-y-6">
      {/* Needs attention */}
      <section className="space-y-3">
        <SectionLabel>Needs attention</SectionLabel>
        {attention.length > 0 ? (
          <div className="grid gap-3 md:grid-cols-2">
            {attention.map((dim) => (
              <HealthDimensionCard
                key={dim.key}
                dim={dim}
                onRun={onRun}
                runningKey={runningKey}
              />
            ))}
          </div>
        ) : (
          <div className="flex items-center gap-3 rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-4 shadow-paper-sm">
            <ShieldCheck className="h-5 w-5 shrink-0 text-emerald-600" />
            <div>
              <p className="font-medium text-alma-800">All systems healthy</p>
              <p className="text-sm text-slate-500">
                Every tracked health dimension is in good shape. Nothing needs your attention.
              </p>
            </div>
          </div>
        )}
      </section>

      {/* All clear */}
      {healthy.length > 0 ? (
        <section className="space-y-3">
          <SectionLabel>All clear</SectionLabel>
          <div className="rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-4 shadow-paper-sm">
            <div className="flex flex-wrap gap-2">
              {healthy.map((dim) => (
                <StatusBadge key={dim.key} tone="neutral" className="gap-1.5">
                  <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600" />
                  {dim.label}
                </StatusBadge>
              ))}
            </div>
          </div>
        </section>
      ) : null}
    </div>
  )
}
