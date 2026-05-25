/**
 * HealthVitals — the page's signature element: a thin "vitals ribbon"
 * (stacked severity bar across all dimensions) over a centered MetricTile
 * scoreboard. Sits on the brightest surface (alma-content) because it's the
 * most-forefront band on the page ("more forefront = lighter").
 *
 * The ribbon is the one place semantic color spans width — the controlled
 * exception to the "calm off-white" rule, justified because it IS the triage.
 */
import { MetricTile } from '@/components/shared/MetricTile'
import { JargonHint } from '@/components/shared/JargonHint'
import type { HealthSnapshot } from '@/api/client'

// Severity → ribbon segment color. Muted weights keep it a calm vitals strip,
// not an alarm board.
const RIBBON_SEGMENTS: { key: string; className: string; label: string }[] = [
  { key: 'critical', className: 'bg-rose-600', label: 'Critical' },
  { key: 'warning', className: 'bg-amber-500', label: 'Warning' },
  { key: 'info', className: 'bg-alma-folio', label: 'Info' },
  { key: 'ok', className: 'bg-emerald-600', label: 'Healthy' },
]

export function HealthVitals({ snapshot }: { snapshot: HealthSnapshot }) {
  const totals = snapshot.totals
  const bySeverity = totals.dimensions_by_severity ?? {}
  const totalDims = RIBBON_SEGMENTS.reduce((sum, s) => sum + (bySeverity[s.key] ?? 0), 0)

  const coverage = Math.round(totals.embedding_coverage_pct ?? 0)

  return (
    <section className="rounded-sm border border-[var(--color-border)] bg-alma-content p-4 shadow-paper-sm sm:p-5">
      {/* Vitals ribbon */}
      <div
        className="flex h-2 w-full overflow-hidden rounded-full bg-alma-100"
        role="img"
        aria-label={`${bySeverity.critical ?? 0} critical, ${bySeverity.warning ?? 0} warning, ${bySeverity.info ?? 0} info, ${bySeverity.ok ?? 0} healthy dimensions`}
      >
        {totalDims > 0 &&
          RIBBON_SEGMENTS.map((seg) => {
            const count = bySeverity[seg.key] ?? 0
            if (count <= 0) return null
            return (
              <div
                key={seg.key}
                className={seg.className}
                style={{ width: `${(count / totalDims) * 100}%` }}
                title={`${count} ${seg.label}`}
              />
            )
          })}
      </div>
      {/* Ribbon legend */}
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-slate-500">
        {RIBBON_SEGMENTS.map((seg) => (
          <span key={seg.key} className="inline-flex items-center gap-1.5">
            <span className={`h-2 w-2 rounded-full ${seg.className}`} />
            {seg.label} {bySeverity[seg.key] ?? 0}
          </span>
        ))}
      </div>

      {/* Scoreboard */}
      <div className="mt-4 grid grid-cols-2 gap-3 lg:grid-cols-4">
        <MetricTile
          label="Critical"
          value={bySeverity.critical ?? 0}
          tone="critical"
          align="center"
        />
        <MetricTile
          label="Warnings"
          value={bySeverity.warning ?? 0}
          tone="warning"
          align="center"
        />
        <MetricTile
          label="Embedding coverage"
          value={`${coverage}%`}
          tone={totals.embeddings_ready ? 'success' : 'warning'}
          align="center"
          hint={totals.embeddings_ready ? 'ready' : 'ready at ≥80%'}
          labelSuffix={
            <JargonHint
              title="Embedding coverage"
              description="Share of papers that have a vector for the active embedding model. Discovery similarity and the paper map need high coverage; readiness flips on at 80%."
            />
          }
        />
        <MetricTile label="Papers" value={totals.papers_total} tone="neutral" align="center" />
      </div>
    </section>
  )
}
