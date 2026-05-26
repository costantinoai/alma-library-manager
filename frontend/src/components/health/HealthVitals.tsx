/**
 * HealthVitals — the page's signature element: a thin "vitals ribbon" (a
 * stacked severity bar across every data dimension) with a legend and a slim
 * KPI caption. Sits on the brightest surface (alma-content) because it's the
 * most-forefront band on the page ("more forefront = lighter").
 *
 * The ribbon is the one place semantic color spans width — the controlled
 * exception to the "calm off-white" rule, justified because it IS the triage.
 * The per-dimension counts (critical / warnings) live IN the ribbon + legend;
 * the redundant scoreboard tiles were folded away (the operational glance now
 * lives in the one-line System status strip that shares this panel). On load
 * the segments grow from zero — the "vital signs coming online" beat —
 * honoring prefers-reduced-motion.
 *
 * Renders bare content (no card chrome): `HealthPage` wraps this and the
 * System status strip in ONE panel so the ribbon and the component glance read
 * as a single band.
 */
import { motion, useReducedMotion } from 'framer-motion'

import { JargonHint } from '@/components/shared/JargonHint'
import type { HealthSnapshot } from '@/api/client'

// Severity → ribbon segment color. Muted weights keep it a calm vitals strip,
// not an alarm board.
const RIBBON_SEGMENTS: { key: string; className: string; label: string }[] = [
  { key: 'critical', className: 'bg-critical-600', label: 'Critical' },
  { key: 'warning', className: 'bg-warning-500', label: 'Warning' },
  { key: 'info', className: 'bg-alma-folio', label: 'Info' },
  { key: 'ok', className: 'bg-success-600', label: 'Healthy' },
]

export function HealthVitals({ snapshot }: { snapshot: HealthSnapshot }) {
  const reducedMotion = useReducedMotion()
  const totals = snapshot.totals
  const bySeverity = totals.dimensions_by_severity ?? {}
  const totalDims = RIBBON_SEGMENTS.reduce((sum, s) => sum + (bySeverity[s.key] ?? 0), 0)
  const coverage = Math.round(totals.embedding_coverage_pct ?? 0)

  return (
    <div>
      {/* Vitals ribbon */}
      <div
        className="flex h-2 w-full overflow-hidden rounded-full bg-alma-100"
        role="img"
        aria-label={`${bySeverity.critical ?? 0} critical, ${bySeverity.warning ?? 0} warning, ${bySeverity.info ?? 0} info, ${bySeverity.ok ?? 0} healthy dimensions`}
      >
        {totalDims > 0 &&
          RIBBON_SEGMENTS.map((seg, i) => {
            const count = bySeverity[seg.key] ?? 0
            if (count <= 0) return null
            const width = `${(count / totalDims) * 100}%`
            return (
              <motion.div
                key={seg.key}
                className={seg.className}
                initial={reducedMotion ? false : { width: 0 }}
                animate={{ width }}
                transition={{ duration: 0.7, ease: [0.16, 1, 0.3, 1], delay: 0.05 * i }}
                title={`${count} ${seg.label}`}
              />
            )
          })}
      </div>

      {/* Ribbon legend — the per-severity dimension counts. */}
      <div className="mt-2.5 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-slate-500">
        {RIBBON_SEGMENTS.map((seg) => (
          <span key={seg.key} className="inline-flex items-center gap-1.5">
            <span className={`h-2 w-2 rounded-full ${seg.className}`} />
            {seg.label} <span className="font-medium tabular-nums text-alma-700">{bySeverity[seg.key] ?? 0}</span>
          </span>
        ))}
      </div>

      {/* Slim KPI caption — the two corpus facts worth a glance, no tile grid. */}
      <div className="mt-3 flex flex-wrap items-center gap-x-2 gap-y-1 border-t border-[var(--color-border)] pt-3 text-xs text-slate-500">
        <span className="tabular-nums text-alma-700">{totals.papers_total.toLocaleString()}</span>
        <span>papers assessed</span>
        <span className="text-alma-200">·</span>
        <span className="tabular-nums text-alma-700">{coverage}%</span>
        <span className="inline-flex items-center gap-1">
          embedding coverage
          <JargonHint
            title="Embedding coverage"
            description="Share of papers that have a vector for the active embedding model. Discovery similarity and the paper map need high coverage; readiness flips on at 80%."
          />
        </span>
        <span
          className={
            totals.embeddings_ready
              ? 'rounded-full bg-success-700/10 px-2 py-0.5 text-[10px] font-medium text-success-700'
              : 'rounded-full bg-warning-700/10 px-2 py-0.5 text-[10px] font-medium text-warning-700'
          }
        >
          {totals.embeddings_ready ? 'ready' : 'ready at ≥80%'}
        </span>
      </div>
    </div>
  )
}
