/**
 * HealthVitals — the page's signature element: a thin "vitals ribbon"
 * (stacked severity bar across all dimensions) over a centered MetricTile
 * scoreboard. Sits on the brightest surface (alma-content) because it's the
 * most-forefront band on the page ("more forefront = lighter").
 *
 * The ribbon is the one place semantic color spans width — the controlled
 * exception to the "calm off-white" rule, justified because it IS the triage.
 * On load the segments grow from zero and the scoreboard tiles rise in a short
 * stagger — the "vital signs coming online" beat — honoring prefers-reduced-motion.
 */
import { motion, useReducedMotion } from 'framer-motion'

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
  const reducedMotion = useReducedMotion()
  const totals = snapshot.totals
  const bySeverity = totals.dimensions_by_severity ?? {}
  const totalDims = RIBBON_SEGMENTS.reduce((sum, s) => sum + (bySeverity[s.key] ?? 0), 0)

  const coverage = Math.round(totals.embedding_coverage_pct ?? 0)

  const tiles = [
    { label: 'Critical', value: bySeverity.critical ?? 0, tone: 'critical' as const, hint: undefined, suffix: undefined },
    { label: 'Warnings', value: bySeverity.warning ?? 0, tone: 'warning' as const, hint: undefined, suffix: undefined },
    {
      label: 'Embedding coverage',
      value: `${coverage}%`,
      tone: (totals.embeddings_ready ? 'success' : 'warning') as 'success' | 'warning',
      hint: totals.embeddings_ready ? 'ready' : 'ready at ≥80%',
      suffix: (
        <JargonHint
          title="Embedding coverage"
          description="Share of papers that have a vector for the active embedding model. Discovery similarity and the paper map need high coverage; readiness flips on at 80%."
        />
      ),
    },
    { label: 'Papers', value: totals.papers_total, tone: 'neutral' as const, hint: undefined, suffix: undefined },
  ]

  return (
    <section className="rounded-sm border border-[var(--color-border)] bg-alma-content p-4 shadow-paper-sm sm:p-5">
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
      {/* Ribbon legend */}
      <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-slate-500">
        {RIBBON_SEGMENTS.map((seg) => (
          <span key={seg.key} className="inline-flex items-center gap-1.5">
            <span className={`h-2 w-2 rounded-full ${seg.className}`} />
            {seg.label} {bySeverity[seg.key] ?? 0}
          </span>
        ))}
      </div>

      {/* Scoreboard — short staggered rise on load. */}
      <div className="mt-4 grid grid-cols-2 gap-3 lg:grid-cols-4">
        {tiles.map((tile, i) => (
          <motion.div
            key={tile.label}
            initial={reducedMotion ? false : { opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.4, ease: 'easeOut', delay: 0.25 + 0.06 * i }}
          >
            <MetricTile
              label={tile.label}
              value={tile.value}
              tone={tile.tone}
              align="center"
              hint={tile.hint}
              labelSuffix={tile.suffix}
            />
          </motion.div>
        ))}
      </div>
    </section>
  )
}
