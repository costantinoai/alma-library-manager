/**
 * RepairGroup — one labelled band of `RepairCard`s (e.g. "Corpus & embeddings",
 * "Authors"). Ops that need attention sort worst-first to the top as full
 * cards; ops that are healthy and idle collapse into a single quiet "All clear"
 * strip of chips. Clicking a chip expands that op's full card inline, so every
 * tool stays one click away without cluttering the calm state.
 */
import { useState } from 'react'
import { motion, useReducedMotion } from 'framer-motion'
import { CheckCircle2 } from 'lucide-react'

import type { HealthDimension, MaintenanceOperation } from '@/api/client'
import { isOpAttention, sortOpsByAttention } from './healthFormat'
import { RepairCard } from './RepairCard'
import { SectionLabel } from './SectionLabel'

interface RepairGroupProps {
  title: string
  ops: MaintenanceOperation[]
  /** Resolve the dimensions one op repairs (op.repairs ∩ snapshot.dimensions). */
  dimsOf: (op: MaintenanceOperation) => HealthDimension[]
  onRun: (key: string, params?: Record<string, unknown>) => void
  onConfig: (key: string, body: { enabled?: boolean; daily_cap?: number; batch_size?: number }) => void
  onOpenDim: (dim: HealthDimension) => void
  runningKey: string | null
}

export function RepairGroup({
  title,
  ops,
  dimsOf,
  onRun,
  onConfig,
  onOpenDim,
  runningKey,
}: RepairGroupProps) {
  const reducedMotion = useReducedMotion()
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  if (ops.length === 0) return null

  const attention = sortOpsByAttention(
    ops.filter((op) => isOpAttention(op, dimsOf(op))),
    dimsOf,
  )
  const allClear = ops.filter((op) => !isOpAttention(op, dimsOf(op)))

  // Each card rises in a short cascade; `index` paces the entrance delay.
  const card = (op: MaintenanceOperation, index: number) => (
    <motion.div
      key={op.key}
      initial={reducedMotion ? false : { opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: 'easeOut', delay: Math.min(index * 0.06, 0.3) }}
    >
      <RepairCard
        op={op}
        dims={dimsOf(op)}
        onRun={onRun}
        onConfig={onConfig}
        onOpenDim={onOpenDim}
        running={runningKey === op.key}
      />
    </motion.div>
  )

  const toggle = (key: string) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      next.has(key) ? next.delete(key) : next.add(key)
      return next
    })

  return (
    <section className="space-y-3">
      <SectionLabel>{title}</SectionLabel>

      {attention.map(card)}

      {allClear.length > 0 ? (
        <div className="space-y-3">
          <div className="flex flex-wrap items-center gap-2 rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-3 shadow-paper-sm">
            <span className="text-[11px] font-medium uppercase tracking-wide text-slate-400">
              All clear
            </span>
            {allClear.map((op) => {
              const open = expanded.has(op.key)
              return (
                <button
                  key={op.key}
                  type="button"
                  onClick={() => toggle(op.key)}
                  aria-expanded={open}
                  className={
                    'inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio ' +
                    (open
                      ? 'border-alma-300 bg-alma-50 text-alma-800'
                      : 'border-[var(--color-border)] bg-alma-content-elev text-alma-700 hover:border-alma-300')
                  }
                >
                  <CheckCircle2 className="h-3.5 w-3.5 text-emerald-600" />
                  {op.label}
                  <span className="text-slate-400">{open ? '−' : '+'}</span>
                </button>
              )
            })}
          </div>
          {/* Expanded all-clear cards (still runnable / configurable). */}
          {allClear.filter((op) => expanded.has(op.key)).map(card)}
        </div>
      ) : null}
    </section>
  )
}
