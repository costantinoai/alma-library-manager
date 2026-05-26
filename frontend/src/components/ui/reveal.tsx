import * as React from 'react'
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion'

/**
 * Reveal — the shared list/grid entrance motion ("paper settling onto the desk").
 *
 * Editorial, not bouncy: a short fade + 8px rise on an ease-out curve with no
 * overshoot and no scale-pop, lightly staggered so a grid resolves top-left to
 * bottom-right. `RevealList` provides the AnimatePresence boundary (so removals
 * animate out); `RevealItem` is the per-child motion wrapper. Entrance/exit are
 * pure transform+opacity — NO `layout` animation (that's the jank). Every
 * motion collapses to instant under `prefers-reduced-motion`.
 *
 *   <RevealList className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
 *     {items.map((it, i) => (
 *       <RevealItem key={it.id} index={i}>
 *         <PaperCard … />
 *       </RevealItem>
 *     ))}
 *   </RevealList>
 */

const EASE = [0.22, 0.61, 0.36, 1] as const

export type RevealListProps = React.HTMLAttributes<HTMLDivElement>

/** Wraps a mapped list so children fade/rise in and animate out on removal.
 * Deliberately NO `layout` / `popLayout` — those drive continuous layout
 * recalculation (the jank). Entrance/exit are pure transform+opacity (GPU). */
export function RevealList({ children, ...props }: RevealListProps) {
  return (
    <div {...props}>
      <AnimatePresence>{children}</AnimatePresence>
    </div>
  )
}

/** Per-route page entrance — a single calm fade + rise on mount. Wrap the
 * routed page and give it a `key` that changes on navigation so it replays.
 * Composes cleanly with RevealList (the page settles, then its lists stagger). */
export function PageReveal({ className, children }: { className?: string; children: React.ReactNode }) {
  const reduced = useReducedMotion()
  return (
    <motion.div
      className={className}
      initial={{ opacity: 0, y: reduced ? 0 : 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: reduced ? 0 : 0.32, ease: EASE }}
    >
      {children}
    </motion.div>
  )
}

export interface RevealItemProps {
  /** Position in the list — drives the entrance stagger delay. */
  index?: number
  /** Per-item stagger step in seconds (capped so long lists don't crawl). */
  stagger?: number
  className?: string
  children: React.ReactNode
}

/** A single list child: fade + rise in (transform/opacity only), fade out on
 * removal. No layout animation — see RevealList. */
export function RevealItem({ index = 0, stagger = 0.04, className, children }: RevealItemProps) {
  const reduced = useReducedMotion()
  return (
    <motion.div
      className={className}
      initial={{ opacity: 0, y: reduced ? 0 : 8 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: reduced ? 0 : 6 }}
      transition={{
        duration: reduced ? 0 : 0.28,
        delay: reduced ? 0 : Math.min(index * stagger, 0.24),
        ease: EASE,
      }}
    >
      {children}
    </motion.div>
  )
}
