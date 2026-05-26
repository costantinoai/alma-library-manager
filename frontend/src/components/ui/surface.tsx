import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * Surface — the relational elevation primitive ("one neutral paper ladder").
 *
 * The whole surface-contrast system rests on one idea: **depth alone decides
 * the color.** A surface at level N is always the same hex, everywhere. Rather
 * than every component picking a background by hand (the old failure mode that
 * produced cream-on-cream), elevation is tracked in React context and computed
 * relationally: a surface renders one level lighter than whatever it sits on,
 * and hands that new level down to its own children. Nesting therefore climbs
 * the ladder automatically and can never reverse.
 *
 *   level 0  the desk / app body          surface-0  #F1EAD8
 *   level 1  card · dialog/sheet body      surface-1  #F8F4E8
 *   level 2  panel in a card · table head  surface-2  #FBF8EE
 *   level 3  nested well · popover/menu     surface-3  #FFFEF9
 *   level 4  top of stack (toast)           surface-4  #FFFFFF
 *
 * `Card` and `SubPanel` are thin wrappers over `<Surface>` that add shadow
 * semantics (lift vs. inset). Portals (dialog, popover, …) reset the counter
 * to a fixed base via `<SurfaceProvider>` so their contents climb from a sane
 * level instead of from 0.
 */

/** Elevation depth on the single neutral paper ladder. */
export type SurfaceLevel = 0 | 1 | 2 | 3 | 4

/* Context default is 0 (the desk). A surface with no provider above it
 * therefore lifts to level 1 — the common "card on the body" case — with no
 * ceremony. */
const SurfaceLevelContext = React.createContext<SurfaceLevel>(0)

/** Current elevation level of the surface this component renders inside. */
export function useSurfaceLevel(): SurfaceLevel {
  return React.useContext(SurfaceLevelContext)
}

/** One step lighter, saturating at the top of the ladder. Deep nesting stops
 * at level 4 rather than overflowing into an undefined class. */
export function nextLevel(n: SurfaceLevel): SurfaceLevel {
  return Math.min(n + 1, 4) as SurfaceLevel
}

/* ── Static class maps ────────────────────────────────────────────────────
 * Tailwind v4 only emits a utility whose COMPLETE class string appears in a
 * scanned source file. A template literal like `bg-surface-${level}` is never
 * seen by the scanner, so it gets purged and the element silently renders with
 * no background. Keeping every class a literal here is what makes the ladder
 * actually exist at runtime. Do NOT refactor these into interpolation. */
export const SURFACE_BG: Record<SurfaceLevel, string> = {
  0: 'bg-surface-0',
  1: 'bg-surface-1',
  2: 'bg-surface-2',
  3: 'bg-surface-3',
  4: 'bg-surface-4',
}

export const SURFACE_BORDER: Record<SurfaceLevel, string> = {
  0: 'border-edge-0',
  1: 'border-edge-1',
  2: 'border-edge-2',
  3: 'border-edge-3',
  4: 'border-edge-4',
}

/**
 * SurfaceProvider — set the elevation level for a subtree WITHOUT painting
 * anything. Used by portals (dialog, popover, …) to reset the counter to a
 * fixed base so their contents climb from there. Rarely needed in app code.
 */
export function SurfaceProvider({
  level,
  children,
}: {
  level: SurfaceLevel
  children: React.ReactNode
}) {
  return (
    <SurfaceLevelContext.Provider value={level}>{children}</SurfaceLevelContext.Provider>
  )
}

export interface SurfaceProps extends React.HTMLAttributes<HTMLDivElement> {
  /** Force an absolute level instead of the relational host+1. */
  level?: SurfaceLevel
  /** Render at the host's level (no bump) — paints at the same depth, still
   * provides that level downward. For grouping wrappers that shouldn't burn a
   * rung of the ladder. */
  asChildLevel?: boolean
  /** Paint the background fill. Off → transparent (inherits the host surface). */
  filled?: boolean
  /** Render the paired hairline border. */
  bordered?: boolean
}

/**
 * Surface — low-level relational surface. Reads the host level, renders at
 * `min(host+1, 4)` (or an explicit `level`), paints `bg-surface-N` +
 * `border-edge-N` from the static maps, and provides its rendered level to
 * descendants. Build `Card` / `SubPanel` on top of this; reach for it directly
 * only for one-off wells that aren't either of those.
 */
export const Surface = React.forwardRef<HTMLDivElement, SurfaceProps>(
  (
    { level, asChildLevel, filled = true, bordered = true, className, children, ...props },
    ref,
  ) => {
    const host = useSurfaceLevel()
    const rendered: SurfaceLevel = level ?? (asChildLevel ? host : nextLevel(host))
    return (
      <SurfaceLevelContext.Provider value={rendered}>
        <div
          ref={ref}
          className={cn(
            bordered && 'border',
            bordered && SURFACE_BORDER[rendered],
            filled && SURFACE_BG[rendered],
            className,
          )}
          {...props}
        >
          {children}
        </div>
      </SurfaceLevelContext.Provider>
    )
  },
)
Surface.displayName = 'Surface'
