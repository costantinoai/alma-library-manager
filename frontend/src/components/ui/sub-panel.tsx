import * as React from 'react'
import { cn } from '@/lib/utils'

/**
 * SubPanel — recessed inner frame for grouping content inside a Card
 * (v3, "two papers, one desk").
 *
 * The Card primitive carries the paper-sheet shadow that lifts off the
 * desk; SubPanel does the opposite — it presses INTO the card. A soft
 * inset shadow + a fainter border + a parchment tint makes the panel
 * read as if it were stamped into the page, like a chapter epigraph
 * box or a sidebar in an old folio book. Two levels of physical
 * hierarchy — the card lifts, the sub-panel recesses — give pages a
 * quiet "rooms within rooms" depth without resorting to bright fills
 * or heavy borders.
 *
 * Tones (v3 split — match the Card tone the SubPanel sits inside):
 *   - `content` (default) — warm parchment tint with the inset paper
 *     shadow. Use INSIDE content cards: drop-cap abstract frames,
 *     citation snippets, score breakdowns nested in paper-detail.
 *     Reads as "the page beneath the page."
 *   - `chrome`            — neutral chrome-elev fill, hairline border,
 *     no inset shadow. Use INSIDE chrome cards: grouping form sections
 *     in settings, sub-config blocks. Reads as "same working paper,
 *     just boxed for grouping" — flat by design.
 *   - `ops`               — slate fill + cool inset shadow. The
 *     non-brand utility surface — used inside the Activity panel
 *     where the palette is intentionally white/grey to feel like
 *     system telemetry, not a reading surface.
 *   - `accent`            — soft Folio-blue tint. For highlighted
 *     info ("top contributor", "selected lane", "branch winner").
 *     Sparingly; the accent should remain rare so it stays
 *     meaningful when it appears.
 *
 * Deprecated aliases (kept temporarily for the v2 → v3 sweep):
 *   `parchment` → `content`, `paper` → `chrome`, `cool` → `ops`.
 */
type SubPanelTone =
  | 'content'
  | 'chrome'
  | 'ops'
  | 'accent'
  /** @deprecated v2 alias → 'content' */
  | 'parchment'
  /** @deprecated v2 alias → 'chrome' */
  | 'paper'
  /** @deprecated v2 alias → 'ops' */
  | 'cool'

const TONE_CLASSES: Record<SubPanelTone, string> = {
  content:
    'bg-parchment-50/85 border-parchment-300/60 shadow-paper-inset',
  chrome:
    'bg-alma-chrome-elev border-[var(--color-border-cool)]',
  ops:
    'bg-slate-50 border-slate-200 shadow-paper-inset-cool',
  accent:
    'bg-alma-folio-soft border-[#9FC3DD]/70',
  // deprecated aliases — same tone as their canonical replacement
  parchment:
    'bg-parchment-50/85 border-parchment-300/60 shadow-paper-inset',
  paper:
    'bg-alma-chrome-elev border-[var(--color-border-cool)]',
  cool:
    'bg-slate-50 border-slate-200 shadow-paper-inset-cool',
}

interface SubPanelProps extends React.HTMLAttributes<HTMLDivElement> {
  tone?: SubPanelTone
  /** Apply default p-4 spacing. Disable for tight compositions. */
  padded?: boolean
}

const SubPanel = React.forwardRef<HTMLDivElement, SubPanelProps>(
  ({ className, tone = 'content', padded = true, ...props }, ref) => (
    <div
      ref={ref}
      className={cn(
        'rounded-sm border',
        padded && 'p-4',
        TONE_CLASSES[tone],
        className,
      )}
      {...props}
    />
  ),
)
SubPanel.displayName = 'SubPanel'

export { SubPanel }
export type { SubPanelTone }
