import { forwardRef, useState, type ComponentType, type ReactNode } from 'react'

import { usePageTheme } from '@/components/ui/page-theme-context'
import { Surface } from '@/components/ui/surface'
import { cn } from '@/lib/utils'

export interface DisclosurePanelProps {
  /** The fold's name. Brand face, sentence case, says what is behind it. */
  title: string
  /** One quiet line under the title — the reason to open it. */
  description?: ReactNode
  /** Glyph in a soft medallion left of the title. Category, never severity. */
  icon?: ComponentType<{ className?: string }>
  /**
   * What the reader should be able to see WITHOUT opening — a chip row, a
   * count, a "nothing captured yet" line. A fold that hides its own summary
   * makes the reader open it just to learn there was no reason to.
   */
  meta?: ReactNode
  /** Initial state when the panel owns its own fold. Closed by default: a
   *  disclosure exists because the content is secondary to the page. */
  defaultOpen?: boolean
  /** Controlled state — pass with `onOpenChange` when the page persists the
   *  choice or opens the panel from elsewhere (a deep link, a button). */
  open?: boolean
  onOpenChange?: (open: boolean) => void
  children: ReactNode
  className?: string
  /** Padding override for the revealed region (default `p-4`). */
  contentClassName?: string
  /** Onboarding-tour anchor (`components/onboarding/tours.ts`). */
  'data-tour'?: string
}

/**
 * DisclosurePanel — the ONE secondary fold.
 *
 * A titled `<details>` on the paper ladder: hairline frame, brand-face title,
 * one line of description, an optional at-a-glance meta row, and a Show / Hide
 * affordance on the right. Everything a page wants to offer without spending
 * vertical space on it goes in one of these.
 *
 * It exists because the same 20 lines of markup had been pasted five times
 * (Discovery ×4, Library's "Needs attention") and had already drifted: two of
 * them centred the summary row so a two-line description dragged the Show
 * label off the baseline, and none of them hid the native `▸` marker.
 *
 * Surface is relational, so a panel inside a card is one rung lighter than the
 * card automatically — nesting a fold inside a fold cannot go cream-on-cream.
 *
 * The affordance is deliberately the word Show / Hide, not the chevron used by
 * `PageSection`: a page-level band and a secondary fold should not read as the
 * same object.
 */
export const DisclosurePanel = forwardRef<HTMLDivElement, DisclosurePanelProps>(
  function DisclosurePanel(
    {
      title,
      description,
      icon: Icon,
      meta,
      defaultOpen = false,
      open,
      onOpenChange,
      children,
      className,
      contentClassName,
      ...rest
    },
    ref,
  ) {
    // Held in React state even when uncontrolled: a native `<details>` whose
    // `open` attribute React set once diverges from the DOM the moment the
    // user clicks it, and the next re-render snaps it back.
    const pageTheme = usePageTheme()
    const [internalOpen, setInternalOpen] = useState(defaultOpen)
    const isOpen = open ?? internalOpen

    return (
      <Surface
        ref={ref}
        className={cn('overflow-hidden rounded-sm shadow-paper-sm', className)}
        {...rest}
      >
        <details
          className="group"
          open={isOpen}
          onToggle={(event) => {
            const next = (event.currentTarget as HTMLDetailsElement).open
            if (open === undefined) setInternalOpen(next)
            onOpenChange?.(next)
          }}
        >
          <summary className="flex cursor-pointer select-none list-none items-start justify-between gap-4 px-4 py-3 text-left [&::-webkit-details-marker]:hidden">
            <div className="flex min-w-0 items-start gap-3">
              {Icon && (
                <span
                  className={cn(
                    'mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full',
                    pageTheme
                      ? `${pageTheme.medallion} ${pageTheme.icon}`
                      : 'bg-accent-soft text-alma-folio',
                  )}
                >
                  <Icon className="h-4 w-4" />
                </span>
              )}
              <div className="flex min-w-0 flex-col gap-1.5">
                <div className="flex flex-col gap-0.5">
                  <span className="font-brand text-sm font-semibold text-alma-800">
                    {title}
                  </span>
                  {description && (
                    <span className="text-xs text-slate-500">{description}</span>
                  )}
                </div>
                {meta}
              </div>
            </div>
            <span className="shrink-0 text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">
              Show
            </span>
            <span className="hidden shrink-0 text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">
              Hide
            </span>
          </summary>
          <div
            className={cn(
              'border-t border-[var(--color-border)] p-4',
              contentClassName,
            )}
          >
            {children}
          </div>
        </details>
      </Surface>
    )
  },
)
