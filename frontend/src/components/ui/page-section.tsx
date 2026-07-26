import { useState, type ComponentType, type ReactNode } from 'react'
import { ChevronDown } from 'lucide-react'

import { Card } from '@/components/ui/card'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { StatusBadge } from '@/components/ui/status-badge'
import { cn } from '@/lib/utils'

export interface PageSectionProps {
  /** Anchor for `aria-labelledby`; also the heading's DOM id. */
  id: string
  title: string
  /**
   * Category glyph beside the title — what this band is ABOUT.
   *
   * One flat folio tint for every section, deliberately: on a page where
   * colour already means valence (chips, severity rows, action buttons), a
   * per-section hue would be a second, contradictory code. The icon carries
   * category; the accent carries "this is a heading".
   */
  icon?: ComponentType<{ className?: string }>
  /**
   * How many items the section holds, as a pill beside the title.
   *
   * The count is the WHOLE set, not what happens to be rendered — the point is
   * to tell the reader how deep the section goes before they scan it. Omit it
   * when the number is unknown or meaningless; a pill reading "0" would be an
   * empty section that should not have rendered at all.
   */
  count?: number
  /** One quiet line under the title. Say what the section contains. */
  description?: ReactNode
  /** Right-aligned control on the title row — usually a link to the owner page. */
  action?: ReactNode
  /**
   * Give the section its own collapsible block: a `Card` (so it reads as a
   * discrete sheet with real depth) whose header row toggles the content.
   *
   * Use it for the long content bands a reader may want to fold away — a page
   * of three tile grids is a lot of scroll if only one of them is today's
   * business. Leave it off for a short band that lives INSIDE another card
   * (Home's activity strip on the blotter), where a second frame would be a box
   * in a box.
   */
  collapsible?: boolean
  /** Whether a collapsible section starts open. Default open — a section that
   *  hides its content by default reads as missing. */
  defaultOpen?: boolean
  children: ReactNode
  className?: string
}

/**
 * PageSection — the top-level band on a page: heading, one line of context,
 * an optional owner-page action, and its content.
 *
 * This is the page-level counterpart to `SectionHeader` (which is a card's
 * internal header). It exists so a page never hand-rolls the h2 + description
 * + action row: the type scale, the spacing rhythm, the collapse affordance and
 * the heading/landmark wiring are decided once here.
 */
export function PageSection({
  id,
  title,
  icon: Icon,
  count,
  description,
  action,
  collapsible = false,
  defaultOpen = true,
  children,
  className,
}: PageSectionProps) {
  const [open, setOpen] = useState(defaultOpen)

  // The pill is a SIBLING of the heading, never inside it: nesting it would
  // fold the number into the heading's accessible name, so "Inbox" would
  // announce and be findable only as "Inbox 4".
  const heading = (
    <div className="min-w-0">
      <div className="flex items-center gap-2">
        {Icon && (
          <Icon className="h-[1.15rem] w-[1.15rem] shrink-0 text-alma-folio" aria-hidden />
        )}
        <h2 id={id} className="font-brand text-lg font-semibold text-alma-800">
          {title}
        </h2>
        {count != null && count > 0 && (
          <StatusBadge
            tone="neutral"
            size="sm"
            className="font-mono tabular-nums"
            aria-label={`${count} in total`}
          >
            {count}
          </StatusBadge>
        )}
      </div>
      {description && <p className="text-sm text-slate-500">{description}</p>}
    </div>
  )

  if (!collapsible) {
    return (
      <section className={cn('space-y-3', className)} aria-labelledby={id}>
        <div className="flex flex-col items-start gap-2 sm:flex-row sm:items-end sm:justify-between sm:gap-3">
          {heading}
          {action}
        </div>
        {children}
      </section>
    )
  }

  // `section` OUTSIDE the Card: the landmark is the semantic band, the Card is
  // its paper. Card is a plain div with no `asChild`, so nesting the other way
  // would need one.
  return (
    <section aria-labelledby={id} className={className}>
      <Card className="overflow-hidden p-0">
        <Collapsible open={open} onOpenChange={setOpen}>
          {/* The whole header row is the toggle, but `action` sits OUTSIDE the
              trigger: an "Open reading list" button nested in a toggle button is
              invalid markup and unclickable. */}
          <div className="flex flex-col items-start gap-2 px-5 pb-3 pt-4 sm:flex-row sm:items-end sm:justify-between sm:gap-3">
            <CollapsibleTrigger className="group flex min-w-0 flex-1 items-center gap-2 rounded-sm text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio">
              <ChevronDown
                aria-hidden
                className={cn(
                  'h-4 w-4 shrink-0 text-slate-400 transition-transform duration-200 group-hover:text-alma-folio motion-reduce:transition-none',
                  !open && '-rotate-90',
                )}
              />
              {heading}
            </CollapsibleTrigger>
            {action}
          </div>
          <CollapsibleContent>
            <div className="px-5 pb-5">{children}</div>
          </CollapsibleContent>
        </Collapsible>
      </Card>
    </section>
  )
}
