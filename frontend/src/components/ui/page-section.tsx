import { useState, type ComponentType, type ReactNode } from 'react'
import { ChevronDown } from 'lucide-react'

import { Card } from '@/components/ui/card'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { StatusBadge } from '@/components/ui/status-badge'
import { usePageTheme } from '@/components/ui/page-theme-context'
import { Surface } from '@/components/ui/surface'
import type { HomeSectionTheme } from '@/lib/palette'
import { cn } from '@/lib/utils'

export interface PageSectionProps {
  /** Anchor for `aria-labelledby`; also the heading's DOM id. */
  id: string
  title: string
  /**
   * Category glyph beside the title — what this band is ABOUT.
   *
   * Its tint is NOT chosen here. A `banded` section inherits the page's
   * identity hue from `PageThemeProvider`; everything else stays folio. The
   * rule that matters is that no call site picks a colour: one hue per page,
   * assigned once in `AppShell`, so a page can never wear two icon-colour
   * systems at the same time.
   */
  icon?: ComponentType<{ className?: string }>
  /** Override with a CATEGORICAL hue instead of the page's. Home's sections
   *  are coloured by what they contain (Inbox green, Reading violet), which is
   *  a different question from "which page am I on". One central registry owns
   *  both sets of hues (`lib/palette.ts`). */
  categoryTheme?: Pick<HomeSectionTheme, 'icon' | 'chip'>
  /**
   * How many items the section holds, as a pill beside the title.
   *
   * The count is the WHOLE set, not what happens to be rendered — the point is
   * to tell the reader how deep the section goes before they scan it. Omit it
   * only when the number is unknown or meaningless.
   *
   * **Zero is a real answer, not a reason to disappear.** A collapsible
   * section with `count === 0` keeps its header bar, wears a `0` pill and
   * starts folded (see `defaultOpen`). A band that vanishes when empty makes
   * the page's shape change under the reader, and "no Inbox section" is
   * indistinguishable from "Inbox is broken" (user report 2026-07-27).
   */
  count?: number
  /** One quiet line under the title. Say what the section contains. */
  description?: ReactNode
  /** Right-aligned control on the title row — usually a link to the owner page. */
  action?: ReactNode
  /**
   * How the band is dressed.
   *
   * `plain` (default) — a bare heading over its content, or, with
   * `collapsible`, a card whose header row is the fold. The everyday grouping
   * band (Home).
   * `banded` — the heading sits in its own tinted strip across the top of the
   * card and the title steps up a size. Reserve it for the two or three bands
   * a reader navigates a page BY ("Lenses", "Suggestions Map", "Author Map"):
   * it is loud on purpose, so it stops meaning anything if every group of
   * tiles wears it.
   */
  variant?: 'plain' | 'banded'
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
   *  hides its content by default reads as missing. An EMPTY section
   *  (`count === 0`) ignores this and starts folded: there is nothing behind
   *  the fold to read. Either way the user's own click wins from then on. */
  defaultOpen?: boolean
  /**
   * Controlled fold state. Pass it (with `onOpenChange`) when the PAGE owns
   * the state — because it persists the choice, or because something else on
   * the page opens the section. Leave both off for the normal case.
   */
  open?: boolean
  onOpenChange?: (open: boolean) => void
  /** What a collapsible section shows behind the fold when `count === 0`.
   *  Defaults to a quiet "nothing here" line, so unfolding an empty section
   *  always answers the question it was just asked. */
  emptyState?: ReactNode
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
  categoryTheme,
  count,
  description,
  action,
  variant = 'plain',
  collapsible = false,
  defaultOpen = true,
  open: openProp,
  onOpenChange,
  emptyState,
  children,
  className,
}: PageSectionProps) {
  // `null` = the reader has not touched the fold yet, so it follows the
  // content: open when there is something to read, shut when the section is
  // empty. Tracking the override separately (instead of seeding `useState`
  // from `count`) keeps that honest when the count arrives AFTER mount, which
  // it always does on a page fed by a query.
  // An explicit `categoryTheme` still wins — Home's sections are coloured by
  // CATEGORY (Inbox green, Reading violet), which is a different question from
  // "which page am I on". Everything else inherits the page's identity hue.
  const banded = variant === 'banded'
  const pageTheme = usePageTheme()
  const glyphTheme = categoryTheme ?? (banded ? pageTheme : null)
  const [userOpen, setUserOpen] = useState<boolean | null>(null)
  const isEmpty = count === 0
  const isControlled = openProp !== undefined
  const open = isControlled ? openProp : userOpen ?? (isEmpty ? false : defaultOpen)
  const setOpen = (next: boolean) => {
    if (!isControlled) setUserOpen(next)
    onOpenChange?.(next)
  }

  // The pill is a SIBLING of the heading, never inside it: nesting it would
  // fold the number into the heading's accessible name, so "Inbox" would
  // announce and be findable only as "Inbox 4".
  const heading = (
    <div className="min-w-0">
      <div className="flex items-center gap-2">
        {Icon && (
          <Icon
            className={cn(
              'shrink-0 text-alma-folio',
              banded ? 'h-5 w-5' : 'h-[1.15rem] w-[1.15rem]',
              glyphTheme?.icon,
            )}
            aria-hidden
          />
        )}
        <h2
          id={id}
          className={cn(
            'font-brand font-semibold text-alma-800',
            banded ? 'text-xl' : 'text-lg',
          )}
        >
          {title}
        </h2>
        {count != null && (
          <StatusBadge
            tone="neutral"
            size="sm"
            className={cn(
              'font-mono tabular-nums',
              glyphTheme?.chip,
            )}
            aria-label={`${count} in total`}
          >
            {count}
          </StatusBadge>
        )}
      </div>
      {description && <p className="text-sm text-slate-500">{description}</p>}
    </div>
  )

  // An empty collapsible section still answers the question it was asked:
  // unfolding it says "nothing here" rather than showing a blank box.
  const body =
    collapsible && isEmpty
      ? emptyState ?? <p className="text-sm text-slate-500">Nothing here right now.</p>
      : children

  if (banded) {
    // The band is a relational `Surface`, not a hardcoded `bg-surface-2`: this
    // section can be nested (a banded band inside a banded band), and a fixed
    // step would collide with its host the moment it is. Bottom hairline only,
    // and only while open — a folded section is a single object, so a rule
    // across its foot would read as a divider to nothing.
    const bandHeader = (
      <Surface
        className={cn(
          'flex flex-col items-start gap-2 rounded-none border-x-0 border-t-0 px-5 py-4 sm:flex-row sm:items-center sm:justify-between sm:gap-4',
          collapsible && !open && 'border-b-0',
        )}
      >
        {collapsible ? (
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
        ) : (
          <div className="min-w-0 flex-1">{heading}</div>
        )}
        {action}
      </Surface>
    )

    return (
      <section aria-labelledby={id} className={className}>
        <Card className="overflow-hidden p-0">
          {collapsible ? (
            <Collapsible open={open} onOpenChange={setOpen}>
              {bandHeader}
              <CollapsibleContent>
                <div className="space-y-4 p-4 sm:p-5">{body}</div>
              </CollapsibleContent>
            </Collapsible>
          ) : (
            <>
              {bandHeader}
              <div className="space-y-4 p-4 sm:p-5">{body}</div>
            </>
          )}
        </Card>
      </section>
    )
  }

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
            <div className="px-5 pb-5">{body}</div>
          </CollapsibleContent>
        </Collapsible>
      </Card>
    </section>
  )
}
