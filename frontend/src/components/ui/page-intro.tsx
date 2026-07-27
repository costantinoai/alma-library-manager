import { Fragment, type ComponentType, type ReactNode } from 'react'

import { BrandRule } from '@/components/ui/brand-rule'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { usePageTheme } from '@/components/ui/page-theme-context'
import { cn } from '@/lib/utils'

/**
 * MetaLine — the ONE live-status line under a page's lede.
 *
 * Counts, freshness, degradations: one row, dot-separated, quiet. It exists
 * so status never grows into a tile row at the top of a page — a reader
 * landing on Feed wants "12 monitors · 11 ready · 1 degraded" in a glance,
 * not six bordered boxes to cross before the content starts.
 *
 * Items are passed as a list and joined here, so the separator is spelled
 * once instead of hand-written `<span>·</span>` on every page (it was, on
 * four of them, in three different greys).
 */
export function MetaLine({
  items,
  className,
}: {
  items: ReactNode[]
  className?: string
}) {
  // `false` / `null` are how a page says "this fact doesn't apply right now"
  // (`degraded > 0 && <span…>`). `0` is a real value and must survive.
  const shown = items.filter((item) => item != null && item !== false && item !== '')
  if (shown.length === 0) return null
  return (
    <div
      className={cn(
        'flex flex-wrap items-center gap-x-2.5 gap-y-1 text-xs text-slate-500',
        className,
      )}
    >
      {shown.map((item, index) => (
        <Fragment key={index}>
          {index > 0 && (
            <span aria-hidden className="text-slate-300">
              ·
            </span>
          )}
          {item}
        </Fragment>
      ))}
    </div>
  )
}

/**
 * PulseDot — "this subsystem is live" beacon for a MetaLine.
 *
 * `success` = running, `warning` = running but degraded. Honours reduced
 * motion: the ping is decoration, and a reader who asked for stillness gets
 * the dot without it.
 */
export function PulseDot({ tone = 'success' }: { tone?: 'success' | 'warning' }) {
  const fill = tone === 'warning' ? 'bg-warning-500' : 'bg-success-500'
  return (
    <span className="relative flex h-2 w-2" aria-hidden>
      <span
        className={cn(
          'absolute inline-flex h-full w-full animate-ping rounded-full opacity-60 motion-reduce:animate-none',
          fill,
        )}
      />
      <span className={cn('relative inline-flex h-2 w-2 rounded-full', fill)} />
    </span>
  )
}

export interface PageGuide {
  /** Overline. Defaults to the one house phrasing so every page says it the
   *  same way — a reader learns to look for those three words once. */
  eyebrow?: string
  /** The one-line answer, always visible without expanding. */
  summary: ReactNode
  /** The full explanation, behind the fold. */
  children: ReactNode
}

export interface PageIntroProps {
  /** Category glyph, in a soft medallion. Gives the masthead something to
   *  anchor on; without it the block reads as a loose caption. */
  icon?: ComponentType<{ className?: string }>
  /**
   * The page's thesis, in one short sentence — what you are looking at, in
   * the reader's words, not the system's. This is the biggest type on the
   * page body and the first thing read, so it must EARN the size: "Papers you
   * haven't seen yet", not "Discovery recommendations".
   *
   * Deliberately not a heading element: the TopBar already renders the page's
   * one `<h1>`, and a second makes the page announce itself twice.
   */
  lede: ReactNode
  /** The supporting sentence under the lede — the detail the thesis omits. */
  detail?: ReactNode
  /** Step the lede up to masthead size. Home's greeting only. */
  masthead?: boolean
  /** The live status line. Use `<MetaLine items={[…]} />`. */
  meta?: ReactNode
  /**
   * The page's `<PageTour>`. It rides at the END of the status line, never in
   * the action column: it is help ABOUT the page, and on a page whose only
   * action is the tour a lone icon button strands itself in an empty corner.
   */
  tour?: ReactNode
  /** Right column: the page's real actions, plus whatever hangs off them
   *  (a freshness line, an auto-refresh toggle). */
  actions?: ReactNode
  /** Close the masthead with the gold rule. Masthead trim only (Home). */
  rule?: boolean
  /** The page's "How this works" explainer, welded to the strip's bottom. */
  guide?: PageGuide
  /** Drop the strip's own paper — for an intro nested inside a Card, where a
   *  second surface at the same level would be cream on cream. */
  bare?: boolean
  /** Extra rows under the status line (an active filter note, a banner). */
  children?: ReactNode
  className?: string
  /**
   * Onboarding-tour anchor for the strip itself (`onboarding/tours.ts`).
   *
   * A page tour's opening step usually points at the page header — "this is
   * what this surface is". When Feed's hand-rolled hero became a `PageIntro`,
   * its `data-tour="feed-hero"` went with it and step 1 silently degraded to a
   * card floating over a dimmed page (2026-07-27 audit). The anchor belongs on
   * the primitive so no future migration can drop it again.
   */
  'data-tour'?: string
}

/**
 * PageIntro — the top of every page, spelled once.
 *
 * Landing on a page has to answer two questions before anything else: *what
 * am I looking at* and *what do I do here*. This primitive is the answer's
 * fixed shape — a thesis line, its supporting sentence, one live status row,
 * the page's actions, and the "How this works" explainer folded into the same
 * object — so the answer is in the same place, in the same voice, on Feed as
 * on Health.
 *
 * It replaced nine different page tops: four rendered a second `<h1>` under
 * the TopBar's, two padded themselves on top of AppShell's padding, one had
 * no title at all, and the explainer callout floated at four different depths
 * under four different eyebrows.
 *
 * The type carries the hierarchy: brand face for the thesis, body grey for
 * the detail, small grey for status. The first draft set the thesis in body
 * copy and the whole block read as a caption with no head (2026-07-27).
 *
 * See `tasks/lessons.md` → "One masthead, one lede, one guide".
 */
export function PageIntro({
  icon: Icon,
  lede,
  detail,
  masthead,
  meta,
  tour,
  actions,
  rule,
  guide,
  bare,
  children,
  className,
  'data-tour': dataTour,
}: PageIntroProps) {
  // The page's identity hue comes from AppShell, never a prop — see
  // `page-theme.tsx` for why a prop was the wrong shape.
  const theme = usePageTheme()
  const body = (
    <>
      <div
        className={cn(
          'flex flex-col gap-4 md:flex-row md:items-start md:justify-between md:gap-8',
          !bare && 'p-5',
        )}
      >
        <div className="min-w-0 flex-1 space-y-3">
          <div className="flex items-start gap-3">
            {Icon && (
              <span
                className={cn(
                  'mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-full',
                  theme ? `${theme.medallion} ${theme.icon}` : 'bg-accent-soft text-alma-folio',
                )}
                aria-hidden
              >
                <Icon className="h-[1.15rem] w-[1.15rem]" />
              </span>
            )}
            <div className="min-w-0 space-y-1">
              <p
                className={cn(
                  'font-brand font-semibold leading-snug text-alma-800',
                  masthead ? 'text-2xl sm:text-[1.75rem]' : 'text-lg',
                )}
              >
                {lede}
              </p>
              {detail && (
                <p className="max-w-2xl text-sm leading-relaxed text-slate-600">{detail}</p>
              )}
            </div>
          </div>
          {meta && (
            <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1">
              {meta}
              {tour}
            </div>
          )}
          {children}
        </div>
        {/* The tour rides the status line when there is one. On a page with no
            status to report (Home) that line does not exist, and rendering it
            for the tour alone left a `?` stranded on its own row — so there it
            joins the actions instead. */}
        {(actions || (!meta && tour)) && (
          <div className="flex shrink-0 flex-col gap-1 md:items-end">
            {!meta && tour ? (
              <div className="flex flex-wrap items-center gap-2">
                {actions}
                {tour}
              </div>
            ) : (
              actions
            )}
          </div>
        )}
      </div>
      {rule && (
        <div className={cn(!bare && 'px-5 pb-4')}>
          <BrandRule center="diamond" />
        </div>
      )}
      {guide && (
        <ConceptCallout
          flush={!bare}
          eyebrow={guide.eyebrow ?? 'How this works'}
          summary={guide.summary}
        >
          {guide.children}
        </ConceptCallout>
      )}
    </>
  )

  if (bare)
    return (
      <div data-tour={dataTour} className={cn('space-y-3', className)}>
        {body}
      </div>
    )

  return (
    <section
      data-tour={dataTour}
      className={cn(
        'overflow-hidden rounded-sm border border-[var(--color-border)] bg-surface-1 shadow-paper-sheet',
        className,
      )}
    >
      {body}
    </section>
  )
}
