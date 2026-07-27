import type { ReactNode } from 'react'
import { ArrowRight } from 'lucide-react'

import { JargonHint } from '@/components/shared/JargonHint'
import { ScoreMeter } from '@/components/shared/ScoreMeter'
import { Card } from '@/components/ui/card'
import {
  HOME_SECTION_THEMES,
  type HomeSectionThemeKey,
} from '@/lib/palette'
import { cn } from '@/lib/utils'

export interface PaperTileProps {
  /** Where the tile hands the paper off — always the surface that OWNS it.
   *  Omit with `onSelect` (a selection tile navigates nowhere), or omit both
   *  for a tile whose paper is the subject of a question and whose only
   *  interaction is its `actions` strip (Signal Lab). */
  href?: string
  /** Selection variant (Signal Lab calibration): the whole tile is one
   *  stretched BUTTON instead of a link. Mutually exclusive with `href`. */
  onSelect?: () => void
  title: string
  /** Authors · journal · year, one line. */
  byline?: string | null
  /** TLDR or abstract; fills the remaining height and clamps. */
  excerpt?: string | null
  /** Chips / labels above the title (period, status). */
  eyebrow?: ReactNode
  /** 0–100 relevance. Rendered as a `ScoreMeter`; omit when the surface that
   *  produced this tile has no score — never pass a placeholder. */
  score?: number | null
  /** Why this paper is here — the quiet line in the footer. */
  reason?: ReactNode
  /** Long-form "why am I seeing this", behind an info popover in the footer. */
  explanation?: ReactNode
  /**
   * Triage controls for a tile whose surface OWNS the decision — today, only
   * Home's Inbox (D13), which has no other page to hand a paper off to.
   *
   * Rendered as its own strip below the reason footer, above the stretched
   * title link, so a click lands on the button rather than navigating. Pass a
   * `PaperActionBar` (compact) rather than loose buttons, so triage looks the
   * same here as it does on a full `PaperCard`.
   *
   * Omit it and the tile stays what it is everywhere else: navigation only.
   */
  actions?: ReactNode
  /** Home-only sticky-note category. Omit elsewhere for ordinary paper. */
  noteTheme?: HomeSectionThemeKey
  className?: string
}

/**
 * PaperTile — the paper item card for grids.
 *
 * Home lists papers as a grid of equal sheets rather than stacked rows: tiles
 * in a row stretch to the same height (grid stretch + `h-full`), so scanning
 * is spatial and no single paper wins by having a longer abstract — but the
 * height is set by the content, not by a fixed ratio, so no vertical space is
 * wasted.
 *
 * The whole tile is one link, implemented as a STRETCHED link on the title
 * rather than an `<a>` wrapper: the footer's explanation popover is a real
 * button, and a button nested inside an anchor is invalid and unclickable.
 * The title anchor's `::after` covers the card; anything interactive sits
 * above it on `z-10`.
 *
 * Navigation by default — a tile hands the paper to the surface that owns it.
 * The one exception is the `actions` slot, for a surface that owns the decision
 * itself and has nowhere to hand it off to (Home's Inbox). For a full row with
 * ratings, collections and a score breakdown, use `PaperCard` instead.
 */
export function PaperTile({
  href,
  onSelect,
  title,
  byline,
  excerpt,
  eyebrow,
  score,
  reason,
  explanation,
  actions,
  noteTheme,
  className,
}: PaperTileProps) {
  const hasFooter = Boolean(reason || explanation)
  const note = noteTheme ? HOME_SECTION_THEMES[noteTheme] : null
  return (
    // `border-edge-0` (the desk's own hairline, one step darker than the card
    // level's) is deliberate: a grid of tiles needs a readable boundary in two
    // directions, where a stacked list gets its structure from row dividers.
    <Card
      interactive
      className={cn(
        'group flex h-full flex-col overflow-hidden border-edge-0 p-0',
        note?.noteSurface,
        className,
      )}
    >
      {note && (
        <span
          className={cn(
            'pointer-events-none absolute right-0 top-0 z-[1] h-0 w-0 border-l-[18px] border-t-[18px] border-l-transparent',
            note.noteFold,
          )}
          aria-hidden
        />
      )}
      <div className="flex flex-1 flex-col gap-2 p-4">
        {(eyebrow || score != null) && (
          <div className="flex items-start justify-between gap-2">
            <div className="flex flex-wrap items-center gap-2">{eyebrow}</div>
            {score != null && <ScoreMeter score={score} className="shrink-0" />}
          </div>
        )}
        <h3 className="font-brand text-sm font-semibold leading-snug text-alma-800">
          {onSelect ? (
            <button
              type="button"
              onClick={onSelect}
              className="line-clamp-3 text-left rounded-sm transition-colors after:absolute after:inset-0 after:content-[''] group-hover:text-alma-folio focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio"
            >
              {title}
            </button>
          ) : href ? (
            <a
              href={href}
              className="line-clamp-3 rounded-sm transition-colors after:absolute after:inset-0 after:content-[''] group-hover:text-alma-folio focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-folio"
            >
              {title}
            </a>
          ) : (
            // Neither a destination nor a selection: the paper is the SUBJECT
            // of a question and the tile's `actions` are the only interaction
            // (Signal Lab). Plain text — an `<a>` with no href is unfocusable
            // and inert, and its stretched `::after` would still sit over the
            // card swallowing clicks meant for the actions strip.
            <span className="line-clamp-3">{title}</span>
          )}
        </h3>
        {byline && <p className="line-clamp-1 text-xs text-slate-500">{byline}</p>}
        {excerpt && (
          <p className="line-clamp-4 text-xs leading-relaxed text-slate-600">{excerpt}</p>
        )}
      </div>
      {/* Footer plate — recessed strip carrying the reason, the explanation
          affordance, and the navigation arrow. Not rendered without a reason:
          an empty plate is a dead strip. */}
      {hasFooter && (
        <div
          className={cn(
            'mt-auto flex items-center justify-between gap-2 border-t border-edge-1 bg-control-quiet px-4 py-2.5',
            note?.notePlate,
          )}
        >
          <span className="min-w-0 truncate text-[11px] font-medium text-slate-500">
            {reason}
          </span>
          <div className="relative z-10 flex shrink-0 items-center gap-1.5">
            {explanation && (
              <JargonHint
                title="Why this is here"
                ariaLabel="Why this paper is here"
                description={explanation}
                side="top"
                align="end"
              />
            )}
            <ArrowRight
              aria-hidden
              className="h-4 w-4 text-slate-400 transition-transform duration-200 group-hover:translate-x-0.5 group-hover:text-alma-folio motion-reduce:transition-none"
            />
          </div>
        </div>
      )}
      {/* Triage strip. `relative z-10` lifts it above the title link's
          `::after` overlay — without it every button would be a navigation
          click. `mt-auto` keeps it pinned to the bottom when there is no
          reason footer to do that job. */}
      {actions && (
        <div
          className={cn(
            'relative z-10 mt-auto border-t border-edge-1 bg-control-quiet px-3 py-2',
            note?.notePlate,
          )}
        >
          {actions}
        </div>
      )}
    </Card>
  )
}
