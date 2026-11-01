import { useCallback, useEffect, useRef, useState } from 'react'
import { ChevronDown, ExternalLink, Loader2, Compass, UserPlus } from 'lucide-react'

import { VenueHoverCard } from '@/components/shared/VenueHoverCard'
import { SignalChip } from '@/components/shared/SignalChip'
import { ScoreMeter } from '@/components/shared/ScoreMeter'

import { Card } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'
import { AuthorHoverCard } from '@/components/authors/AuthorHoverCard'
import { PaperHoverCard } from './PaperHoverCard'
import { PaperActionBar, type PaperReaction } from '@/components/discovery/PaperActionBar'
import { AddToCollectionMenu } from '@/components/discovery/AddToCollectionMenu'
import { StarRating } from '@/components/StarRating'
import { addPaperToCollections, trackInteraction, type ScoreBreakdown } from '@/api/client'
import { cn, normalizeAuthorName, truncate } from '@/lib/utils'
import { PaperPdfLink } from '@/components/pdf/PaperPdfLink'
import { formatPaperDate } from '@/lib/format'
import { ScoreBreakdownPanel, ScoreBreakdownTeaser } from '@/components/ScoreBreakdownPanel'
import { Button } from '@/components/ui/button'
import { SubPanel } from '@/components/ui/sub-panel'

/** The card header's small round icon controls (PDF link, Discover similar).
 *  Local PDF access is independent of fetch plugins. */
const HEADER_ICON_BUTTON = cn(
  'inline-flex h-7 w-7 items-center justify-center rounded-full border border-control-edge bg-control-well text-slate-500 shadow-paper-sm transition-colors duration-150',
  'hover:border-control-edge-strong hover:bg-control-quiet hover:text-alma-700',
  'focus-ring',
)

export interface PaperCardPaper {
  id: string
  title: string
  authors?: string
  year?: number | null
  journal?: string
  url?: string
  doi?: string
  publication_date?: string | null
  cited_by_count?: number
  rating?: number
  status?: string
  abstract?: string
  /** S2's 1-2 sentence AI summary. Rendered italic just above the
   *  abstract toggle when present; hidden when absent. */
  tldr?: string | null
  /** S2's learned "this citation mattered" count — rendered as a
   *  subtle badge alongside `cited_by_count` when > 0. */
  influential_citation_count?: number
  /** paper_signal composite ranking (0..1) — rendered as "Rank N"
   *  in the metadata strip when > 0. Distinct from the `rating`
   *  star field (user curation). */
  global_signal_score?: number
}

/** Visual size / density variant.
 *
 * - `compact`: dense rows (p-3, no labels on action buttons). Use in
 *   lists where many cards are stacked close together (CommandPalette
 *   results, sidebar peek panels, graph node inspectors).
 * - `default`: the main surface variant used by Feed, Library tabs,
 *   Discovery result lists, and search results.
 * - `detailed`: default surface + always-expanded abstract + always-
 *   expanded score breakdown. Use on high-focus pages (paper detail
 *   popup or author-detail publications
 *   when the reader is zoomed in on a single work).
 */
export type PaperCardSize = 'compact' | 'default' | 'detailed'

interface PaperCardProps {
  paper: PaperCardPaper
  score?: number
  rank?: number
  scoreBreakdown?: ScoreBreakdown | null
  explanation?: string | null
  followedAuthorNames?: Set<string>
  followAuthorPendingName?: string | null
  onFollowAuthor?: (authorName: string, paperId: string) => void
  followedVenueKeys?: Set<string>
  venueFollowPending?: string | null
  onFollowVenue?: (args: { sourceId: string; displayName: string; keywords?: string[] }) => void
  onDetails?: () => void
  onDismiss?: () => void
  onQueue?: () => void
  onAdd?: () => void
  onLike?: () => void
  onLove?: () => void
  onDislike?: () => void
  /** Per-aspect toggle-off: re-clicking an applied action undoes only its own
   *  effect (Save→'membership', Queue→'reading', active reaction→'rating'). */
  onUndo?: (aspect: 'membership' | 'rating' | 'reading') => void
  /** "Discover similar" pivot — re-seeds the Discovery lens with this
   *  paper as the anchor. Rendered as a small neutral chip above the
   *  triage action bar. Library + Discovery surfaces pass this; Feed
   *  does not (Feed is chronological monitoring, not exploration). */
  onPivot?: () => void
  actionDisabled?: boolean
  onRate?: (rating: number) => void
  children?: React.ReactNode
  className?: string
  /** @deprecated use `size="compact"` instead. Kept so in-progress migrations
   *  don't break; prefer the `size` prop for new call sites. */
  compact?: boolean
  /** Visual size / density variant. See {@link PaperCardSize}. */
  size?: PaperCardSize
  dismissLabel?: string
  dismissTitle?: string
  dislikeLabel?: string
  dislikeTitle?: string
  /** Save-button "saved" label + passive (non-removing) saved state, and a
   *  distinct "Add to collection" action. A collection lens uses these to show
   *  an in-Library paper as a checked "In library" indicator plus a folio-accent
   *  "Add to collection" button. Forwarded to PaperActionBar. */
  savedLabel?: string
  savedReadOnly?: boolean
  onAddToCollection?: () => void
  addToCollectionLabel?: string
  addToCollectionTitle?: string
  onAddToCollections?: (collectionIds: string[]) => void | Promise<void>
  defaultCollectionIds?: string[]
  onExpandBreakdown?: () => void
  quickActions?: React.ReactNode
  /** Provenance chips rendered in the metadata row — one short badge per
   *  source that returned the paper (e.g. `["openalex", "semantic_scholar"]`
   *  from the multi-source online search). Purely informational; does not
   *  change behavior. */
  sources?: string[]
  /** Optional slot inserted at the end of the metadata row. Ideal home for
   *  a reading-status dropdown (Library surfaces) or a bulk-action menu
   *  without forking the card layout. */
  readingStatusSlot?: React.ReactNode
  /** The surface's own why-signal, rendered INLINE at the HEAD of the signal
   *  row — Feed's matched authors / monitors, Discovery's provenance chips.
   *  It shares the line with the citations, the score meter and the Why
   *  toggle (which close the row), so a normal-view card stays four rows:
   *  title / authors · date · journal / signals / actions. Pass chips, never
   *  a block — anything taller belongs in `children`. */
  metaSlot?: React.ReactNode
  /** Optional slot inserted inline with the title, right-aligned. Use for
   *  row-context affordances that must sit next to the title (e.g. a
   *  reading-status pill in a compact Library row, a provenance chip in
   *  the Corpus explorer). */
  trailingHeader?: React.ReactNode
  /** Active reaction on the paper (like/love/dislike mutually exclusive). */
  reaction?: PaperReaction
  /** Whether the paper is already saved to Library (controls Save button). */
  isSaved?: boolean
  /** When saved, clicking Save removes from Library (toggle). Surfaces whose
   *  Save handler removes (Feed) set this so the title reads "Remove from
   *  library". */
  savedClickRemoves?: boolean
  /** Whether the paper is already on the reading list (controls Queue button). */
  isQueued?: boolean
  /** When true, the TLDR line and the abstract toggle are both hidden,
   *  even at the `default` size. Used by Discovery's normal view to keep
   *  cards dense — the user opens the detail panel for the full text. */
  suppressSummaries?: boolean
  /** When true, the bottom action bar renders in its compact (icon-only,
   *  shorter-height) form even at the `default` card size. Lets surfaces
   *  keep dense triage buttons without flipping the whole card to compact. */
  compactActions?: boolean
  /** Extra detail behind the Why toggle, below the score breakdown. */
  whyDetail?: React.ReactNode
  /** Optional bulk-selection affordance — renders a hover-revealed checkbox
   * in the card header's leading column so it never overlaps the title.
   * The checkbox stays visible while the row is checked. Pass a stable
   * `onCheckedChange` callback; the primitive handles `stopPropagation` so
   * clicking the checkbox doesn't trigger `onDetails` on the card root. */
  selection?: {
    checked: boolean
    onCheckedChange: (checked: boolean) => void
    ariaLabel?: string
  }
}

function parseAuthorNames(value?: string | null): string[] {
  const raw = String(value || '').trim()
  if (!raw) return []
  const parts = raw
    .split(/,|;|\sand\s|\s&\s/gi)
    .map((item) => item.trim())
    .filter((item) => item.length > 0 && item.toLowerCase() !== 'et al.')

  const unique: string[] = []
  const seen = new Set<string>()
  for (const part of parts) {
    const normalized = normalizeAuthorName(part)
    if (!normalized || seen.has(normalized)) continue
    seen.add(normalized)
    unique.push(part)
  }
  return unique
}

/** A list this short is shown whole: eliding it would save nothing. */
const AUTHORS_SHOWN_WHOLE = 6
const AUTHORS_KEPT_FIRST = 3
const AUTHORS_KEPT_LAST = 2

type AuthorRun =
  | { kind: 'name'; name: string; index: number }
  | { kind: 'hidden'; count: number; index: number }

/**
 * Collapses a long author list into names and `+N` gaps. A reader decides on
 * the first authors, who did the work, and the last, whose lab it is; 24% of
 * the corpus has more than 6 authors and the longest lists run past 100.
 * A followed author in the middle is never hidden: that is the name you came for.
 */
function authorRunsFor(names: string[], isKept: (name: string) => boolean): AuthorRun[] {
  if (names.length <= AUTHORS_SHOWN_WHOLE) {
    return names.map((name, index) => ({ kind: 'name', name, index }))
  }
  const runs: AuthorRun[] = []
  names.forEach((name, index) => {
    const keep =
      index < AUTHORS_KEPT_FIRST || index >= names.length - AUTHORS_KEPT_LAST || isKept(name)
    const previous = runs[runs.length - 1]
    if (keep) runs.push({ kind: 'name', name, index })
    else if (previous?.kind === 'hidden') previous.count += 1
    else runs.push({ kind: 'hidden', count: 1, index })
  })
  return runs
}

// ── Main card ──

// Minimum dwell time (ms) before we consider abstract engagement intentional
const ABSTRACT_MIN_DWELL_MS = 2000

export function PaperCard({
  paper,
  score,
  rank,
  scoreBreakdown,
  explanation,
  followedAuthorNames,
  followAuthorPendingName,
  onFollowAuthor,
  followedVenueKeys,
  venueFollowPending,
  onFollowVenue,
  onDetails,
  onDismiss,
  onQueue,
  onAdd,
  onLike,
  onLove,
  onDislike,
  onUndo,
  onPivot,
  actionDisabled = false,
  onRate,
  children,
  className = '',
  compact = false,
  size,
  dismissLabel,
  dismissTitle,
  dislikeLabel,
  dislikeTitle,
  savedLabel,
  savedReadOnly,
  onAddToCollection,
  addToCollectionLabel,
  addToCollectionTitle,
  onAddToCollections,
  defaultCollectionIds,
  onExpandBreakdown,
  quickActions,
  sources,
  readingStatusSlot,
  metaSlot,
  trailingHeader,
  reaction = null,
  isSaved = false,
  savedClickRemoves = false,
  isQueued = false,
  suppressSummaries = false,
  compactActions = false,
  whyDetail,
  selection,
}: PaperCardProps) {
  // Resolve the effective size: explicit `size` wins; legacy `compact` prop
  // maps to 'compact' for in-flight call sites; otherwise 'default'.
  const effectiveSize: PaperCardSize = size ?? (compact ? 'compact' : 'default')
  const isCompact = effectiveSize === 'compact'
  const isDetailed = effectiveSize === 'detailed'
  const showAbstractByDefault = isDetailed
  const [showBreakdown, setShowBreakdown] = useState(isDetailed)
  const [showAbstract, setShowAbstract] = useState(showAbstractByDefault)
  const [authorsExpanded, setAuthorsExpanded] = useState(false)

  // When the caller flips forceShowAbstract or size between variants keep the
  // card in sync so already-mounted cards update with the surface choice.
  useEffect(() => {
    setShowAbstract(showAbstractByDefault)
  }, [showAbstractByDefault])
  const abstractExpandedAt = useRef<number | null>(null)
  const collectionConfirm = onAddToCollections ?? (
    paper.id
      ? async (collectionIds: string[]) => {
          await addPaperToCollections(paper.id, collectionIds)
        }
      : undefined
  )
  const hasActions = !!(onDismiss || onQueue || onLike || onLove || onAdd || onDislike || collectionConfirm)

  // T15 — derived display helpers for the card's metadata strip.
  //
  // Year inline with authors: pub_date preferred over bare year because
  // "Feb 2024" is more scannable than "2024". Rendered at the precision the
  // source provides — "26 May 2026" when the day is known, else "May 2026",
  // else the bare year — never fabricating a day (see lessons.md).
  const yearInline = ((): string | null => {
    const formatted = formatPaperDate((paper.publication_date || '').trim())
    if (formatted) return formatted
    if (paper.year != null) return String(paper.year)
    return null
  })()

  // Signal strip values — only render fields that exist (sparse-field
  // policy, per T5 + `lessons.md`). "Rank" is the paper_signal
  // composite (0-100 int); distinct from the star rating (user
  // curation) which is rendered separately as `★N`.
  const rankDisplay: number | null =
    paper.global_signal_score != null && paper.global_signal_score > 0
      ? Math.round(paper.global_signal_score * 100)
      : null
  const starDisplay: string | null =
    paper.rating && paper.rating > 0 ? `${paper.rating}★` : null
  const citationsLabel: string | null = ((): string | null => {
    const cites = paper.cited_by_count ?? 0
    if (cites <= 0) return null
    const influential = paper.influential_citation_count ?? 0
    if (influential > 0) return `${cites.toLocaleString()} cited · ${influential} influential`
    return `${cites.toLocaleString()} cited`
  })()
  // Does the signal row carry any of the numeric fields? Decides whether the
  // row renders at all when the surface supplies only `metaSlot`.
  // The score bar is NOT part of this — it has its own row below the chips
  // (see the score-row comment further down).
  const hasMetaLead = !!citationsLabel || rankDisplay != null || !!starDisplay
  const padding = isCompact ? 'p-3' : 'p-4'
  const hasBreakdown = scoreBreakdown && Object.keys(scoreBreakdown).length > 0
  const hasExplanation = !!explanation?.trim()
  const canToggleAbstract = !suppressSummaries && !showAbstractByDefault && !!paper.abstract
  const authorNames = parseAuthorNames(paper.authors)
  const authorRuns: AuthorRun[] = authorNames.length > 0
    ? (authorsExpanded
        ? authorNames.map((name, index) => ({ kind: 'name' as const, name, index }))
        : authorRunsFor(authorNames, (name) => followedAuthorNames?.has(normalizeAuthorName(name)) ?? false))
    : (paper.authors ? [{ kind: 'name', name: truncate(paper.authors, 120), index: 0 }] : [])
  const canFollowAuthors = !!(paper.id && onFollowAuthor)
  const unfollowedAuthorCount = authorNames.filter(
    (name) => !(followedAuthorNames?.has(normalizeAuthorName(name)) ?? false),
  ).length
  const canFollowVenue = !!(paper.journal && onFollowVenue)

  // Track abstract engagement duration on collapse or unmount
  const flushAbstractEngagement = useCallback(() => {
    if (abstractExpandedAt.current && paper.id) {
      const durationMs = Date.now() - abstractExpandedAt.current
      if (durationMs > ABSTRACT_MIN_DWELL_MS) {
        trackInteraction('abstract_engagement', paper.id, { duration_ms: durationMs })
      }
      abstractExpandedAt.current = null
    }
  }, [paper.id])

  // Flush on unmount
  useEffect(() => flushAbstractEngagement, [flushAbstractEngagement])

  const handleAbstractToggle = () => {
    if (showAbstract) {
      // Collapsing — flush engagement
      flushAbstractEngagement()
    } else {
      // Expanding — start timer
      abstractExpandedAt.current = Date.now()
    }
    setShowAbstract((prev) => !prev)
  }

  const handleExternalLinkClick = (urlType: string) => {
    if (paper.id) {
      trackInteraction('external_link_click', paper.id, { url_type: urlType })
    }
  }

  // Whole-card click opens the paper details popup (when onDetails is wired).
  // Inner interactive elements — external-link title, author follow buttons,
  // action bar, Why toggle, abstract toggle, star rating — all call
  // stopPropagation so they don't also fire this.
  const rootClickable = !!onDetails
  const handleRootClick = () => {
    if (onDetails) onDetails()
  }
  const handleRootKey = (event: React.KeyboardEvent) => {
    if (!onDetails) return
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      onDetails()
    }
  }

  return (
    <Card
      // A PaperCard lifts one level off whatever surface it sits on; the
      // relational ladder handles the contrast, so no manual tone is needed.
      className={cn(
        'group/paper-card relative overflow-hidden transition-all duration-150 hover:shadow-paper-sheet-hover',
        rootClickable && 'cursor-pointer',
        className,
      )}
      onClick={rootClickable ? handleRootClick : undefined}
      onKeyDown={rootClickable ? handleRootKey : undefined}
      role={rootClickable ? 'button' : undefined}
      tabIndex={rootClickable ? 0 : undefined}
    >
      {/* "Saved to library" gold corner ribbon — a small rotated gold
          square half-clipped by the Card's overflow:hidden, leaving a
          gold triangle in the top-left corner. At-a-glance indicator
          that this paper is in the user's Library, beyond the action-
          bar pill. Echoes the bookmark/spine motif from the brand mark. */}
      {isSaved && (
        <div
          className="pointer-events-none absolute -left-3 -top-3 z-local h-6 w-6 rotate-45 bg-gold-400"
          aria-hidden
        />
      )}
      {/* Selection rail — page-gutter metaphor. Absolute-positioned so it
          spans the full card height without being part of the flow; the
          content div reserves `pl-11` to keep the title off the rail.
          Idle: invisible. Hover: soft slate fill + checkbox fades in.
          Selected: folio ribbon + inverted (white-on-accent) checkbox. */}
      {selection && (
        <div
          className={cn(
            'absolute inset-y-0 left-0 z-local flex w-8 items-start justify-center pt-[18px] transition-colors duration-200',
            selection.checked
              ? 'bg-alma-folio'
              : 'bg-transparent group-hover/paper-card:bg-control-quiet',
          )}
          onClick={(e) => e.stopPropagation()}
        >
          <div
            className={cn(
              'transition-all duration-150',
              selection.checked
                ? 'opacity-100'
                : 'opacity-0 -translate-x-0.5 group-hover/paper-card:translate-x-0 group-hover/paper-card:opacity-100 focus-within:translate-x-0 focus-within:opacity-100',
            )}
          >
            <Checkbox
              aria-label={selection.ariaLabel ?? 'Select paper'}
              checked={selection.checked}
              onCheckedChange={(value) => selection.onCheckedChange(value === true)}
              className={cn(
                selection.checked &&
                  'border-on-fill/80 data-[state=checked]:border-on-fill data-[state=checked]:bg-surface-4 data-[state=checked]:text-alma-600',
              )}
            />
          </div>
        </div>
      )}

      <div className={cn(padding, selection && 'pl-11')}>
        {/* Header row: rank badge + title + details button */}
        <div className="flex items-start gap-3">
          {/* Rank pill */}
          {rank != null && (
            <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-control-quiet text-caption font-bold text-slate-500">
              {rank}
            </div>
          )}

          <div className="min-w-0 flex-1">
            {/* Title */}
            <div className="flex items-start gap-1.5">
              {/* Trailing header slot — the PDF link and the pivot ("Discover
                  similar") sit here as small icon buttons so they never cost a
                  full action row. Any caller-supplied trailingHeader (e.g.
                  Library's reading-status pill) renders alongside them; with no
                  paper id and nothing supplied, the slot collapses entirely. */}
              {(onPivot || trailingHeader || paper.id) && (
                <div
                  className="ml-auto order-last flex shrink-0 items-center gap-1.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  <PaperPdfLink paperId={paper.id} className={HEADER_ICON_BUTTON} iconClassName="h-3.5 w-3.5" />
                  {onPivot && (
                    <button
                      type="button"
                      onClick={(e) => { e.stopPropagation(); onPivot() }}
                      title="Discover similar papers — re-seed Discovery with this paper as the anchor"
                      aria-label="Discover similar papers"
                      className={HEADER_ICON_BUTTON}
                    >
                      <Compass className="h-3.5 w-3.5" aria-hidden />
                    </button>
                  )}
                  {canFollowAuthors && unfollowedAuthorCount > 0 && (
                    <Popover>
                      <PopoverTrigger asChild>
                        <button
                          type="button"
                          title="Follow an author of this paper"
                          aria-label="Follow an author of this paper"
                          className={HEADER_ICON_BUTTON}
                        >
                          <UserPlus className="h-3.5 w-3.5" aria-hidden />
                        </button>
                      </PopoverTrigger>
                      <PopoverContent align="end" className="w-72 p-1" onClick={(e) => e.stopPropagation()}>
                        <ul className="max-h-72 overflow-y-auto">
                          {authorNames.map((name) => {
                            const normalized = normalizeAuthorName(name)
                            const followed = followedAuthorNames?.has(normalized) ?? false
                            const pending = followAuthorPendingName === normalized
                            return (
                              <li key={normalized} className="flex items-center justify-between gap-2 px-2 py-1">
                                <span className="min-w-0 truncate text-sm text-alma-800" title={name}>{name}</span>
                                {followed ? (
                                  <span className="shrink-0 text-caption text-slate-500">Following</span>
                                ) : (
                                  <Button
                                    variant="outline"
                                    size="xs"
                                    className="shrink-0"
                                    disabled={pending}
                                    onClick={() => onFollowAuthor?.(name, paper.id)}
                                  >
                                    {pending && <Loader2 className="h-3 w-3 animate-spin" />}
                                    Follow
                                  </Button>
                                )}
                              </li>
                            )
                          })}
                        </ul>
                      </PopoverContent>
                    </Popover>
                  )}
                  {trailingHeader}
                </div>
              )}
              <PaperHoverCard
                paper={paper}
                score={score}
                scoreBreakdown={scoreBreakdown}
                explanation={explanation}
              >
                {paper.url ? (
                  <a
                    href={paper.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-lede font-semibold leading-snug text-alma-800 transition-colors hover:text-alma-600"
                    onClick={(e) => { e.stopPropagation(); handleExternalLinkClick('url') }}
                  >
                    {paper.title}
                    <ExternalLink className="ml-1 inline-block h-3 w-3 text-slate-300" />
                  </a>
                ) : (
                  <h3 className="text-lede font-semibold leading-snug text-alma-800">
                    {paper.title}
                  </h3>
                )}
              </PaperHoverCard>
            </div>

            {/* Byline: who on the left, where and when in a fixed column on the
                right. The date always ends at the same x, so a date-sorted list
                reads down one edge; on a phone it drops to a line of its own.
                A long author list keeps the people who decide it for a reader:
                the first three, the last two, and anyone you follow. */}
            {(paper.authors || paper.journal || yearInline) && (
              <div className="mt-0.5 flex flex-wrap items-baseline justify-between gap-x-6 gap-y-0.5 text-sm leading-snug text-slate-500 sm:flex-nowrap">
                {paper.authors && (
                  <span className="min-w-0">
                    {authorRuns.map((run, runIndex) => {
                      const isLastRun = runIndex === authorRuns.length - 1
                      if (run.kind === 'hidden') {
                        return (
                          <span key={`hidden-${run.index}`} className="whitespace-nowrap">
                            <Button
                              variant="muted"
                              size="inline"
                              onClick={(e) => { e.stopPropagation(); setAuthorsExpanded(true) }}
                              title={`Show ${run.count} more authors`}
                              aria-label={`Show ${run.count} more authors`}
                              className="tabular-nums"
                            >
                              +{run.count}
                            </Button>
                            {!isLastRun && <span className="text-slate-400">, </span>}
                          </span>
                        )
                      }
                      const authorName = run.name
                      const normalized = normalizeAuthorName(authorName)
                      const isFollowed = followedAuthorNames?.has(normalized) ?? false
                      const canWrapHover = authorNames.length > 0
                      const label = (
                        <span
                          className={cn(
                            'cursor-default rounded-control transition-colors hover:bg-control-quiet hover:text-slate-700',
                            isFollowed && 'font-medium text-slate-700',
                          )}
                        >
                          {authorName}
                        </span>
                      )
                      return (
                        <span key={`${paper.id}-${authorName}-${run.index}`} className="group/author">
                          {canWrapHover ? (
                            <AuthorHoverCard name={authorName} paperId={paper.id || undefined}>
                              {label}
                            </AuthorHoverCard>
                          ) : (
                            label
                          )}
                          {!isLastRun && <span className="text-slate-400">, </span>}
                        </span>
                      )
                    })}
                    {authorsExpanded && authorNames.length > AUTHORS_SHOWN_WHOLE && (
                      <Button
                        variant="muted"
                        size="inline"
                        onClick={(e) => { e.stopPropagation(); setAuthorsExpanded(false) }}
                        className="ml-2"
                      >
                        Show fewer
                      </Button>
                    )}
                  </span>
                )}
                {(paper.journal || yearInline) && (
                  <span className="inline-flex min-w-0 max-w-full items-baseline gap-1.5 sm:ml-auto sm:max-w-[45%] sm:shrink-0">
                    {paper.journal && (
                      canFollowVenue ? (
                        <VenueHoverCard
                          journal={paper.journal}
                          isFollowed={followedVenueKeys?.has(paper.journal.toLowerCase()) ?? false}
                          followPending={venueFollowPending === paper.journal.toLowerCase()}
                          onFollow={onFollowVenue}
                        >
                          <span
                            className="min-w-0 cursor-default truncate rounded-control italic transition-colors hover:bg-control-quiet hover:text-slate-700"
                            title={paper.journal}
                          >
                            {paper.journal}
                          </span>
                        </VenueHoverCard>
                      ) : (
                        <span className="min-w-0 truncate italic" title={paper.journal}>{paper.journal}</span>
                      )
                    )}
                    {paper.journal && yearInline && <span className="text-slate-300">·</span>}
                    {yearInline && (
                      <span className="shrink-0 whitespace-nowrap tabular-nums text-slate-500" title={paper.publication_date || undefined}>
                        {yearInline}
                      </span>
                    )}
                  </span>
                )}
              </div>
            )}

            {/* Signal row. The surface's why on the left; the paper's own facts
                on the right, under the date, so they stay in one column however
                many chips the left side wraps to. Every field is optional
                (sparse-field policy); the row hides when there is nothing. */}
            {(hasMetaLead || metaSlot) && (
              <div className="mt-1.5 flex flex-wrap items-center justify-between gap-x-6 gap-y-1 text-xs text-slate-500">
                {metaSlot && (
                  <span className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
                    {metaSlot}
                  </span>
                )}
                {hasMetaLead && (
                  <span className="ml-auto flex shrink-0 items-center gap-x-1.5 tabular-nums max-sm:ml-0">
                    {citationsLabel && (
                      <span
                        title={
                          (paper.influential_citation_count ?? 0) > 0
                            ? `${paper.cited_by_count} citations (${paper.influential_citation_count} flagged influential by S2)`
                            : `${paper.cited_by_count} citations`
                        }
                      >
                        {citationsLabel}
                      </span>
                    )}
                    {citationsLabel && rankDisplay != null && <span className="text-slate-300">·</span>}
                    {rankDisplay != null && (
                      <span
                        className="text-slate-600"
                        title="paper_signal composite (0–100) — ALMa's taste-fit score. Distinct from your star rating."
                      >
                        Rank {rankDisplay}
                      </span>
                    )}
                    {(citationsLabel || rankDisplay != null) && starDisplay && <span className="text-slate-300">·</span>}
                    {starDisplay && (
                      <span className="text-gold-500" title={`Your rating: ${paper.rating}/5`}>
                        {starDisplay}
                      </span>
                    )}
                  </span>
                )}
              </div>
            )}

            {/* Provenance chips — which external sources returned this paper.
                `source` kind: quiet plumbing, one Globe glyph leads the row and
                the rest are bare so a 4-source row doesn't turn into a picket
                fence of identical icons. */}
            {sources && sources.length > 0 && (
              <div className="mt-1.5 flex flex-wrap items-center gap-1">
                {sources.map((source, i) => (
                  <SignalChip
                    key={source}
                    kind="source"
                    hideIcon={i > 0}
                    title={`Returned by ${source.replace(/_/g, ' ')}`}
                  >
                    {source.replace(/_/g, ' ')}
                  </SignalChip>
                ))}
              </div>
            )}

            {/* Reading-status / row-context slot (Library tabs inject the reading-status dropdown here). */}
            {readingStatusSlot && (
              <div className="mt-1.5" onClick={(e) => e.stopPropagation()}>
                {readingStatusSlot}
              </div>
            )}

            {/* TLDR + Abstract — both summary surfaces. Hidden when
                `suppressSummaries` is set so the dense Discovery normal
                view stays slim; users open the detail panel for the full
                text instead of expanding inline. */}
            {!suppressSummaries && paper.tldr && paper.tldr.trim() && (
              <p
                className="mt-1.5 line-clamp-2 text-caption italic leading-snug text-slate-500"
                title={paper.tldr}
              >
                <span className="mr-1 font-semibold not-italic text-slate-400">
                  TLDR
                </span>
                {paper.tldr}
              </p>
            )}

            {!suppressSummaries && showAbstractByDefault && paper.abstract && (
              <SubPanel variant="flat" padded={false} className="mt-1.5 px-3 py-2 text-xs leading-relaxed text-slate-600">
                {paper.abstract}
              </SubPanel>
            )}
            {!suppressSummaries && showAbstractByDefault && !paper.abstract && (
              <div className="mt-1.5 rounded-card border border-dashed border-divider px-3 py-2 text-xs italic text-slate-400">
                No abstract available.
              </div>
            )}

            {/* Star rating (library view) */}
            {onRate && (
              <div className="mt-2" onClick={(e) => e.stopPropagation()}>
                <StarRating
                  value={paper.rating ?? 0}
                  onChange={onRate}
                />
              </div>
            )}
          </div>

        </div>

        {children}

        {/* Caller-provided quick actions row. Discover-similar pivot used to
         *  live here; it's now a small icon button in the header trailing
         *  slot, so this row only renders when a caller actually injects
         *  quickActions of their own. */}
        {quickActions && (
          <div className="mt-3 flex flex-wrap items-center gap-2" onClick={(e) => e.stopPropagation()}>
            {quickActions}
          </div>
        )}

        {/* Footer: what you can do on the left, what you can read on the right.
            The score ends at the card's right edge on every card and every
            surface, so a ranked list reads down one column. Both toggles open
            below this row, so the control you pressed does not move. */}
        {(hasActions || score != null || canToggleAbstract) && (
          <div
            className={cn(
              'mt-3 flex flex-wrap items-center justify-between gap-x-4 gap-y-2 border-t border-slate-100 pt-3',
              rank != null && 'sm:pl-10',
            )}
            onClick={(e) => e.stopPropagation()}
          >
            {hasActions && (
              <PaperActionBar
                onDismiss={onDismiss}
                onQueue={onQueue}
                onAdd={onAdd}
                onLike={onLike}
                onLove={onLove}
                onDislike={onDislike}
                onUndo={onUndo}
                disabled={actionDisabled}
                compact={isCompact || compactActions}
                dismissLabel={dismissLabel}
                dismissTitle={dismissTitle}
                dislikeLabel={dislikeLabel}
                dislikeTitle={dislikeTitle}
                savedLabel={savedLabel}
                savedReadOnly={savedReadOnly}
                  onAddToCollection={onAddToCollection}
                addToCollectionLabel={addToCollectionLabel}
                addToCollectionTitle={addToCollectionTitle}
                reaction={reaction}
                isSaved={isSaved}
                savedClickRemoves={savedClickRemoves}
                isQueued={isQueued}
                collectionAction={collectionConfirm ? (
                  <AddToCollectionMenu
                    onConfirm={collectionConfirm}
                    disabled={actionDisabled}
                    compact={isCompact || compactActions}
                    defaultSelectedIds={defaultCollectionIds}
                    isSaved={isSaved}
                  />
                ) : undefined}
              />
            )}
            {(canToggleAbstract || score != null) && (
              <div className="ml-auto flex items-center gap-3 max-sm:ml-0">
                {canToggleAbstract && (
                  <Button variant="muted" size="inline"
                    onClick={handleAbstractToggle}
                    aria-expanded={showAbstract}
                    className="gap-0.5 text-caption"
                  >
                    Abstract
                    <ChevronDown
                      className={`h-3 w-3 transition-transform duration-200 ${showAbstract ? 'rotate-180' : ''}`}
                    />
                  </Button>
                )}
                {score != null && (hasBreakdown || hasExplanation) && (
                  <HoverCard openDelay={200} closeDelay={100}>
                    <HoverCardTrigger asChild>
                      <Button variant="muted" size="inline"
                        onClick={() => {
                          const next = !showBreakdown
                          setShowBreakdown(next)
                          if (next && onExpandBreakdown) onExpandBreakdown()
                        }}
                        aria-expanded={showBreakdown}
                        className="gap-0.5 text-caption"
                        title="Show score breakdown"
                      >
                        Why
                        <ChevronDown
                          className={`h-3 w-3 transition-transform duration-200 ${showBreakdown ? 'rotate-180' : ''}`}
                        />
                      </Button>
                    </HoverCardTrigger>
                    <HoverCardContent
                      side="top"
                      align="end"
                      className="w-72 p-3"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <ScoreBreakdownTeaser
                        breakdown={scoreBreakdown}
                        explanation={explanation}
                      />
                    </HoverCardContent>
                  </HoverCard>
                )}
                {score != null && (
                  <ScoreMeter score={score} reference={scoreBreakdown?.explanation?.reference_score} />
                )}
              </div>
            )}
          </div>
        )}

        {canToggleAbstract && showAbstract && (
          <SubPanel variant="flat" padded={false} className="mt-2 px-3 py-2 text-xs leading-relaxed text-slate-600">
            {paper.abstract}
          </SubPanel>
        )}

        {showBreakdown && (hasBreakdown || hasExplanation) && (
          <SubPanel variant="flat" padded={false} className="mt-2 px-3 py-2.5">
            <p className="mb-2 text-caption font-semibold uppercase tracking-wide text-slate-400">
              Score Breakdown
            </p>
            <ScoreBreakdownPanel breakdown={scoreBreakdown} explanation={explanation} />
          </SubPanel>
        )}
        {showBreakdown && whyDetail}
      </div>
    </Card>
  )
}
