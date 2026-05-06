import { useEffect, useRef, useState } from 'react'
import { ChevronDown, ExternalLink, HelpCircle, Loader2, Plus, Compass } from 'lucide-react'

import { Card } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import { AuthorHoverCard } from '@/components/authors/AuthorHoverCard'
import { PaperHoverCard } from './PaperHoverCard'
import { PaperActionBar, type PaperReaction } from '@/components/discovery/PaperActionBar'
import { StarRating } from '@/components/StarRating'
import { trackInteraction } from '@/api/client'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { cn, normalizeAuthorName, truncate } from '@/lib/utils'

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

export interface ScoreSignal {
  value: number
  weight: number
  weighted: number
  description?: string
}

interface PaperCardProps {
  paper: PaperCardPaper
  score?: number
  rank?: number
  scoreBreakdown?: Record<string, ScoreSignal> | null
  explanation?: string | null
  followedAuthorNames?: Set<string>
  followAuthorPendingName?: string | null
  onFollowAuthor?: (authorName: string, paperId: string) => void
  onDetails?: () => void
  onDismiss?: () => void
  onQueue?: () => void
  onAdd?: () => void
  onLike?: () => void
  onLove?: () => void
  onDislike?: () => void
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
  /** Optional slot inserted inline with the title, right-aligned. Use for
   *  row-context affordances that must sit next to the title (e.g. a
   *  reading-status pill in a compact Library row, a provenance chip in
   *  the Corpus explorer). */
  trailingHeader?: React.ReactNode
  /** Active reaction on the paper (like/love/dislike mutually exclusive). */
  reaction?: PaperReaction
  /** Whether the paper is already saved to Library (controls Save button). */
  isSaved?: boolean
  /** Whether the paper is already on the reading list (controls Queue button). */
  isQueued?: boolean
  /** When true, the abstract is expanded by default (Feed extended view). */
  forceShowAbstract?: boolean
  /** When true, the TLDR line and the abstract toggle are both hidden,
   *  even at the `default` size. Used by Discovery's normal view to keep
   *  cards dense — the user opens the detail panel for the full text. */
  suppressSummaries?: boolean
  /** When true, the bottom action bar renders in its compact (icon-only,
   *  shorter-height) form even at the `default` card size. Lets surfaces
   *  keep dense triage buttons without flipping the whole card to compact. */
  compactActions?: boolean
  /** Explicit override for action-bar label visibility. */
  showActionLabels?: boolean
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

// ── Signal labels & colors ──

const SIGNAL_META: Record<string, { label: string; color: string; description: string }> = {
  source_relevance:    { label: 'Source Relevance',    color: 'bg-alma-500',    description: 'Position in retrieval results (1st = highest)' },
  topic_score:         { label: 'Topic Match',         color: 'bg-emerald-500', description: 'Topic overlap with your rated papers' },
  text_similarity:     { label: 'Text Similarity',     color: 'bg-sky-500',     description: 'Semantic similarity to your top-rated papers' },
  author_affinity:     { label: 'Author Affinity',     color: 'bg-violet-500',  description: 'Author overlap with papers you follow' },
  journal_affinity:    { label: 'Journal Affinity',    color: 'bg-indigo-400',  description: 'Published in a journal you read' },
  recency_boost:       { label: 'Recency',             color: 'bg-amber-500',   description: 'Publication recency (newer = higher)' },
  citation_quality:    { label: 'Citation Quality',    color: 'bg-orange-400',  description: 'Citation count quality indicator' },
  feedback_adj:        { label: 'Your Feedback',       color: 'bg-rose-400',    description: 'Adjusted based on your past feedback' },
  preference_affinity: { label: 'Preference Match',    color: 'bg-fuchsia-400', description: 'Affinity learned from your accumulated feedback interactions' },
  usefulness_boost:    { label: 'Usefulness',          color: 'bg-teal-500',    description: 'Rewards timely, credible, and less redundant papers' },
}

// ── Score bar ──

function ScoreBar({ score }: { score: number }) {
  const pct = Math.round(score)
  let barColor: string
  if (pct >= 70) barColor = 'bg-emerald-500'
  else if (pct >= 40) barColor = 'bg-amber-500'
  else barColor = 'bg-red-400'
  return (
    <div className="flex items-center gap-2">
      <div className="h-1.5 w-16 overflow-hidden rounded-full bg-slate-200">
        <div
          className={`h-1.5 rounded-full transition-all ${barColor}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      <span className="text-xs font-semibold tabular-nums text-slate-600">{pct}</span>
    </div>
  )
}

// ── Score breakdown teaser (HoverCard preview) ──
//
// Lightweight summary shown when the user hovers the "Why" chip. Surfaces
// only the top-3 contributing signals plus the (optional) provenance
// explanation, so the card can preview intent without forcing a full click
// into the expanded panel. Clicking the chip still toggles the in-card
// ScoreBreakdownPanel for the deep view.
function ScoreBreakdownTeaser({
  breakdown,
  explanation,
}: {
  breakdown?: Record<string, ScoreSignal> | null
  explanation?: string | null
}) {
  const signals = Object.entries(breakdown ?? {})
    .map(([key, signal]) => ({
      key,
      meta: SIGNAL_META[key] || { label: key.replace(/_/g, ' '), color: 'bg-slate-400' },
      signal,
    }))
    .filter(({ signal }) => signal.weighted > 0.001)
    .sort((a, b) => b.signal.weighted - a.signal.weighted)
    .slice(0, 3)

  const hasSignals = signals.length > 0

  if (!hasSignals && !explanation) {
    return <p className="text-xs text-slate-400">No signal data for this recommendation.</p>
  }

  return (
    <div className="space-y-2">
      <EyebrowLabel tone="muted">Why this paper</EyebrowLabel>
      {explanation && (
        <p className="text-xs italic leading-relaxed text-slate-600">{explanation}</p>
      )}
      {hasSignals && (
        <ul className="space-y-1.5">
          {signals.map(({ key, meta, signal }) => (
            <li key={key} className="flex items-center justify-between gap-3 text-xs">
              <span className="flex items-center gap-1.5 text-slate-700">
                <span className={cn('inline-block h-2 w-2 rounded-full', meta.color)} aria-hidden />
                {meta.label}
              </span>
              <span className="tabular-nums text-slate-500">{signal.weighted.toFixed(1)}</span>
            </li>
          ))}
        </ul>
      )}
      <p className="border-t border-slate-100 pt-1.5 text-[11px] text-slate-400">
        Click for full breakdown
      </p>
    </div>
  )
}

// ── Score breakdown panel ──

function ScoreBreakdownPanel({
  breakdown,
  explanation,
}: {
  breakdown?: Record<string, ScoreSignal> | null
  explanation?: string | null
}) {
  // Sort signals by weighted value (highest first), filter out zero
  const signals = Object.entries(breakdown ?? {})
    .map(([key, signal]) => ({
      key,
      meta: SIGNAL_META[key] || { label: key.replace(/_/g, ' '), color: 'bg-slate-400' },
      signal,
    }))
    .filter(({ signal }) => signal.weighted > 0.001)
    .sort((a, b) => b.signal.weighted - a.signal.weighted)

  if (signals.length === 0) {
    if (explanation) {
      return <p className="py-2 text-xs italic text-slate-500">{explanation}</p>
    }
    return <p className="py-2 text-xs text-slate-400">No signal data available for this recommendation.</p>
  }

  const maxWeighted = Math.max(...signals.map((s) => s.signal.weighted), 0.01)

  return (
    <div className="space-y-2">
      {explanation && (
        <p className="text-xs italic text-slate-500 pb-1 border-b border-slate-100">{explanation}</p>
      )}
      {signals.map(({ key, meta, signal }) => {
        const barPct = Math.round((signal.weighted / maxWeighted) * 100)
        return (
          <div key={key} className="group">
            <div className="mb-0.5 flex items-center justify-between">
              <span className="inline-flex items-center gap-1 text-[11px] font-medium text-slate-600">
                {meta.label}
                <span title={meta.description} className="cursor-help"><HelpCircle className="h-3 w-3 text-slate-300 hover:text-slate-500" /></span>
              </span>
              <span className="tabular-nums text-[11px] text-slate-400">
                {signal.weighted.toFixed(1)}
                <span className="ml-1 text-slate-300">
                  ({(signal.value * 100).toFixed(0)}% &times; {signal.weight.toFixed(2)}w)
                </span>
              </span>
            </div>
            <div className="h-1.5 w-full overflow-hidden rounded-full bg-parchment-100">
              <div
                className={`h-1.5 rounded-full transition-all duration-300 ${meta.color}`}
                style={{ width: `${barPct}%`, opacity: 0.85 }}
              />
            </div>
          </div>
        )
      })}
    </div>
  )
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
  onDetails,
  onDismiss,
  onQueue,
  onAdd,
  onLike,
  onLove,
  onDislike,
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
  onExpandBreakdown,
  quickActions,
  sources,
  readingStatusSlot,
  trailingHeader,
  reaction = null,
  isSaved = false,
  isQueued = false,
  forceShowAbstract = false,
  suppressSummaries = false,
  compactActions = false,
  showActionLabels,
  selection,
}: PaperCardProps) {
  // Resolve the effective size: explicit `size` wins; legacy `compact` prop
  // maps to 'compact' for in-flight call sites; otherwise 'default'.
  const effectiveSize: PaperCardSize = size ?? (compact ? 'compact' : 'default')
  const isCompact = effectiveSize === 'compact'
  const isDetailed = effectiveSize === 'detailed'
  const showAbstractByDefault = forceShowAbstract || isDetailed
  const [showBreakdown, setShowBreakdown] = useState(isDetailed)
  const [showAbstract, setShowAbstract] = useState(showAbstractByDefault)

  // When the caller flips forceShowAbstract or size between variants keep the
  // card in sync so already-mounted cards update with the surface choice.
  useEffect(() => {
    setShowAbstract(showAbstractByDefault)
  }, [showAbstractByDefault])
  const abstractExpandedAt = useRef<number | null>(null)
  const hasActions = !!(onDismiss || onQueue || onLike || onLove || onAdd || onDislike)

  // T15 — derived display helpers for the card's metadata strip.
  //
  // Year inline with authors: pub_date preferred over bare year because
  // "Feb 2024" is more scannable than "2024". Short-form via en-GB
  // locale because it's compact ("13 Feb 2024" not "February 13, 2024").
  const yearInline = ((): string | null => {
    const pubDate = (paper.publication_date || '').trim()
    if (pubDate) {
      const parsed = new Date(pubDate)
      if (!isNaN(parsed.getTime())) {
        return parsed.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })
      }
    }
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
  const padding = isCompact ? 'p-3' : 'p-4'
  const hasBreakdown = scoreBreakdown && Object.keys(scoreBreakdown).length > 0
  const hasExplanation = !!explanation?.trim()
  const authorNames = parseAuthorNames(paper.authors)
  const canFollowAuthors = !!(paper.id && onFollowAuthor)

  // Track abstract engagement duration on collapse or unmount
  const flushAbstractEngagement = () => {
    if (abstractExpandedAt.current && paper.id) {
      const durationMs = Date.now() - abstractExpandedAt.current
      if (durationMs > ABSTRACT_MIN_DWELL_MS) {
        trackInteraction('abstract_engagement', paper.id, { duration_ms: durationMs })
      }
      abstractExpandedAt.current = null
    }
  }

  // Flush on unmount
  useEffect(() => flushAbstractEngagement, [])

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
      // tone="content" — papers in Feed/Discovery/Library use the
      // warmer parchment-tinted surface so they read as "real paper
      // with ink on it" against the cream chrome around them. Avoids
      // the cream-on-cream flatness when a PaperCard sits inside a
      // section Card.
      tone="content"
      className={cn(
        'group/paper-card relative overflow-hidden transition-all duration-150 hover:shadow-md',
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
          className="pointer-events-none absolute -left-3 -top-3 z-10 h-6 w-6 rotate-45 bg-gold-400"
          aria-hidden
        />
      )}
      {/* Selection rail — page-gutter metaphor. Absolute-positioned so it
          spans the full card height without being part of the flow; the
          content div reserves `pl-11` to keep the title off the rail.
          Idle: invisible. Hover: soft slate fill + checkbox fades in.
          Selected: alma ribbon + inverted (white-on-alma) checkbox. */}
      {selection && (
        <div
          className={cn(
            'absolute inset-y-0 left-0 z-10 flex w-8 items-start justify-center pt-[18px] transition-colors duration-200',
            selection.checked
              ? 'bg-alma-500'
              : 'bg-transparent group-hover/paper-card:bg-parchment-50',
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
                  'border-white/80 data-[state=checked]:border-white data-[state=checked]:bg-alma-chrome data-[state=checked]:text-alma-600',
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
            <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-full bg-parchment-100 text-[11px] font-bold text-slate-500">
              {rank}
            </div>
          )}

          <div className="min-w-0 flex-1">
            {/* Title */}
            <div className="flex items-start gap-1.5">
              {/* Trailing header slot — pivot ("Discover similar") sits here as
                  a small icon button so it never costs a full action row. Any
                  caller-supplied trailingHeader (e.g. Library's reading-status
                  pill) renders alongside it; if neither is present the slot
                  collapses entirely. */}
              {(onPivot || trailingHeader) && (
                <div
                  className="ml-auto order-last flex shrink-0 items-center gap-1.5"
                  onClick={(e) => e.stopPropagation()}
                >
                  {onPivot && (
                    <button
                      type="button"
                      onClick={(e) => { e.stopPropagation(); onPivot() }}
                      title="Discover similar papers — re-seed Discovery with this paper as the anchor"
                      aria-label="Discover similar papers"
                      className={cn(
                        'inline-flex h-7 w-7 items-center justify-center rounded-full border border-[var(--color-border)] bg-alma-chrome text-slate-500 shadow-sm transition-colors duration-150',
                        'hover:border-alma-300 hover:bg-alma-50 hover:text-alma-700',
                        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-500 focus-visible:ring-offset-1',
                      )}
                    >
                      <Compass className="h-3.5 w-3.5" aria-hidden />
                    </button>
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
                    className="text-[15px] font-semibold leading-snug text-alma-800 transition-colors hover:text-alma-600"
                    onClick={(e) => { e.stopPropagation(); handleExternalLinkClick('url') }}
                  >
                    {paper.title}
                    <ExternalLink className="ml-1 inline-block h-3 w-3 text-slate-300" />
                  </a>
                ) : (
                  <h3 className="text-[15px] font-semibold leading-snug text-alma-800">
                    {paper.title}
                  </h3>
                )}
              </PaperHoverCard>
            </div>

            {/* Authors — with publication year appended inline
                (T15, 2026-04-24). Year was previously a separate row;
                inline keeps the "who, when" pair readable at a glance
                and frees vertical space. Hidden when the paper has no
                year at all (sparse-field policy). */}
            {paper.authors && (
              <div className="mt-0.5 flex flex-wrap items-center gap-x-1.5 gap-y-1 text-sm leading-snug text-slate-500">
                {(authorNames.length > 0 ? authorNames : [truncate(paper.authors, 120)]).map((authorName, index, list) => {
                  const normalized = normalizeAuthorName(authorName)
                  const isFollowed = followedAuthorNames?.has(normalized) ?? false
                  const isPending = followAuthorPendingName === normalized
                  const canWrapHover = authorNames.length > 0
                  const label = (
                    <span className="cursor-default rounded px-0.5 transition-colors hover:bg-parchment-100 hover:text-slate-700">
                      {authorName}
                    </span>
                  )
                  return (
                    <span key={`${paper.id}-${authorName}-${index}`} className="group/author inline-flex items-center gap-1">
                      {canWrapHover ? (
                        <AuthorHoverCard
                          name={authorName}
                          isFollowed={isFollowed}
                          followPending={isPending}
                          onFollow={canFollowAuthors ? () => onFollowAuthor?.(authorName, paper.id) : undefined}
                        >
                          {label}
                        </AuthorHoverCard>
                      ) : (
                        label
                      )}
                      {canFollowAuthors && !isFollowed && (
                        <button
                          type="button"
                          className="inline-flex h-4 w-4 items-center justify-center rounded-full border border-[var(--color-border)] bg-alma-chrome text-slate-400 opacity-0 transition group-hover/author:opacity-100 hover:border-alma-200 hover:text-alma-600 disabled:cursor-not-allowed disabled:opacity-100"
                          onClick={(event) => {
                            event.preventDefault()
                            event.stopPropagation()
                            onFollowAuthor?.(authorName, paper.id)
                          }}
                          disabled={isPending}
                          title={`Follow ${authorName}`}
                          aria-label={`Follow ${authorName}`}
                        >
                          {isPending ? <Loader2 className="h-3 w-3 animate-spin" /> : <Plus className="h-3 w-3" />}
                        </button>
                      )}
                      {index < list.length - 1 && <span className="text-slate-300">,</span>}
                    </span>
                  )
                })}
                {yearInline && (
                  <span className="text-slate-400 tabular-nums" title={paper.publication_date || undefined}>
                    · {yearInline}
                  </span>
                )}
              </div>
            )}

            {/* Metadata strip — collapsed to one dense line with
                bullet separators (T15, 2026-04-24). Holds venue,
                citations (with S2 influential-count when > 0),
                paper_signal ranking, user star rating, and the
                Discovery ScoreBar + Why affordance. Every field is
                optional (sparse-field policy); the row hides entirely
                when nothing to show. Year is in the authors row
                above. */}
            {(paper.journal ||
              citationsLabel ||
              rankDisplay != null ||
              starDisplay ||
              score != null) && (
              <div className="mt-1.5 flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-slate-500">
                {paper.journal && (
                  <span
                    className="truncate font-medium text-slate-600"
                    title={paper.journal}
                  >
                    {truncate(paper.journal, 50)}
                  </span>
                )}
                {citationsLabel && (
                  <>
                    {paper.journal && <span className="text-slate-300">·</span>}
                    <span
                      className="tabular-nums"
                      title={
                        (paper.influential_citation_count ?? 0) > 0
                          ? `${paper.cited_by_count} citations (${paper.influential_citation_count} flagged influential by S2)`
                          : `${paper.cited_by_count} citations`
                      }
                    >
                      {citationsLabel}
                    </span>
                  </>
                )}
                {rankDisplay != null && (
                  <>
                    <span className="text-slate-300">·</span>
                    <span
                      className="tabular-nums text-slate-600"
                      title="paper_signal composite (0–100) — ALMa's taste-fit score. Distinct from your star rating."
                    >
                      Rank {rankDisplay}
                    </span>
                  </>
                )}
                {starDisplay && (
                  <>
                    <span className="text-slate-300">·</span>
                    <span
                      className="tabular-nums text-amber-600"
                      title={`Your rating: ${paper.rating}/5`}
                    >
                      {starDisplay}
                    </span>
                  </>
                )}
                {score != null && (
                  <>
                    <span className="text-slate-300">·</span>
                    <span className="inline-flex items-center gap-2">
                      <ScoreBar score={score} />
                      {(hasBreakdown || hasExplanation) && (
                        <HoverCard openDelay={200} closeDelay={100}>
                          <HoverCardTrigger asChild>
                            <button
                              type="button"
                              onClick={(e) => {
                                e.stopPropagation()
                                const next = !showBreakdown
                                setShowBreakdown(next)
                                if (next && onExpandBreakdown) onExpandBreakdown()
                              }}
                              className="inline-flex items-center gap-0.5 rounded-full px-1.5 py-0.5 text-[11px] font-medium text-slate-400 transition-colors hover:bg-parchment-100 hover:text-slate-600"
                              title="Show score breakdown"
                            >
                              Why
                              <ChevronDown
                                className={`h-3 w-3 transition-transform duration-200 ${showBreakdown ? 'rotate-180' : ''}`}
                              />
                            </button>
                          </HoverCardTrigger>
                          <HoverCardContent
                            side="top"
                            align="start"
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
                    </span>
                  </>
                )}
              </div>
            )}

            {/* Provenance chips — which external sources returned this paper. */}
            {sources && sources.length > 0 && (
              <div className="mt-1.5 flex flex-wrap items-center gap-1">
                {sources.map((source) => (
                  <span
                    key={source}
                    className="inline-flex items-center rounded-full border border-[var(--color-border)] bg-parchment-50 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-slate-500"
                    title={`Returned by ${source}`}
                  >
                    {source.replace(/_/g, ' ')}
                  </span>
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
                className="mt-1.5 line-clamp-2 text-[11.5px] italic leading-snug text-slate-500"
                title={paper.tldr}
              >
                <span className="mr-1 font-semibold not-italic text-slate-400">
                  TLDR
                </span>
                {paper.tldr}
              </p>
            )}

            {!suppressSummaries && !showAbstractByDefault && paper.abstract && (
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); handleAbstractToggle() }}
                className="mt-1.5 inline-flex items-center gap-0.5 text-[11px] font-medium text-slate-400 transition-colors hover:text-slate-600"
              >
                Abstract
                <ChevronDown
                  className={`h-3 w-3 transition-transform duration-200 ${showAbstract ? 'rotate-180' : ''}`}
                />
              </button>
            )}
            {!suppressSummaries && ((showAbstractByDefault || showAbstract) && paper.abstract) && (
              <div className="mt-1.5 rounded-md border border-slate-100 bg-parchment-50/50 px-3 py-2 text-xs leading-relaxed text-slate-600">
                {paper.abstract}
              </div>
            )}
            {!suppressSummaries && showAbstractByDefault && !paper.abstract && (
              <div className="mt-1.5 rounded-md border border-dashed border-slate-200 px-3 py-2 text-xs italic text-slate-400">
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

        {/* Score breakdown (expandable) */}
        {showBreakdown && (hasBreakdown || hasExplanation) && (
          <div className="mt-3 rounded-md border border-slate-100 bg-parchment-50/50 px-3 py-2.5">
            <p className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-slate-400">
              Score Breakdown
            </p>
            <ScoreBreakdownPanel breakdown={scoreBreakdown} explanation={explanation} />
          </div>
        )}

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

        {/* Action bar */}
        {hasActions && (
          <div className="mt-3 border-t border-slate-100 pt-3" onClick={(e) => e.stopPropagation()}>
            <PaperActionBar
              onDismiss={onDismiss}
              onQueue={onQueue}
              onAdd={onAdd}
              onLike={onLike}
              onLove={onLove}
              onDislike={onDislike}
              disabled={actionDisabled}
              compact={isCompact || compactActions}
              dismissLabel={dismissLabel}
              dismissTitle={dismissTitle}
              dislikeLabel={dislikeLabel}
              dislikeTitle={dislikeTitle}
              reaction={reaction}
              isSaved={isSaved}
              isQueued={isQueued}
              showLabels={showActionLabels}
            />
          </div>
        )}
      </div>
    </Card>
  )
}
