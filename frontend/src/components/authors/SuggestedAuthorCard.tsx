import { Check, Loader2, UserMinus, UserPlus } from 'lucide-react'

import type { AuthorSuggestion } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { SignalChip } from '@/components/shared/SignalChip'
import { byRarity } from '@/components/shared/signalKinds'
import { truncate } from '@/lib/utils'
import { authorSuggestionChips } from './authorSuggestionEvidence'

interface SuggestedAuthorCardProps {
  suggestion: AuthorSuggestion
  onFollow: () => void
  onReject: () => void
  onClick?: () => void
  followPending?: boolean
  rejectPending?: boolean
  /** When the author is already followed, the Follow button becomes a
   *  disabled "Following" state instead of re-triggering a follow. The
   *  suggestion rail never passes this (it filters followed authors out);
   *  the Discovery author search does, because it surfaces followed
   *  authors alongside new ones. */
  alreadyFollowed?: boolean
  /** Hide the personal-fit ranking bar. The Discovery author search reuses
   *  this card but doesn't rank results — it surfaces the author's notable
   *  papers (`sample_titles`) instead. Defaults to shown (the rail). */
  showScore?: boolean
  /** Institution / affiliation line rendered under the name. The suggestion
   *  rail doesn't carry it; the author search does. */
  institution?: string | null
  /** The author's notable papers (`sample_titles`) are still loading in a
   *  background request — show a quiet placeholder instead of the
   *  "no titles" empty state. */
  titlesLoading?: boolean
  /** How many cards in the list carry each chip (`countChipKeys`). */
  chipCounts?: Map<string, number>
}

/**
 * One suggested-author card. Sits in the rail's container-measured grid
 * (as many ≥240px columns as fit — see SuggestedAuthorsRail); the whole
 * card is clickable (opens the detail dialog) but the Follow / Dismiss
 * buttons are individually focusable and swallow the click so they don't
 * trigger the parent handler.
 */
export function SuggestedAuthorCard({
  suggestion,
  onFollow,
  onReject,
  onClick,
  followPending,
  rejectPending,
  alreadyFollowed,
  showScore = true,
  institution,
  titlesLoading,
  chipCounts,
}: SuggestedAuthorCardProps) {
  const pct = Math.max(0, Math.min(100, suggestion.score))

  // T7: prefer the backend-computed signals list (4 priority-ordered
  // evidence chips). Fall back to the pre-T7 summary caption for
  // legacy cached rows that pre-date the rollout.
  const signals = suggestion.signals ?? []
  const legacyCaption =
    signals.length === 0
      ? [
          suggestion.shared_topics.length ? `${suggestion.shared_topics.length} topics` : null,
          suggestion.shared_venues.length ? `${suggestion.shared_venues.length} venues` : null,
          suggestion.shared_followed_count ? `${suggestion.shared_followed_count} coauthors` : null,
        ]
          .filter(Boolean)
          .join(' · ')
      : ''

  return (
    <Card
      interactive
      onClick={onClick}
      className="group flex h-full flex-col gap-3 p-4 text-left"
    >
      <header className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="truncate text-sm font-semibold text-alma-800">{suggestion.name}</h3>
          {institution ? (
            <p className="mt-0.5 truncate text-caption text-slate-500">{institution}</p>
          ) : null}
        </div>
        {showScore ? (
          <span
            className="shrink-0 text-lede font-semibold tabular-nums text-slate-700"
            title={
              typeof suggestion.bucket_calibration_multiplier === 'number'
                ? `Fit score ${Math.round(pct)} of 100, including your follow history with this kind of suggestion (×${suggestion.bucket_calibration_multiplier.toFixed(2)})`
                : `Fit score ${Math.round(pct)} of 100`
            }
          >
            {Math.round(pct)}
          </span>
        ) : null}
      </header>

      {suggestion.sample_titles.length > 0 ? (
        <ul className="space-y-1 text-caption text-slate-600">
          {suggestion.sample_titles.slice(0, 2).map((title) => (
            <li key={title} className="line-clamp-1">
              {truncate(title, 80)}
            </li>
          ))}
        </ul>
      ) : titlesLoading ? (
        <p className="text-caption italic text-slate-400">Loading top papers…</p>
      ) : (
        <p className="text-caption italic text-slate-400">No sample titles yet.</p>
      )}

      {/* One row, rarest first across the rail: what sets this person apart
          from the card beside them, then what they all share. */}
      <div className="flex flex-wrap items-center gap-1">
        {byRarity(authorSuggestionChips(suggestion), (chip) => chip.rarityKey, chipCounts).map((chip) => (
          <SignalChip key={chip.key} kind={chip.kind} explain={chip.explain}>
            {chip.label}
          </SignalChip>
        ))}
        {legacyCaption && showScore ? (
          <span className="text-caption text-slate-500">shares {legacyCaption}</span>
        ) : null}
        {suggestion.local_paper_count ? (
          <span className="text-caption text-slate-500">{suggestion.local_paper_count} in DB</span>
        ) : null}
      </div>

      {/* Secondary row: shared topics as Folio-blue translucent
          chips. Hidden when the signal chips already cover topic
          evidence (T7). */}
      {signals.length === 0 && suggestion.shared_topics.length > 0 ? (
        <div className="flex flex-wrap gap-1">
          {suggestion.shared_topics.slice(0, 3).map((topic, i) => (
            // One Target glyph leads the row; the rest are bare so three
            // topics don't read as three separate signals.
            <SignalChip key={topic} kind="topic" hideIcon={i > 0} title={`Shared topic: ${topic}`}>
              {truncate(topic, 24)}
            </SignalChip>
          ))}
        </div>
      ) : null}

      <footer
        className="mt-auto flex flex-wrap items-center gap-1.5"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Follow is a filled primary action → `default` (navy alma-800), the
            app's ONE heavy button fill. Not `accent`: folio is the interactive
            identity (links, active nav, selected/checked states), and using it
            as a second button fill put two different blues on the same verb —
            folio here, navy on the same Follow in AuthorHoverCard. */}
        <Button
          size="sm"
          variant={alreadyFollowed ? 'ghost' : 'default'}
          className="min-w-0 flex-1"
          onClick={(e) => {
            e.stopPropagation()
            if (!alreadyFollowed) onFollow()
          }}
          disabled={followPending || rejectPending || alreadyFollowed}
        >
          {followPending ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : alreadyFollowed ? (
            <Check className="h-3.5 w-3.5" />
          ) : (
            <UserPlus className="h-3.5 w-3.5" />
          )}
          {alreadyFollowed ? 'Following' : 'Follow'}
        </Button>
        <Button
          size="sm"
          variant="muted"
          className="shrink-0"
          onClick={(e) => {
            e.stopPropagation()
            if (!alreadyFollowed) onReject()
          }}
          // Dismiss writes a negative signal — never offer it for an author
          // you already follow (the suggestion rail never shows followed
          // authors, but the Discovery author search does).
          disabled={followPending || rejectPending || alreadyFollowed}
        >
          {rejectPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <UserMinus className="h-3.5 w-3.5" />}
          Dismiss
        </Button>
      </footer>
    </Card>
  )
}
