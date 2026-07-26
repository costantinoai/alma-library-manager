import { Check, Loader2, TrendingDown, TrendingUp, UserMinus, UserPlus } from 'lucide-react'

import type { AuthorSuggestion } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { SignalChip } from '@/components/shared/SignalChip'
import { StatusBadge } from '@/components/ui/status-badge'
import { Meter } from '@/components/ui/meter'
import { truncate } from '@/lib/utils'
import { authorSuggestionSourceLabel } from './authorSuggestionEvidence'

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
      <header className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 className="truncate text-sm font-semibold text-alma-800">{suggestion.name}</h3>
          {institution ? (
            <p className="mt-0.5 truncate text-[11px] text-slate-500">{institution}</p>
          ) : null}
          <div className="mt-1 flex min-w-0 flex-wrap items-center gap-1.5">
            {/* Provenance chip uses the Folio-blue translucent tone:
                the card sits on warm off-white paper, so saturated
                semantic tones (emerald / amber) fight the surface;
                the brand accent at low alpha reads as "metadata
                stamp" rather than "alarm" while still being clearly
                a chip and not body text. `max-w-full` + an inner
                truncate keep a long label (e.g. "Cited by your ★4+
                papers") inside the card when it's narrow. */}
            <StatusBadge
              tone="accent"
              size="sm"
              className="max-w-full uppercase tracking-wide"
              title={authorSuggestionSourceLabel(suggestion.suggestion_type)}
            >
              <span className="min-w-0 truncate">
                {authorSuggestionSourceLabel(suggestion.suggestion_type)}
              </span>
            </StatusBadge>
            {/* Consensus chip — only when ≥2 independent buckets agree.
                The bonus is band-relative (+12 / +17 / +21 / +24 for
                2 / 3 / 4 / 5 buckets) and is already folded into the
                progress bar score; this chip explains *why* the score
                climbed when no single bucket would justify it. */}
            {suggestion.consensus_count && suggestion.consensus_count >= 2 ? (
              // Same registry kind as Discovery's "Found by N sources" chip, so
              // multi-source agreement looks identical wherever it appears.
              <SignalChip
                kind="consensus"
                className="uppercase tracking-wide"
                title={(suggestion.consensus_buckets ?? []).join(' · ')}
              >
                {suggestion.consensus_count} sources
              </SignalChip>
            ) : null}
            {/* Bucket calibration — only when the multiplier deviates
                meaningfully from 1.0 (fresh DB returns 1.0 for every
                bucket; a chip there would be noise). The multiplier is
                already folded into `score`, so this is purely
                provenance: "this bucket's recommendations have worked
                out for you in the past" / "haven't". */}
            {typeof suggestion.bucket_calibration_multiplier === 'number' &&
            Math.abs(suggestion.bucket_calibration_multiplier - 1.0) >= 0.05 ? (
              <StatusBadge
                tone={suggestion.bucket_calibration_multiplier > 1.0 ? 'positive' : 'warning'}
                size="sm"
                icon={suggestion.bucket_calibration_multiplier > 1.0 ? TrendingUp : TrendingDown}
                className="uppercase tracking-wide"
                title="Per-bucket outcome calibration: how often you've followed vs rejected this bucket's suggestions"
              >
                bucket {suggestion.bucket_calibration_multiplier.toFixed(2)}×
              </StatusBadge>
            ) : null}
            {/* Paper-feedback projection — surface only when the magnitude
                cleared a small noise floor so neutral cards stay quiet. */}
            {typeof suggestion.paper_signal_adjustment === 'number' &&
            Math.abs(suggestion.paper_signal_adjustment) >= 1 ? (
              // Your own feedback loop speaking — same green/amber pairing and
              // same registry kinds as Discovery's "Matches what you save".
              <SignalChip
                kind={suggestion.paper_signal_adjustment > 0 ? 'taste-match' : 'taste-avoid'}
                className="uppercase tracking-wide"
                title="Net pull from your saved, rated, and removed papers in this area"
              >
                {suggestion.paper_signal_adjustment > 0
                  ? `+${suggestion.paper_signal_adjustment.toFixed(1)} from saves`
                  : `${suggestion.paper_signal_adjustment.toFixed(1)} from rejects`}
              </SignalChip>
            ) : null}
            {suggestion.local_paper_count ? (
              <span className="text-[11px] text-slate-500">
                {suggestion.local_paper_count} in DB
              </span>
            ) : null}
          </div>
        </div>
      </header>

      {suggestion.sample_titles.length > 0 ? (
        <ul className="space-y-1 text-[11px] text-slate-600">
          {suggestion.sample_titles.slice(0, 2).map((title) => (
            <li key={title} className="line-clamp-1">
              {truncate(title, 80)}
            </li>
          ))}
        </ul>
      ) : titlesLoading ? (
        <p className="text-[11px] italic text-slate-400">Loading top papers…</p>
      ) : (
        <p className="text-[11px] italic text-slate-400">No sample titles yet.</p>
      )}

      {showScore ? (
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <Meter value={pct} tone="neutral" size="xs" decorative />
            <span className="shrink-0 text-[11px] font-semibold tabular-nums text-slate-700">
              {Math.round(pct)}
            </span>
          </div>
          {signals.length > 0 ? (
            <div className="flex flex-wrap gap-1">
              {signals.map((signal, idx) => (
                <StatusBadge
                  key={`${signal.kind}-${idx}`}
                  tone="accent"
                  size="sm"
                  className="max-w-full"
                  title={signal.subject || signal.label}
                >
                  <span className="min-w-0 truncate">{truncate(signal.label, 28)}</span>
                </StatusBadge>
              ))}
            </div>
          ) : legacyCaption ? (
            <p className="text-[11px] text-slate-500">shares {legacyCaption}</p>
          ) : null}
        </div>
      ) : null}

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
          variant="ghost"
          className="shrink-0 text-critical-600 hover:bg-critical-700/10 hover:text-critical-700"
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
