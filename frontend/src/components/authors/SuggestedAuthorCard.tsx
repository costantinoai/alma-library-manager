import { Loader2, UserMinus, UserPlus } from 'lucide-react'

import type { AuthorSuggestion } from '@/api/client'
import { Button } from '@/components/ui/button'
import { type StatusBadgeTone } from '@/components/ui/status-badge'
import { truncate } from '@/lib/utils'

interface SuggestedAuthorCardProps {
  suggestion: AuthorSuggestion
  onFollow: () => void
  onReject: () => void
  onClick?: () => void
  followPending?: boolean
  rejectPending?: boolean
}

/**
 * Human-readable label for a suggestion's provenance bucket. D12
 * (locked 2026-04-24) added `cited_by_high_signal` and
 * `semantic_similar`; network-backed `openalex_related` and
 * `s2_related` will land with AUTH-SUG-3/4 and are pre-labelled
 * here so the rail doesn't need a second churn.
 */
function kindLabel(kind: string): string {
  if (kind === 'library_core') return 'Library-heavy'
  if (kind === 'collaborator') return 'Coauthor'
  if (kind === 'cited_by_high_signal') return 'Cited by your ★4+ papers'
  if (kind === 'semantic_similar') return 'Semantically similar'
  if (kind === 'openalex_related') return 'OpenAlex related'
  if (kind === 's2_related') return 'Semantic Scholar related'
  return 'Adjacent'
}

/**
 * Tone for the provenance chip. Colours encode signal character:
 *  - `accent` = signal you curated (library_core, semantic_similar
 *     derived from your library centroid)
 *  - `positive` = strong endorsement (your high-rating papers cite them)
 *  - `info` = network / graph adjacency
 *  - `neutral` = external source you haven't curated yet
 */
function kindTone(kind: string): StatusBadgeTone {
  if (kind === 'library_core') return 'accent'
  if (kind === 'semantic_similar') return 'accent'
  if (kind === 'cited_by_high_signal') return 'positive'
  if (kind === 'collaborator' || kind === 'adjacent') return 'info'
  if (kind === 'openalex_related' || kind === 's2_related') return 'neutral'
  return 'neutral'
}

/**
 * One suggested-author card. Designed to sit in a horizontal strip of 5;
 * the whole card is clickable (opens the detail dialog) but the Follow /
 * Dismiss buttons are individually focusable and swallow the click so
 * they don't trigger the parent handler.
 */
export function SuggestedAuthorCard({
  suggestion,
  onFollow,
  onReject,
  onClick,
  followPending,
  rejectPending,
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
    <article
      onClick={onClick}
      className="group flex h-full flex-col gap-3 rounded-sm border border-alma-100 bg-[#FFFEF7] p-4 shadow-paper-sm text-left shadow-sm transition hover:border-alma-300 hover:shadow-md cursor-pointer"
    >
      <header className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <h3 className="truncate text-sm font-semibold text-alma-800">{suggestion.name}</h3>
          <div className="mt-1 flex flex-wrap items-center gap-1.5">
            {/* Provenance chip uses the Folio-blue translucent tone:
                the card sits on warm off-white paper, so saturated
                semantic tones (emerald / amber) fight the surface;
                the brand accent at low alpha reads as "metadata
                stamp" rather than "alarm" while still being clearly
                a chip and not body text. */}
            <span
              className="inline-flex items-center rounded-full bg-alma-folio/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-alma-folio"
              title={kindTone(suggestion.suggestion_type)}
            >
              {kindLabel(suggestion.suggestion_type)}
            </span>
            {/* Consensus chip — only when ≥2 independent buckets agree.
                The bonus is band-relative (+12 / +17 / +21 / +24 for
                2 / 3 / 4 / 5 buckets) and is already folded into the
                progress bar score; this chip explains *why* the score
                climbed when no single bucket would justify it. */}
            {suggestion.consensus_count && suggestion.consensus_count >= 2 ? (
              <span
                className="inline-flex items-center rounded-full bg-alma-folio/10 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-alma-folio"
                title={(suggestion.consensus_buckets ?? []).join(' · ')}
              >
                {suggestion.consensus_count} sources
              </span>
            ) : null}
            {/* Bucket calibration — only when the multiplier deviates
                meaningfully from 1.0 (fresh DB returns 1.0 for every
                bucket; a chip there would be noise). The multiplier is
                already folded into `score`, so this is purely
                provenance: "this bucket's recommendations have worked
                out for you in the past" / "haven't". */}
            {typeof suggestion.bucket_calibration_multiplier === 'number' &&
            Math.abs(suggestion.bucket_calibration_multiplier - 1.0) >= 0.05 ? (
              <span
                className={
                  suggestion.bucket_calibration_multiplier > 1.0
                    ? 'inline-flex items-center rounded-full bg-emerald-50 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-emerald-700'
                    : 'inline-flex items-center rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-amber-700'
                }
                title="Per-bucket outcome calibration: how often you've followed vs rejected this bucket's suggestions"
              >
                {suggestion.bucket_calibration_multiplier > 1.0 ? '↑' : '↓'} bucket{' '}
                {suggestion.bucket_calibration_multiplier.toFixed(2)}×
              </span>
            ) : null}
            {/* Paper-feedback projection — surface only when the magnitude
                cleared a small noise floor so neutral cards stay quiet. */}
            {typeof suggestion.paper_signal_adjustment === 'number' &&
            Math.abs(suggestion.paper_signal_adjustment) >= 1 ? (
              <span
                className={
                  suggestion.paper_signal_adjustment > 0
                    ? 'inline-flex items-center rounded-full bg-emerald-50 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-emerald-700'
                    : 'inline-flex items-center rounded-full bg-amber-50 px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide text-amber-700'
                }
                title="Net pull from your saved + dismissed papers in this area"
              >
                {suggestion.paper_signal_adjustment > 0
                  ? `+${suggestion.paper_signal_adjustment.toFixed(1)} from saves`
                  : `${suggestion.paper_signal_adjustment.toFixed(1)} from rejects`}
              </span>
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
      ) : (
        <p className="text-[11px] italic text-slate-400">No sample titles yet.</p>
      )}

      <div className="space-y-1">
        <div className="flex items-center gap-2">
          <div className="relative h-1 w-full overflow-hidden rounded-full bg-slate-200">
            <div
              className="absolute inset-y-0 left-0 rounded-full bg-alma-500 transition-all"
              style={{ width: `${pct}%` }}
            />
          </div>
          <span className="shrink-0 text-[11px] font-semibold tabular-nums text-slate-700">
            {Math.round(pct)}
          </span>
        </div>
        {signals.length > 0 ? (
          <div className="flex flex-wrap gap-1">
            {signals.map((signal, idx) => (
              <span
                key={`${signal.kind}-${idx}`}
                title={signal.subject || signal.label}
                className="inline-flex items-center rounded-full bg-alma-folio/10 px-2 py-0.5 text-[10px] font-medium text-alma-folio"
              >
                {truncate(signal.label, 38)}
              </span>
            ))}
          </div>
        ) : legacyCaption ? (
          <p className="text-[11px] text-slate-500">shares {legacyCaption}</p>
        ) : null}
      </div>

      {/* Secondary row: shared topics as Folio-blue translucent
          chips. Hidden when the signal chips already cover topic
          evidence (T7). */}
      {signals.length === 0 && suggestion.shared_topics.length > 0 ? (
        <div className="flex flex-wrap gap-1">
          {suggestion.shared_topics.slice(0, 3).map((topic) => (
            <span
              key={topic}
              className="inline-flex items-center rounded-full bg-alma-folio/10 px-2 py-0.5 text-[10px] font-medium text-alma-folio"
            >
              {truncate(topic, 24)}
            </span>
          ))}
        </div>
      ) : null}

      <footer
        className="mt-auto flex items-center gap-1"
        onClick={(e) => e.stopPropagation()}
      >
        <Button
          size="sm"
          variant="outline"
          className="flex-1 border-alma-200 bg-white text-alma-700 hover:border-alma-300 hover:bg-alma-50 hover:text-alma-800"
          onClick={(e) => {
            e.stopPropagation()
            onFollow()
          }}
          disabled={followPending || rejectPending}
        >
          {followPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <UserPlus className="h-3.5 w-3.5" />}
          Follow
        </Button>
        <Button
          size="sm"
          variant="ghost"
          className="text-slate-500 hover:bg-alma-100 hover:text-slate-700"
          onClick={(e) => {
            e.stopPropagation()
            onReject()
          }}
          disabled={followPending || rejectPending}
        >
          {rejectPending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <UserMinus className="h-3.5 w-3.5" />}
          Dismiss
        </Button>
      </footer>
    </article>
  )
}
