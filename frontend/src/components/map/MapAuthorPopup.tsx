import { ChevronDown, ExternalLink, Loader2, UserMinus, UserPlus, X } from 'lucide-react'

import { ScoreMeter } from '@/components/shared/ScoreMeter'
import { SignalChip } from '@/components/shared/SignalChip'
import { StatusBadge } from '@/components/ui/status-badge'
import { Button } from '@/components/ui/button'

export interface MapAuthorSuggestionEvidence {
  /** Human-readable provenance bucket, e.g. “OpenAlex related”. */
  source: string
  /** Concrete evidence emitted by the suggestion engine. */
  reasons: string[]
  /** Personal-fit rank from the author suggestion engine. */
  score?: number | null
}

export interface MapAuthorSummary {
  id: string
  name: string
  affiliation?: string | null
  publicationCount?: number | null
  hIndex?: number | null
  score?: number | null
  clusterLabel?: string | null
  interests?: string[]
  suggestion?: MapAuthorSuggestionEvidence | null
}

interface MapAuthorPopupProps {
  author: MapAuthorSummary
  isFollowed: boolean
  isOwner?: boolean
  pending?: boolean
  onFollow: () => void
  onUnfollow: () => void
  onOpenDetails: () => void
  onClose: () => void
}

/** Author-network counterpart to MapPaperPopup. */
export function MapAuthorPopup({
  author,
  isFollowed,
  isOwner = false,
  pending = false,
  onFollow,
  onUnfollow,
  onOpenDetails,
  onClose,
}: MapAuthorPopupProps) {
  return (
    <section
      role="dialog"
      aria-label={`Actions for ${author.name}`}
      className="space-y-3 p-3"
    >
      <div className="flex items-start gap-2">
        <div className="min-w-0 flex-1">
          <h3 className="truncate text-sm font-semibold text-alma-800">{author.name}</h3>
          {author.affiliation && (
            <p className="mt-1 line-clamp-2 text-[11px] text-slate-500">{author.affiliation}</p>
          )}
        </div>
        <button
          type="button"
          onClick={onClose}
          className="shrink-0 rounded-sm p-1 text-slate-400 hover:bg-control-quiet hover:text-slate-700"
          aria-label="Close author popup"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      <div className="flex flex-wrap gap-1">
        {isOwner && <StatusBadge tone="accent" size="sm">This is you</StatusBadge>}
        {isFollowed && <StatusBadge tone="positive" size="sm">Following</StatusBadge>}
        {author.suggestion && <StatusBadge tone="warning" size="sm">Suggested</StatusBadge>}
        {author.publicationCount != null && (
          <SignalChip kind="meta">{author.publicationCount} papers</SignalChip>
        )}
      </div>

      {author.score != null && (
        <div className="flex items-center justify-between rounded-sm border border-edge-2 px-2.5 py-2">
          <div>
            <p className="text-xs font-medium text-alma-800">Internal score</p>
            <p className="text-[10px] text-slate-400">Mean relevance of their papers</p>
          </div>
          <ScoreMeter score={author.score} />
        </div>
      )}

      <div className="flex items-center gap-2 border-t border-edge-2 pt-2.5">
        {!isOwner && (
          <Button
            type="button"
            variant={isFollowed ? 'outline' : 'default'}
            size="sm"
            className="h-7 gap-1.5 text-xs"
            disabled={pending}
            onClick={isFollowed ? onUnfollow : onFollow}
          >
            {pending ? (
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
            ) : isFollowed ? (
              <UserMinus className="h-3.5 w-3.5" />
            ) : (
              <UserPlus className="h-3.5 w-3.5" />
            )}
            {isFollowed ? 'Unfollow' : 'Follow'}
          </Button>
        )}
        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="ml-auto h-7 gap-1.5 text-xs"
          onClick={onOpenDetails}
        >
          Open full profile
          <ExternalLink className="h-3.5 w-3.5" />
        </Button>
      </div>

      {(author.hIndex != null ||
        (author.interests?.length ?? 0) > 0 ||
        (author.clusterLabel && author.clusterLabel !== 'Unclustered') ||
        author.suggestion) && (
        <details className="group border-t border-edge-2 pt-2">
          <summary className="flex cursor-pointer list-none items-center gap-1 text-[11px] font-medium text-slate-500 hover:text-alma-800">
            <ChevronDown className="h-3.5 w-3.5 transition-transform group-open:rotate-180" />
            More context
          </summary>
          <div className="mt-2 space-y-2.5">
            <div className="flex flex-wrap gap-1">
              {author.hIndex != null && <SignalChip kind="meta">h-index {author.hIndex}</SignalChip>}
              {author.clusterLabel && author.clusterLabel !== 'Unclustered' && (
                <SignalChip kind="topic" title="Author-network cluster">
                  {author.clusterLabel}
                </SignalChip>
              )}
              {(author.interests ?? []).slice(0, 3).map((interest) => (
                <SignalChip key={interest} kind="topic">{interest}</SignalChip>
              ))}
            </div>
            {author.suggestion && (
              <div className="space-y-2 rounded-sm border border-gold-300/70 bg-gold-50/50 px-2.5 py-2">
                <div className="flex items-center justify-between gap-2">
                  <div>
                    <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-gold-700">
                      Why suggested
                    </p>
                    <p className="text-[11px] text-slate-600">{author.suggestion.source}</p>
                  </div>
                  {author.suggestion.score != null && (
                    <span
                      className="shrink-0 text-[11px] font-semibold tabular-nums text-gold-700"
                      title="Author suggestion fit score"
                    >
                      fit {Math.round(author.suggestion.score)}/100
                    </span>
                  )}
                </div>
                {author.suggestion.reasons.length > 0 && (
                  <ul className="space-y-1 text-[11px] leading-snug text-slate-600">
                    {author.suggestion.reasons.slice(0, 4).map((reason) => (
                      <li key={reason}>• {reason}</li>
                    ))}
                  </ul>
                )}
              </div>
            )}
          </div>
        </details>
      )}
    </section>
  )
}
