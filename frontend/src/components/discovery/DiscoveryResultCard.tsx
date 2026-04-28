import { useEffect, useRef } from 'react'

import type { Recommendation } from '@/api/client'
import { Badge } from '@/components/ui/badge'
import { PaperCard, type PaperCardPaper, type ScoreSignal } from '@/components/shared'
import { formatDate, truncate } from '@/lib/utils'

import { SOURCE_TYPE_CONFIG } from './constants'

interface DiscoveryResultCardProps {
  rec: Recommendation
  onLike: (id: string) => void
  onDismiss: (id: string) => void
  onSeen: (id: string) => void
  likeLoading: boolean
  dismissLoading: boolean
}

export function DiscoveryResultCard({
  rec,
  onLike,
  onDismiss,
  onSeen,
  likeLoading,
  dismissLoading,
}: DiscoveryResultCardProps) {
  const cardRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (rec.seen) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          onSeen(rec.id)
          observer.disconnect()
        }
      },
      { threshold: 0.5 },
    )
    if (cardRef.current) observer.observe(cardRef.current)
    return () => observer.disconnect()
  }, [rec.id, rec.seen, onSeen])

  const sourceConfig = SOURCE_TYPE_CONFIG[rec.source_type]
  const paper: PaperCardPaper = {
    id: rec.id,
    title: rec.recommended_title,
    authors: rec.recommended_authors ?? '',
    url: rec.recommended_url ?? undefined,
  }
  const sources: string[] = []
  if (rec.source_type) sources.push(rec.source_type)
  if (rec.source_api && rec.source_api !== rec.source_type) sources.push(rec.source_api)

  // Visual tone overlay: NEW / liked / dismissed — applied on the outer
  // wrapper, not inside PaperCard, so the primitive stays neutral.
  const toneClass = rec.liked
    ? 'border-pink-200 bg-pink-50/30'
    : rec.dismissed
      ? 'border-slate-200 opacity-60'
      : !rec.seen
        ? 'border-alma-200 bg-alma-50/30'
        : ''

  return (
    <div ref={cardRef}>
      <PaperCard
        paper={paper}
        size="default"
        className={toneClass}
        score={rec.score}
        scoreBreakdown={rec.score_breakdown as unknown as Record<string, ScoreSignal> | null}
        sources={sources.length > 0 ? sources : undefined}
        onLike={!rec.liked && !rec.dismissed ? () => onLike(rec.id) : undefined}
        onDismiss={!rec.liked && !rec.dismissed ? () => onDismiss(rec.id) : undefined}
        actionDisabled={likeLoading || dismissLoading}
        reaction={rec.liked ? 'like' : null}
      >
        {/* Surface-specific chrome: the colored source rail + meta row
            (via / date / NEW / Liked) lives as PaperCard children so the
            card layout stays consistent while keeping Discovery's
            provenance wayfinding. */}
        <div className="mt-2 flex flex-wrap items-center gap-2">
          {sourceConfig && (
            <span
              className="inline-block h-2 w-2 rounded-full"
              style={{ backgroundColor: sourceConfig.color }}
              aria-hidden
            />
          )}
          {rec.source_key && (
            <span className="text-xs text-slate-400" title={rec.source_key}>
              via {truncate(rec.source_key, 35)}
            </span>
          )}
          <span className="text-xs text-slate-400">{formatDate(rec.created_at)}</span>
          {!rec.seen && (
            <Badge variant="default" className="bg-alma-500 text-[10px] text-white">
              NEW
            </Badge>
          )}
        </div>
      </PaperCard>
    </div>
  )
}
