import { ExternalLink } from 'lucide-react'

import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import { ScoreMeter } from '@/components/shared/ScoreMeter'
import { ScoreBreakdownTeaser } from '@/components/ScoreBreakdownPanel'
import type { ScoreBreakdown } from '@/api/client'
import type { PaperCardPaper } from './PaperCard'

export interface PaperHoverCardProps {
  paper: PaperCardPaper
  score?: number
  scoreBreakdown?: ScoreBreakdown | null
  explanation?: string | null
  children: React.ReactNode
}

/**
 * Hover preview for a paper title. Same vibe as `AuthorHoverCard`: compact
 * summary surface that answers "should I click?" without forcing a
 * navigation. Shows title, venue, abstract teaser, citation count, score,
 * and top-3 signals.
 *
 * The trigger is passed in as `children` so callers control whether the
 * visible element is an `<a>`, `<h3>`, or anything else — HoverCardTrigger
 * renders via `asChild`.
 */
export function PaperHoverCard({
  paper,
  score,
  scoreBreakdown,
  explanation,
  children,
}: PaperHoverCardProps) {
  const abstractSnippet = paper.abstract
    ? paper.abstract.length > 280
      ? `${paper.abstract.slice(0, 280).trimEnd()}…`
      : paper.abstract
    : null
  const venueBits: string[] = []
  if (paper.journal) venueBits.push(paper.journal)
  if (paper.year != null) venueBits.push(String(paper.year))

  return (
    <HoverCard openDelay={400} closeDelay={120}>
      <HoverCardTrigger asChild>{children}</HoverCardTrigger>
      <HoverCardContent
        side="top"
        align="start"
        className="w-80 p-3"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="space-y-2.5">
          <p className="text-sm font-semibold leading-snug text-alma-800">
            {paper.title}
          </p>

          {venueBits.length > 0 && (
            <p className="truncate text-[11px] text-slate-500" title={venueBits.join(' · ')}>
              {venueBits.join(' · ')}
            </p>
          )}

          {abstractSnippet && (
            <p className="line-clamp-4 text-xs leading-relaxed text-slate-600">
              {abstractSnippet}
            </p>
          )}

          <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-500">
            {paper.cited_by_count != null && paper.cited_by_count > 0 && (
              <span className="tabular-nums">
                {paper.cited_by_count.toLocaleString()} citations
              </span>
            )}
            {score != null && <ScoreMeter score={score} />}
          </div>

          <ScoreBreakdownTeaser breakdown={scoreBreakdown} explanation={explanation} />

          <div className="flex items-center justify-between border-t border-slate-100 pt-1.5 text-[11px]">
            <span className="text-slate-400">Click card for full details</span>
            {paper.url && (
              <a
                href={paper.url}
                target="_blank"
                rel="noopener noreferrer"
                className="inline-flex items-center gap-0.5 font-medium text-alma-700 hover:text-alma-900"
                onClick={(e) => e.stopPropagation()}
              >
                Open source
                <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>
        </div>
      </HoverCardContent>
    </HoverCard>
  )
}
