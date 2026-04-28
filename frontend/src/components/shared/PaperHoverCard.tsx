import { ExternalLink } from 'lucide-react'

import { HoverCard, HoverCardContent, HoverCardTrigger } from '@/components/ui/hover-card'
import type { PaperCardPaper, ScoreSignal } from './PaperCard'

// ── Signal palette ─────────────────────────────────────────────────────────
// Mirrors the PaperCard signal colors. Kept in sync with the card's
// ScoreBreakdownTeaser so the hover preview and the in-card breakdown feel
// consistent.
const SIGNAL_META: Record<string, { label: string; color: string }> = {
  source_relevance: { label: 'Source Relevance', color: 'bg-alma-500' },
  topic_score: { label: 'Topic Match', color: 'bg-emerald-500' },
  text_similarity: { label: 'Text Similarity', color: 'bg-sky-500' },
  author_affinity: { label: 'Author Affinity', color: 'bg-violet-500' },
  journal_affinity: { label: 'Journal Affinity', color: 'bg-indigo-400' },
  recency_boost: { label: 'Recency', color: 'bg-amber-500' },
  citation_quality: { label: 'Citation Quality', color: 'bg-orange-400' },
  feedback_adj: { label: 'Your Feedback', color: 'bg-rose-400' },
  preference_affinity: { label: 'Preference Match', color: 'bg-fuchsia-400' },
  usefulness_boost: { label: 'Usefulness', color: 'bg-teal-500' },
}

function ScoreBar({ score }: { score: number }) {
  const pct = Math.round(score)
  const barColor =
    pct >= 70 ? 'bg-emerald-500' : pct >= 40 ? 'bg-amber-500' : 'bg-red-400'
  return (
    <div className="flex items-center gap-2">
      <div className="h-1.5 w-16 overflow-hidden rounded-full bg-slate-200">
        <div className={`h-1.5 rounded-full transition-all ${barColor}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs font-semibold tabular-nums text-slate-600">{pct}</span>
    </div>
  )
}

function TopSignals({
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

  if (signals.length === 0 && !explanation) return null

  return (
    <div className="rounded-md border border-slate-100 bg-parchment-50/70 p-2 space-y-1.5">
      <p className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
        Why this paper
      </p>
      {explanation && (
        <p className="text-xs italic leading-relaxed text-slate-600">{explanation}</p>
      )}
      {signals.length > 0 && (
        <ul className="space-y-1">
          {signals.map(({ key, meta, signal }) => (
            <li key={key} className="flex items-center justify-between gap-3 text-xs">
              <span className="flex items-center gap-1.5 text-slate-700">
                <span className={`inline-block h-2 w-2 rounded-full ${meta.color}`} aria-hidden />
                {meta.label}
              </span>
              <span className="tabular-nums text-slate-500">{signal.weighted.toFixed(1)}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

export interface PaperHoverCardProps {
  paper: PaperCardPaper
  score?: number
  scoreBreakdown?: Record<string, ScoreSignal> | null
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
            {score != null && <ScoreBar score={score} />}
          </div>

          <TopSignals breakdown={scoreBreakdown} explanation={explanation} />

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
