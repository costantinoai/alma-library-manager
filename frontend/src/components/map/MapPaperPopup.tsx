import { ChevronDown, ExternalLink, LocateFixed, X } from 'lucide-react'

import { AddToCollectionMenu } from '@/components/discovery/AddToCollectionMenu'
import {
  PaperActionBar,
  type PaperReaction,
} from '@/components/discovery/PaperActionBar'
import { ScoreMeter } from '@/components/shared/ScoreMeter'
import { SignalChip } from '@/components/shared/SignalChip'
import { Button } from '@/components/ui/button'

export interface MapPaperNeighbour {
  id: string
  title: string
  relation?: string | null
}

export interface MapPaperSummary {
  id: string
  title: string
  authors?: string | null
  tldr?: string | null
  year?: number | null
  journal?: string | null
  citedByCount?: number | null
  score?: number | null
  statusLabel?: string | null
  branchLabel?: string | null
  clusterLabel?: string | null
  neighbours?: MapPaperNeighbour[]
}

interface MapPaperPopupProps {
  paper: MapPaperSummary
  onClose: () => void
  onOpenDetails?: () => void
  /** Discovery-only explicit navigation to this paper's list card. Dot
   *  clicks never invoke it; the user chooses this small popup action. */
  onGoToPaper?: () => void
  onQueue: () => void
  onAdd: () => void
  onLike: () => void
  onLove: () => void
  onDislike: () => void
  onUndo?: (aspect: 'membership' | 'rating' | 'reading') => void
  onAddToCollections: (collectionIds: string[]) => void | Promise<void>
  defaultCollectionIds?: string[]
  isSaved?: boolean
  isQueued?: boolean
  reaction?: PaperReaction
  pending?: boolean
  savedReadOnly?: boolean
  savedLabel?: string
}

/** The shared, interactive paper card rendered at a clicked map dot.
 *
 * It deliberately owns no mutations: Discovery passes its recommendation
 * mutations; corpus hosts pass the canonical paper mutations. That keeps one
 * visual/interaction primitive without inventing a second action contract.
 */
export function MapPaperPopup({
  paper,
  onClose,
  onOpenDetails,
  onGoToPaper,
  onQueue,
  onAdd,
  onLike,
  onLove,
  onDislike,
  onUndo,
  onAddToCollections,
  defaultCollectionIds,
  isSaved = false,
  isQueued = false,
  reaction = null,
  pending = false,
  savedReadOnly = false,
  savedLabel,
}: MapPaperPopupProps) {
  return (
    <section
      role="dialog"
      aria-label={`Actions for ${paper.title}`}
      className="space-y-3 p-3"
    >
      <div className="flex items-start gap-2">
        <div className="min-w-0 flex-1">
          <h3 className="line-clamp-3 text-sm font-semibold leading-snug text-alma-800">
            {paper.title}
          </h3>
          {paper.authors && (
            <p className="mt-1 line-clamp-2 text-[11px] leading-relaxed text-slate-500">
              {paper.authors}
            </p>
          )}
        </div>
        <button
          type="button"
          onClick={onClose}
          className="shrink-0 rounded-sm p-1 text-slate-400 hover:bg-control-quiet hover:text-slate-700"
          aria-label="Close paper popup"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      {paper.score != null && (
        <div className="flex items-center justify-between gap-3 rounded-sm border border-edge-2 px-2.5 py-2">
          <div>
            <p className="text-[11px] font-semibold text-alma-800">Internal score</p>
            <p className="text-[10px] text-slate-400">Latest Discovery relevance</p>
          </div>
          <ScoreMeter score={paper.score} />
        </div>
      )}

      <div className="flex flex-wrap gap-1">
        {paper.statusLabel && (
          <SignalChip kind="collection" title="Paper state">
            {paper.statusLabel}
          </SignalChip>
        )}
        {paper.year != null && <SignalChip kind="year">{paper.year}</SignalChip>}
      </div>

      <div className="border-t border-edge-2 pt-2.5">
        <PaperActionBar
          compact
          showLabels={false}
          onQueue={onQueue}
          onAdd={onAdd}
          onLike={onLike}
          onLove={onLove}
          onDislike={onDislike}
          onUndo={onUndo}
          disabled={pending}
          isSaved={isSaved}
          isQueued={isQueued}
          reaction={reaction}
          savedReadOnly={savedReadOnly}
          savedLabel={savedLabel}
          collectionAction={
            <AddToCollectionMenu
              compact
              disabled={pending}
              isSaved={isSaved}
              defaultSelectedIds={defaultCollectionIds}
              onConfirm={onAddToCollections}
            />
          }
        />
      </div>

      {(onGoToPaper || onOpenDetails) && (
        <div className="flex items-center justify-end gap-1">
          {onGoToPaper && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-7 gap-1.5 px-2 text-xs"
              onClick={onGoToPaper}
            >
              <LocateFixed className="h-3.5 w-3.5" />
              Go to paper
            </Button>
          )}
          {onOpenDetails && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-7 gap-1.5 px-2 text-xs"
              onClick={onOpenDetails}
            >
              Open full details
              <ExternalLink className="h-3.5 w-3.5" />
            </Button>
          )}
        </div>
      )}

      {(paper.tldr ||
        paper.journal ||
        paper.citedByCount ||
        paper.branchLabel ||
        (paper.clusterLabel && paper.clusterLabel !== 'Unclustered') ||
        (paper.neighbours?.length ?? 0) > 0) && (
        <details className="group border-t border-edge-2 pt-2">
          <summary className="flex cursor-pointer list-none items-center gap-1 text-[11px] font-medium text-slate-500 hover:text-alma-800">
            <ChevronDown className="h-3.5 w-3.5 transition-transform group-open:rotate-180" />
            More context
          </summary>
          <div className="mt-2 space-y-2.5">
            {paper.tldr && (
              <div className="rounded-sm border border-edge-2 bg-surface-2 px-2.5 py-2">
                <p className="text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                  TLDR
                </p>
                <p className="mt-1 line-clamp-4 text-xs italic leading-relaxed text-slate-600">
                  {paper.tldr}
                </p>
              </div>
            )}
            <div className="flex flex-wrap gap-1">
              {paper.citedByCount != null && paper.citedByCount > 0 && (
                <SignalChip kind="meta">
                  {paper.citedByCount.toLocaleString()} citations
                </SignalChip>
              )}
              {paper.journal && (
                <SignalChip kind="venue" title={paper.journal}>
                  <span className="max-w-40 truncate">{paper.journal}</span>
                </SignalChip>
              )}
              {paper.branchLabel && (
                <SignalChip kind="branch" title="Discovery branch">
                  {paper.branchLabel}
                </SignalChip>
              )}
              {paper.clusterLabel && paper.clusterLabel !== 'Unclustered' && (
                <SignalChip kind="topic" title="Map cluster">
                  {paper.clusterLabel}
                </SignalChip>
              )}
            </div>
            {(paper.neighbours?.length ?? 0) > 0 && (
              <div>
                <p className="mb-1.5 text-[10px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                  Neighbours on the map
                </p>
                <ul className="space-y-1.5">
                  {paper.neighbours?.slice(0, 4).map((neighbour) => (
                    <li key={neighbour.id} className="min-w-0">
                      <p className="line-clamp-1 text-[11px] font-medium text-slate-700">
                        {neighbour.title}
                      </p>
                      {neighbour.relation && (
                        <p className="line-clamp-1 text-[10px] text-slate-400">
                          {neighbour.relation}
                        </p>
                      )}
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </details>
      )}
    </section>
  )
}
