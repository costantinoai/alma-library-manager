import { useState } from 'react'
import {
  Heart,
  Loader2,
  Sparkles,
  ExternalLink,
  RefreshCw,
  Database,
} from 'lucide-react'
import { DiscoverIcon } from '@/components/ui/brand-icons'
import { type SimilarityResultItem, type SimilarityResponse } from '@/api/client'
import { Button } from '@/components/ui/button'
import { StatusBadge } from '@/components/ui/status-badge'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { EmptyState } from '@/components/ui/empty-state'
import { LoadingState } from '@/components/ui/LoadingState'
import { Progress } from '@/components/ui/progress'
import { ScoreBreakdownPanel } from '@/components/ScoreBreakdownPanel'
import { SubPanel } from '@/components/ui/sub-panel'
import { formatAuthorNames } from '@/lib/authorNames'

interface SimilarResultsDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  data: SimilarityResponse | null
  isLoading: boolean
  onRefresh: () => void
  onSave: (item: SimilarityResultItem) => void
}

export function SimilarResultsDialog({
  open,
  onOpenChange,
  data,
  isLoading,
  onRefresh,
  onSave,
}: SimilarResultsDialogProps) {
  const [expandedId, setExpandedId] = useState<string | null>(null)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-accent" />
            Similar Papers
            {data && (
              <StatusBadge tone="neutral" className="ml-2">
                {data.results.length} found
              </StatusBadge>
            )}
            {data?.cached && (
              <StatusBadge tone="outline" className="ml-1 text-xs">
                <Database className="mr-1 h-3 w-3" />
                Cached
              </StatusBadge>
            )}
          </DialogTitle>
          <DialogDescription>
            Papers related to your {data?.seed_count ?? 0} selected publication{(data?.seed_count ?? 0) !== 1 ? 's' : ''}.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 overflow-y-auto space-y-3 py-2">
          {isLoading ? (
            <LoadingState message="Searching for similar papers..." />
          ) : !data || data.results.length === 0 ? (
            <EmptyState
              icon={DiscoverIcon}
              title="No similar papers found."
              description="Try selecting different papers or adjusting your discovery settings."
            />
          ) : (
            data.results.map((item, idx) => {
              const itemKey = `${item.source_type}-${item.title}-${idx}`
              const isExpanded = expandedId === itemKey
              return (
                <SubPanel variant="flat" padded={false}
                  key={itemKey} className="p-4 shadow-paper-sm transition-shadow hover:shadow-paper-md"
                >
                  <div className="flex items-start gap-3">
                    <div className="min-w-0 flex-1">
                      <h4 className="text-sm font-medium text-alma-800 leading-snug">
                        {item.title}
                      </h4>
                      {item.authors && (
                        <p className="mt-0.5 text-xs text-slate-500">{formatAuthorNames(item.authors)}</p>
                      )}
                      <div className="mt-2 flex flex-wrap items-center gap-2">
                        <Progress
                          value={Math.min(item.score, 100)}
                          className="h-1.5 w-24 [&>div]:bg-accent"
                        />
                        <span className="text-xs font-mono text-slate-500">
                          {Math.round(item.score)}%
                        </span>
                        <StatusBadge tone="outline" className="text-xs">
                          {item.source_type}
                        </StatusBadge>
                        {item.year && (
                          <span className="text-xs text-slate-400">{item.year}</span>
                        )}
                      </div>
                      {item.score_breakdown && (
                        <Button variant="link-ink" size="inline"
                          onClick={() => setExpandedId(isExpanded ? null : itemKey)}
                          className="mt-2 text-xs"
                        >
                          {isExpanded ? 'Hide details' : 'Why this paper?'}
                        </Button>
                      )}
                      {isExpanded && item.score_breakdown && (
                        <ScoreBreakdownPanel
                          breakdown={item.score_breakdown}
                          className="mt-2"
                        />
                      )}
                    </div>
                    <div className="flex shrink-0 flex-col gap-1">
                      {item.url && (
                        <a
                          href={item.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center justify-center rounded-control p-2 text-slate-500 hover:bg-control-quiet hover:text-slate-700"
                          title="Open paper"
                        >
                          <ExternalLink className="h-4 w-4" />
                        </a>
                      )}
                      <Button variant="muted" size="icon-sm"
                        onClick={() => onSave(item)}
                        title="Save to library"
                      >
                        <Heart className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </SubPanel>
              )
            })
          )}
        </div>

        <DialogFooter className="border-t pt-3">
          <Button variant="outline" onClick={onRefresh} disabled={isLoading}>
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <RefreshCw className="h-4 w-4" />
            )}
            Refresh
          </Button>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
