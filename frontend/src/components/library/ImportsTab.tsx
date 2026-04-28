import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertCircle, Info, Loader2, RefreshCw, UploadCloud, Wand2 } from 'lucide-react'

import {
  enrichImportedPublications,
  listUnresolvedImportedPublications,
  resolveImportedPublicationsOpenAlex,
} from '@/api/client'
import { ImportDialog } from '@/components/ImportDialog'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { ErrorState } from '@/components/ui/ErrorState'
import { LoadingState } from '@/components/ui/LoadingState'
import { PaperCard, type PaperCardPaper } from '@/components/shared'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'

/**
 * Map the backend `openalex_resolution_status` enum into a
 * user-readable label + StatusBadge tone. Phase C (P6) requires
 * distinguishing "never tried to resolve" from "tried and no
 * canonical OpenAlex hit" — previously the UI rendered
 * `{status}` verbatim which read as "pending / pending_enrichment /
 * not_openalex_resolved" jargon and flattened them into one
 * neutral outline chip. Tones:
 *   - `info`: pending / pending_enrichment / unresolved / empty —
 *     paper is queued, OpenAlex enrichment hasn't run yet
 *     (or ran and is waiting to re-try).
 *   - `warning`: not_openalex_resolved — we tried, nothing matched.
 *     The import is still in Library (D4), OpenAlex metadata just
 *     isn't enriched. Surfaces a distinct colour so the user can
 *     triage these separately.
 *   - `negative`: failed — the enrichment job errored (non-match).
 */
function resolveStatusBadgeProps(
  status?: string | null,
): { label: string; tone: StatusBadgeTone } {
  const s = (status || '').trim().toLowerCase()
  if (!s || s === 'pending' || s === 'pending_enrichment' || s === 'unresolved') {
    return { label: 'Pending enrichment', tone: 'info' }
  }
  if (s === 'not_openalex_resolved') {
    return { label: 'No OpenAlex match', tone: 'warning' }
  }
  if (s === 'failed') {
    return { label: 'Enrichment failed', tone: 'negative' }
  }
  return { label: status || 'Unknown', tone: 'neutral' }
}
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

export function ImportsTab() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [dialogOpen, setDialogOpen] = useState(false)

  const unresolvedQuery = useQuery({
    queryKey: ['library-import-unresolved'],
    queryFn: () => listUnresolvedImportedPublications(250),
    retry: 1,
  })

  const resolveMutation = useMutation({
    mutationFn: () =>
      resolveImportedPublicationsOpenAlex({
        unresolved_only: true,
        background: true,
        limit: 1000,
      }),
    onSuccess: (data) => {
      void invalidateQueries(queryClient, ['activity-operations'])
      if (data.status === 'noop') {
        toast({ title: 'No unresolved imports', description: data.message || 'Everything is resolved.' })
        return
      }
      if (data.status === 'already_running') {
        toast({ title: 'Already running', description: data.message || 'Resolve job is already running.' })
        return
      }
      toast({
        title: 'Resolve started',
        description: data.job_id ? `Job ${data.job_id} is now tracked in Activity.` : 'Resolution queued.',
      })
    },
    onError: () => {
      errorToast('Error', 'Failed to queue OpenAlex resolution.')
    },
  })

  const enrichMutation = useMutation({
    mutationFn: () => enrichImportedPublications(true),
    onSuccess: (data) => {
      void invalidateQueries(queryClient, ['activity-operations'])
      toast({
        title: data.status === 'already_running' ? 'Already running' : 'Enrichment started',
        description: data.job_id ? `Job ${data.job_id} is now tracked in Activity.` : (data.message || 'Enrichment queued.'),
      })
    },
    onError: () => {
      errorToast('Error', 'Failed to queue enrichment.')
    },
  })

  const unresolvedItems = unresolvedQuery.data?.items ?? []
  const unresolvedTotal = unresolvedQuery.data?.total ?? 0

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <h3 className="text-sm font-semibold text-slate-800">Imports</h3>
          <p className="text-xs text-slate-500">Import from BibTeX or Zotero, then resolve/enrich metadata.</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={() => setDialogOpen(true)}>
            <UploadCloud className="h-4 w-4" />
            Import Papers
          </Button>
          <Button
            variant="outline"
            onClick={() => resolveMutation.mutate()}
            disabled={resolveMutation.isPending}
          >
            {resolveMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Wand2 className="h-4 w-4" />}
            Resolve OpenAlex
          </Button>
          <Button
            variant="outline"
            onClick={() => enrichMutation.mutate()}
            disabled={enrichMutation.isPending}
          >
            {enrichMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
            Enrich Metadata
          </Button>
        </div>
      </div>

      <div className="flex items-start gap-2 rounded-md border border-blue-200 bg-blue-50 px-3 py-2.5 text-sm text-blue-800">
        <Info className="mt-0.5 h-4 w-4 flex-shrink-0" />
        <p>
          Imported papers land directly in <span className="font-semibold">Saved Library</span>. Use this panel to resolve and enrich imported metadata, not to promote imports into Library.
        </p>
      </div>

      <Card>
        <CardContent className="space-y-3 p-4">
          <div className="flex items-center justify-between gap-2">
            <p className="text-sm text-slate-600">
              Unresolved imported papers: <span className="font-semibold text-alma-800">{unresolvedTotal}</span>
            </p>
            <Button
              size="sm"
              variant="outline"
              onClick={() => { void invalidateQueries(queryClient, ['library-import-unresolved']) }}
            >
              <RefreshCw className="h-3.5 w-3.5" />
              Refresh
            </Button>
          </div>

          {unresolvedQuery.isLoading ? (
            <LoadingState message="Loading unresolved imports..." />
          ) : unresolvedQuery.isError ? (
            <ErrorState message="Failed to load unresolved imported papers." />
          ) : unresolvedItems.length === 0 ? (
            <Alert variant="success">
              <AlertDescription>All imported papers are resolved.</AlertDescription>
            </Alert>
          ) : (
            <div className="max-h-80 space-y-2 overflow-y-auto">
              {unresolvedItems.slice(0, 80).map((paper) => {
                const cardPaper: PaperCardPaper = {
                  id: paper.id,
                  title: paper.title,
                  authors: paper.authors,
                  year: paper.year ?? null,
                  journal: paper.journal ?? undefined,
                  doi: paper.doi ?? undefined,
                }
                const { label: statusLabel, tone: statusTone } =
                  resolveStatusBadgeProps(paper.openalex_resolution_status)
                const reason = (paper.openalex_resolution_reason || '').trim()
                const statusBadge = (
                  <StatusBadge tone={statusTone} size="sm">
                    {statusLabel}
                  </StatusBadge>
                )
                const readingStatusSlot = (
                  <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
                    {paper.doi && <span>DOI: {paper.doi}</span>}
                    {/* Hover shows the concrete resolution reason when
                        one exists (e.g. "no_doi_no_title_match").
                        Without a reason, the badge renders standalone. */}
                    {reason ? (
                      <Tooltip>
                        <TooltipTrigger asChild>
                          <span className="inline-flex">{statusBadge}</span>
                        </TooltipTrigger>
                        <TooltipContent>
                          <span className="font-mono text-[11px]">{reason}</span>
                        </TooltipContent>
                      </Tooltip>
                    ) : (
                      statusBadge
                    )}
                  </div>
                )
                return (
                  <PaperCard
                    key={paper.id}
                    paper={cardPaper}
                    size="compact"
                    readingStatusSlot={readingStatusSlot}
                  />
                )
              })}
              {unresolvedItems.length > 80 && (
                <div className="flex items-center gap-1 text-xs text-slate-500">
                  <AlertCircle className="h-3.5 w-3.5" />
                  Showing first 80 of {unresolvedItems.length} unresolved items.
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <ImportDialog
        open={dialogOpen}
        onOpenChange={setDialogOpen}
        onImportComplete={() => {
          void invalidateQueries(queryClient, ['library-import-unresolved'], ['papers'], ['library-saved'], ['library-collections'])
        }}
      />
    </div>
  )
}
