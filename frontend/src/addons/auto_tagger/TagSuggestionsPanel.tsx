import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Check, Cpu, Loader2, RefreshCw, Sparkles, XCircle } from 'lucide-react'

import { listAllSavedPapers } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { ErrorState } from '@/components/ui/ErrorState'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { errorToast, useToast } from '@/hooks/useToast'
import { describeJobLaunch } from '@/lib/activity'
import { formatPercent } from '@/lib/format'
import { invalidateQueries } from '@/lib/queryHelpers'

import {
  acceptTagSuggestion,
  bulkGenerateTagSuggestions,
  dismissTagSuggestion,
  getTagSuggestions,
} from './api'
import { SubPanel } from '@/components/ui/sub-panel'

/** Generate tag suggestions, then accept or dismiss them one paper at a time. */
export function TagSuggestionsPanel() {
  const [paperSelection, setPaperSelection] = useState('')
  const [suggestionPaperId, setSuggestionPaperId] = useState<string | null>(null)

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const likesQuery = useQuery({
    queryKey: ['library-saved', 'tag-picker'],
    queryFn: listAllSavedPapers,
    retry: 1,
  })

  const suggestionsQuery = useQuery({
    queryKey: ['tag-suggestions', suggestionPaperId],
    queryFn: () => getTagSuggestions(suggestionPaperId as string),
    enabled: Boolean(suggestionPaperId),
    retry: 1,
  })

  const bulkSuggestMutation = useMutation({
    mutationFn: () => bulkGenerateTagSuggestions(),
    onSuccess: (data) => {
      void invalidateQueries(queryClient, ['activity-operations'])
      toast(describeJobLaunch(data, 'Smart tagging'))
    },
    onError: () => {
      errorToast('Error', 'Failed to start bulk tag suggestion generation.')
    },
  })

  const acceptSuggestionMutation = useMutation({
    mutationFn: ({ paperId, tag }: { paperId: string; tag: string }) => acceptTagSuggestion(paperId, tag),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-tags'], ['tag-suggestions', suggestionPaperId])
      toast({ title: 'Accepted', description: 'Tag suggestion applied.' })
    },
    onError: () => {
      errorToast('Error')
    },
  })

  const dismissSuggestionMutation = useMutation({
    mutationFn: ({ paperId, tag }: { paperId: string; tag: string }) => dismissTagSuggestion(paperId, tag),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['tag-suggestions', suggestionPaperId])
      toast({ title: 'Dismissed', description: 'Suggestion removed.' })
    },
    onError: () => {
      errorToast('Error', 'Could not dismiss suggestion.')
    },
  })

  const likes = useMemo(() => likesQuery.data ?? [], [likesQuery.data])
  const selectedPaper = useMemo(
    () => likes.find((paper) => paper.id === (suggestionPaperId || paperSelection)),
    [likes, paperSelection, suggestionPaperId],
  )

  function loadSuggestionsForSelectedPaper() {
    if (!paperSelection) return
    setSuggestionPaperId(paperSelection)
  }

  return (
    <>
      {bulkSuggestMutation.isPending && <p role="status">Updating tags…</p>}
      {likesQuery.isError && (
        <ErrorState
          message="Paper choices could not be loaded."
          actionLabel="Retry"
          onAction={() => void likesQuery.refetch()}
        />
      )}
      <Button
        variant="outline"
        onClick={() => bulkSuggestMutation.mutate()}
        disabled={bulkSuggestMutation.isPending}
      >
        {bulkSuggestMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Cpu className="h-4 w-4" />}
        AI Generate Suggestions
      </Button>
      <Card>
        <CardContent className="space-y-3 p-4">
          <div className="flex items-center justify-between gap-2">
            <div>
              <h3 className="text-sm font-semibold text-slate-800">Smart Tag Suggestions</h3>
              <p className="text-xs text-slate-500">Generate or review AI suggestions for one paper.</p>
            </div>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                if (suggestionPaperId) {
                  void invalidateQueries(queryClient, ['tag-suggestions', suggestionPaperId])
                }
              }}
              disabled={!suggestionPaperId}
            >
              <RefreshCw className="h-3.5 w-3.5" />
              Refresh
            </Button>
          </div>

          {likes.length === 0 ? (
            <p className="text-xs text-slate-500">No library papers available yet. Add papers to Library first.</p>
          ) : (
            <div className="grid gap-2 sm:grid-cols-[1fr_auto]">
              <Select value={paperSelection} onValueChange={setPaperSelection}>
                <SelectTrigger>
                  <SelectValue placeholder="Select a paper" />
                </SelectTrigger>
                <SelectContent>
                  {likes.map((paper) => (
                    <SelectItem key={paper.id} value={paper.id}>
                      {paper.title}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button onClick={loadSuggestionsForSelectedPaper} disabled={!paperSelection}>
                <Sparkles className="h-4 w-4" />
                Load Suggestions
              </Button>
            </div>
          )}

          {suggestionPaperId && (
            <SubPanel variant="flat" padded={false}  className="p-3">
              <p className="mb-2 text-xs text-slate-600">
                Paper: <span className="font-medium text-slate-800">{selectedPaper?.title ?? suggestionPaperId}</span>
              </p>
              {suggestionsQuery.isLoading ? (
                <div className="flex items-center gap-2 text-xs text-slate-500">
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  Loading suggestions...
                </div>
              ) : suggestionsQuery.isError ? (
                <ErrorState message="Failed to load tag suggestions." />
              ) : (suggestionsQuery.data?.suggestions.length ?? 0) === 0 ? (
                <p className="text-xs text-slate-500">No suggestions for this paper yet.</p>
              ) : (
                <div className="space-y-2">
                  {suggestionsQuery.data?.suggestions.map((item) => (
                    <SubPanel variant="flat" padded={false}
                      key={`${item.paper_id}:${item.tag}`} className="flex flex-wrap items-center justify-between gap-2 px-3 py-2"
                    >
                      <div className="min-w-0">
                        <p className="text-sm font-medium text-slate-800">{item.tag}</p>
                        <p className="text-xs text-slate-500">
                          confidence {formatPercent(item.confidence, 0)} · source {item.source}
                        </p>
                      </div>
                      <div className="flex items-center gap-1">
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => acceptSuggestionMutation.mutate({ paperId: item.paper_id, tag: item.tag })}
                          disabled={acceptSuggestionMutation.isPending}
                        >
                          <Check className="h-3.5 w-3.5" />
                          Accept
                        </Button>
                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => dismissSuggestionMutation.mutate({ paperId: item.paper_id, tag: item.tag })}
                          disabled={dismissSuggestionMutation.isPending}
                        >
                          <XCircle className="h-3.5 w-3.5" />
                          Dismiss
                        </Button>
                      </div>
                    </SubPanel>
                  ))}
                </div>
              )}
            </SubPanel>
          )}
        </CardContent>
      </Card>
    </>
  )
}
