import { useMemo, useState } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Tags,
  Plus,
  Trash2,
  Loader2,
  Star,
  BookOpen,
  Info,
  AlertCircle,
  Cpu,
  Sparkles,
  GitMerge,
  Check,
  XCircle,
  RefreshCw,
} from 'lucide-react'
import {
  api,
  acceptTagSuggestion,
  bulkGenerateTagSuggestions,
  dismissTagSuggestion,
  getTagMergeSuggestions,
  getTagSuggestions,
  mergeTags,
  type Publication,
  type Tag,
} from '@/api/client'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { LoadingState } from '@/components/ui/LoadingState'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { useToast, errorToast} from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'
import { PRESET_COLORS } from './types'
import { ConfirmDialog } from './ConfirmDialog'
import { ColorPicker } from './ColorPicker'

export function TagsTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [deleteId, setDeleteId] = useState<string | null>(null)
  const [assignTagId, setAssignTagId] = useState<string | null>(null)
  const [assignPubKey, setAssignPubKey] = useState('')
  const [paperSelection, setPaperSelection] = useState('')
  const [suggestionPaperId, setSuggestionPaperId] = useState<string | null>(null)

  // Form state
  const [formName, setFormName] = useState('')
  const [formColor, setFormColor] = useState(PRESET_COLORS[2])

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const tagsQuery = useQuery({
    queryKey: ['library-tags'],
    queryFn: () => api.get<Tag[]>('/library/tags'),
    retry: 1,
  })

  const likesQuery = useQuery({
    queryKey: ['library-saved'],
    queryFn: () => api.get<Publication[]>('/library/saved?limit=300'),
    retry: 1,
  })

  const suggestionsQuery = useQuery({
    queryKey: ['tag-suggestions', suggestionPaperId],
    queryFn: () => getTagSuggestions(suggestionPaperId as string),
    enabled: Boolean(suggestionPaperId),
    retry: 1,
  })

  const mergeSuggestionsQuery = useQuery({
    queryKey: ['tag-merge-suggestions'],
    queryFn: () => getTagMergeSuggestions(12, 0.8),
    retry: 1,
  })

  const createMutation = useMutation({
    mutationFn: (body: { name: string; color?: string }) => api.post<Tag>('/library/tags', body),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-tags'], ['tag-merge-suggestions'])
      setCreateOpen(false)
      resetForm()
      toast({ title: 'Created', description: 'Tag created successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to create tag. The name may already exist.')
    },
  })

  const deleteMutation = useMutation({
    mutationFn: (id: string) => api.delete(`/library/tags/${id}`),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-tags'], ['tag-merge-suggestions'])
      setDeleteId(null)
      toast({ title: 'Deleted', description: 'Tag deleted.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to delete tag.')
    },
  })

  const assignMutation = useMutation({
    mutationFn: (body: { paper_id: string; tag_id: string }) => api.post('/library/tags/assign', body),
    onSuccess: () => {
      setAssignTagId(null)
      setAssignPubKey('')
      void invalidateQueries(queryClient, ['library-tags'])
      toast({ title: 'Assigned', description: 'Tag assigned to publication.' })
    },
    onError: (error) => {
      errorToast('Error')
    },
  })

  const bulkSuggestMutation = useMutation({
    mutationFn: () => bulkGenerateTagSuggestions(),
    onSuccess: (data) => {
      void invalidateQueries(queryClient, ['activity-operations'])
      toast({
        title: data.status === 'already_running' ? 'Already running' : 'Smart tagging started',
        description: data.job_id ? `Job ${data.job_id} is now in Activity.` : (data.message || 'Started'),
      })
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
    onError: (error) => {
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

  const mergeMutation = useMutation({
    mutationFn: ({ sourceTagId, targetTagId }: { sourceTagId: string; targetTagId: string }) =>
      mergeTags(sourceTagId, targetTagId),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['library-tags'], ['tag-merge-suggestions'])
      toast({ title: 'Merged', description: 'Tags merged successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to merge tags.')
    },
  })

  const tags = tagsQuery.data ?? []
  const likes = likesQuery.data ?? []
  const selectedPaper = useMemo(
    () => likes.find((paper) => paper.id === (suggestionPaperId || paperSelection)),
    [likes, paperSelection, suggestionPaperId],
  )

  function resetForm() {
    setFormName('')
    setFormColor(PRESET_COLORS[2])
  }

  function handleCreate() {
    if (!formName.trim()) return
    createMutation.mutate({
      name: formName.trim(),
      color: formColor,
    })
  }

  function handleAssign() {
    if (!assignTagId || !assignPubKey) return
    assignMutation.mutate({
      paper_id: assignPubKey,
      tag_id: assignTagId,
    })
  }

  function loadSuggestionsForSelectedPaper() {
    if (!paperSelection) return
    setSuggestionPaperId(paperSelection)
  }

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div className="space-y-1">
          <div className="flex items-center gap-2">
            <p className="text-sm text-slate-500">
              {tags.length} tag{tags.length !== 1 ? 's' : ''}
            </p>
            <div className="flex items-center gap-1 rounded-md bg-alma-50 px-2 py-1">
              <Info className="h-3 w-3 text-alma-500" />
              <span className="text-xs text-alma-700">Tags boost recommendation scores by 2x</span>
            </div>
          </div>
          <p className="text-xs text-slate-500">
            User-defined or AI-defined keywords for search and signals. They do not need to be official topics, and each paper may have at most 5 tags.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            onClick={() => bulkSuggestMutation.mutate()}
            disabled={bulkSuggestMutation.isPending}
          >
            {bulkSuggestMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Cpu className="h-4 w-4" />}
            AI Generate Suggestions
          </Button>
          <Button onClick={() => { resetForm(); setCreateOpen(true) }}>
            <Plus className="h-4 w-4" />
            New Tag
          </Button>
        </div>
      </div>

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
            <div className="rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3">
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
                    <div
                      key={`${item.paper_id}:${item.tag}`}
                      className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-[var(--color-border)] bg-alma-chrome px-3 py-2"
                    >
                      <div className="min-w-0">
                        <p className="text-sm font-medium text-slate-800">{item.tag}</p>
                        <p className="text-xs text-slate-500">
                          confidence {(item.confidence * 100).toFixed(0)}% · source {item.source}
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
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}
        </CardContent>
      </Card>

      <Card>
        <CardContent className="space-y-3 p-4">
          <div>
            <h3 className="text-sm font-semibold text-slate-800">Merge Suggestions</h3>
            <p className="text-xs text-slate-500">Consolidate duplicate tags.</p>
          </div>
          {mergeSuggestionsQuery.isLoading ? (
            <div className="flex items-center gap-2 text-xs text-slate-500">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Loading merge suggestions...
            </div>
          ) : (mergeSuggestionsQuery.data ?? []).length === 0 ? (
            <p className="text-xs text-slate-500">No merge candidates detected.</p>
          ) : (
            <div className="space-y-2">
              {mergeSuggestionsQuery.data?.map((item) => (
                <div
                  key={`${item.source_tag_id}:${item.target_tag_id}`}
                  className="flex flex-wrap items-center justify-between gap-2 rounded-md border border-[var(--color-border)] bg-alma-chrome px-3 py-2"
                >
                  <div className="text-sm text-slate-700">
                    <span className="font-medium">{item.source_tag}</span>
                    <span className="mx-2 text-slate-400">→</span>
                    <span className="font-medium">{item.target_tag}</span>
                    <span className="ml-2 text-xs text-slate-500">
                      {(item.confidence * 100).toFixed(0)}% · {item.reason}
                    </span>
                  </div>
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={() => mergeMutation.mutate({ sourceTagId: item.source_tag_id, targetTagId: item.target_tag_id })}
                    disabled={mergeMutation.isPending}
                  >
                    <GitMerge className="h-3.5 w-3.5" />
                    Merge
                  </Button>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Content */}
      {tagsQuery.isLoading ? (
        <LoadingState message="Loading tags..." />
      ) : tagsQuery.isError ? (
        <ErrorState message="Failed to load tags." />
      ) : tags.length === 0 ? (
        <div className="py-16 text-center">
          <Tags className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm font-medium text-slate-500">No tags yet</p>
          <p className="mt-1 text-xs text-slate-400">
            Create tags to categorize papers and improve recommendations.
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
          {tags.map((tag) => (
            <Card key={tag.id} className="transition-shadow hover:shadow-md">
              <CardContent className="p-4">
                <div className="flex items-center justify-between gap-3">
                  <div className="min-w-0 flex items-center gap-2.5">
                    <div
                      className="h-3.5 w-3.5 shrink-0 rounded-full shadow-sm"
                      style={{ backgroundColor: tag.color }}
                    />
                    <span className="truncate font-medium text-alma-800">{tag.name}</span>
                  </div>
                  <div className="flex shrink-0 items-center gap-1">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => {
                        setAssignTagId(tag.id)
                        setAssignPubKey('')
                      }}
                      title="Assign to publication"
                    >
                      <Plus className="h-4 w-4 text-alma-500" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => setDeleteId(tag.id)}
                      title="Delete tag"
                    >
                      <Trash2 className="h-4 w-4 text-red-400" />
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      {/* Create Tag Dialog */}
      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Tag</DialogTitle>
            <DialogDescription>Create a new tag to categorize your publications.</DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-700">Name</label>
              <Input
                placeholder="e.g., Machine Learning, NLP, Must Read"
                value={formName}
                onChange={(e) => setFormName(e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-medium text-slate-700">Color</label>
              <ColorPicker value={formColor} onChange={setFormColor} />
            </div>
          </div>
          {createMutation.isError && (
            <div className="flex items-center gap-2 rounded-lg border border-red-200 bg-red-50 p-3">
              <AlertCircle className="h-4 w-4 text-red-500" />
              <span className="text-sm text-red-700">Failed to create tag.</span>
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreate} disabled={!formName.trim() || createMutation.isPending}>
              {createMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create Tag
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Assign Tag Dialog */}
      <Dialog
        open={!!assignTagId}
        onOpenChange={(open) => {
          if (!open) {
            setAssignTagId(null)
            setAssignPubKey('')
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Assign Tag to Publication</DialogTitle>
            <DialogDescription>Select a publication from your library to tag.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-4">
            {likes.length === 0 ? (
              <p className="py-4 text-center text-sm text-slate-500">No library publications found.</p>
            ) : (
              <div className="max-h-64 space-y-2 overflow-y-auto">
                {likes.map((like) => (
                  <button
                    key={like.id}
                    onClick={() => setAssignPubKey(like.id)}
                    className={`flex w-full items-center gap-3 rounded-lg border px-3 py-2.5 text-left transition-colors ${
                      assignPubKey === like.id
                        ? 'border-alma-500 bg-alma-50'
                        : 'border-slate-200 hover:border-[var(--color-border)] hover:bg-parchment-50'
                    }`}
                  >
                    <BookOpen className="h-4 w-4 shrink-0 text-slate-400" />
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-medium text-slate-800">{like.title}</p>
                      <p className="text-xs text-slate-500">{like.authors}</p>
                    </div>
                    <div className="flex items-center gap-0.5">
                      {Array.from({ length: like.rating ?? 0 }).map((_, i) => (
                        <Star key={i} className="h-3 w-3 fill-amber-400 text-amber-400" />
                      ))}
                    </div>
                  </button>
                ))}
              </div>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setAssignTagId(null)
                setAssignPubKey('')
              }}
            >
              Cancel
            </Button>
            <Button onClick={handleAssign} disabled={!assignPubKey || assignMutation.isPending}>
              {assignMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Assign Tag
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation */}
      <ConfirmDialog
        open={!!deleteId}
        onOpenChange={(open) => {
          if (!open) setDeleteId(null)
        }}
        title="Delete Tag"
        description="Are you sure you want to delete this tag? All publication assignments will be removed. This action cannot be undone."
        onConfirm={() => deleteId && deleteMutation.mutate(deleteId)}
        isPending={deleteMutation.isPending}
      />
    </div>
  )
}
