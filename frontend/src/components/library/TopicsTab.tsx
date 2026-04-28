import { useState, useCallback } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import {
  Plus,
  Trash2,
  Edit3,
  Loader2,
  ArrowUpDown,
  X,
  Layers,
  ChevronRight,
  ChevronDown,
} from 'lucide-react'
import { api, type TopicSummary, type TopicHierarchyResponse } from '@/api/client'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
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
import { ConfirmDialog } from './ConfirmDialog'

export function TopicsTab() {
  const [createOpen, setCreateOpen] = useState(false)
  const [aliasOpen, setAliasOpen] = useState(false)
  const [renameTopic, setRenameTopic] = useState<string | null>(null)
  const [groupOpen, setGroupOpen] = useState(false)
  const [deleteTopic, setDeleteTopic] = useState<string | null>(null)
  const [deleteAlias, setDeleteAlias] = useState<string | null>(null)
  const [expandedDomains, setExpandedDomains] = useState<Set<string>>(new Set())
  const [expandedFields, setExpandedFields] = useState<Set<string>>(new Set())
  const [hierarchyFilter, setHierarchyFilter] = useState<string | null>(null)

  const [newTopicName, setNewTopicName] = useState('')
  const [aliasName, setAliasName] = useState('')
  const [aliasCanonical, setAliasCanonical] = useState('')
  const [renameTo, setRenameTo] = useState('')
  const [groupSource, setGroupSource] = useState('')
  const [groupTarget, setGroupTarget] = useState('')
  const [deleteReplacement, setDeleteReplacement] = useState('')

  const queryClient = useQueryClient()
  const { toast } = useToast()

  const topicsQuery = useQuery({
    queryKey: ['library-topics'],
    queryFn: () => api.get<TopicSummary[]>('/library/topics'),
    retry: 1,
  })

  const hierarchyQuery = useQuery({
    queryKey: ['library-topics-hierarchy'],
    queryFn: () => api.get<TopicHierarchyResponse>('/library/topics/hierarchy'),
    retry: 1,
  })

  const topics = topicsQuery.data ?? []
  const canonicalNames = topics.map((t) => t.canonical)
  const hierarchy = hierarchyQuery.data?.domains ?? []

  const toggleDomain = useCallback((domain: string) => {
    setExpandedDomains(prev => {
      const next = new Set(prev)
      if (next.has(domain)) {
        next.delete(domain)
      } else {
        next.add(domain)
      }
      return next
    })
  }, [])

  const toggleField = useCallback((fieldKey: string) => {
    setExpandedFields(prev => {
      const next = new Set(prev)
      if (next.has(fieldKey)) {
        next.delete(fieldKey)
      } else {
        next.add(fieldKey)
      }
      return next
    })
  }, [])

  const selectHierarchyNode = useCallback((nodeName: string) => {
    setHierarchyFilter(nodeName)
  }, [])

  const invalidateAfterTopicMutation = useCallback(() => {
    void invalidateQueries(queryClient, ['library-topics'], ['insights'], ['graph'])
    api.post('/graphs/rebuild').catch(() => undefined)
  }, [queryClient])

  const createTopicMutation = useMutation({
    mutationFn: (name: string) => api.post<TopicSummary>('/library/topics', { name }),
    onSuccess: () => {
      invalidateAfterTopicMutation()
      setCreateOpen(false)
      setNewTopicName('')
      toast({ title: 'Created', description: 'Canonical topic created.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to create topic.')
    },
  })

  const createAliasMutation = useMutation({
    mutationFn: (body: { alias: string; canonical: string }) =>
      api.post<TopicSummary>('/library/topics/aliases', body),
    onSuccess: () => {
      invalidateAfterTopicMutation()
      setAliasOpen(false)
      setAliasName('')
      toast({ title: 'Saved', description: 'Alias mapped successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to save alias.')
    },
  })

  const renameMutation = useMutation({
    mutationFn: ({ topic, newName }: { topic: string; newName: string }) =>
      api.put<TopicSummary>(`/library/topics/${encodeURIComponent(topic)}`, { new_name: newName }),
    onSuccess: () => {
      invalidateAfterTopicMutation()
      setRenameTopic(null)
      setRenameTo('')
      toast({ title: 'Renamed', description: 'Topic renamed successfully.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to rename topic.')
    },
  })

  const groupMutation = useMutation({
    mutationFn: ({ source, target }: { source: string; target: string }) =>
      api.post<TopicSummary>('/library/topics/group', { source, target }),
    onSuccess: () => {
      invalidateAfterTopicMutation()
      setGroupOpen(false)
      setGroupSource('')
      setGroupTarget('')
      toast({ title: 'Grouped', description: 'Topics grouped under canonical alias.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to group topics.')
    },
  })

  const deleteTopicMutation = useMutation({
    mutationFn: ({ topic, replacement }: { topic: string; replacement?: string }) => {
      const qs = replacement ? `?replacement=${encodeURIComponent(replacement)}` : ''
      return api.delete(`/library/topics/${encodeURIComponent(topic)}${qs}`)
    },
    onSuccess: () => {
      invalidateAfterTopicMutation()
      setDeleteTopic(null)
      setDeleteReplacement('')
      toast({ title: 'Deleted', description: 'Topic mapping deleted.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to delete topic.')
    },
  })

  const deleteAliasMutation = useMutation({
    mutationFn: (alias: string) => api.delete(`/library/topics/aliases/${encodeURIComponent(alias)}`),
    onSuccess: () => {
      invalidateAfterTopicMutation()
      setDeleteAlias(null)
      toast({ title: 'Deleted', description: 'Alias removed.' })
    },
    onError: () => {
      errorToast('Error', 'Failed to remove alias.')
    },
  })

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between gap-3">
        <div className="space-y-1">
          <div className="flex items-center gap-2 text-sm text-slate-500">
            <span>{topics.length} canonical topic{topics.length !== 1 ? 's' : ''}</span>
            <Badge variant="outline" className="text-xs">
              {topics.reduce((sum, t) => sum + t.aliases.length, 0)} aliases
            </Badge>
          </div>
          <p className="text-xs text-slate-500">
            Topics are official source-backed concepts from OpenAlex and related scholarly metadata, with canonical names and aliases for cleanup.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            onClick={() => {
              setAliasCanonical(canonicalNames[0] ?? '')
              setAliasName('')
              setAliasOpen(true)
            }}
          >
            <Plus className="h-4 w-4" />
            Add Alias
          </Button>
          <Button
            onClick={() => {
              setNewTopicName('')
              setCreateOpen(true)
            }}
          >
            <Plus className="h-4 w-4" />
            New Topic
          </Button>
        </div>
      </div>

      {/* Topic Hierarchy */}
      {hierarchy.length > 0 && (
        <Card className="mb-4">
          <CardContent className="p-4">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-slate-700">Topic Hierarchy</h3>
              {hierarchyFilter && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setHierarchyFilter(null)}
                  className="text-xs"
                >
                  Clear filter
                </Button>
              )}
            </div>
            <div className="space-y-1">
              {hierarchy.map((domain) => {
                const isDomainExpanded = expandedDomains.has(domain.name)
                return (
                  <div key={domain.name} className="text-sm">
                    <div className="flex items-center gap-2 py-1">
                      <button
                        onClick={() => toggleDomain(domain.name)}
                        className="flex items-center gap-1 hover:text-alma-600"
                      >
                        {isDomainExpanded ? (
                          <ChevronDown className="h-4 w-4" />
                        ) : (
                          <ChevronRight className="h-4 w-4" />
                        )}
                      </button>
                      <button
                        onClick={() => selectHierarchyNode(domain.name)}
                        className={`flex-1 text-left font-medium hover:text-alma-600 ${
                          hierarchyFilter === domain.name ? 'text-alma-700' : 'text-slate-800'
                        }`}
                      >
                        {domain.name}
                      </button>
                      <Badge variant="secondary" className="text-xs">
                        {domain.paper_count}
                      </Badge>
                    </div>
                    {isDomainExpanded && domain.fields.length > 0 && (
                      <div className="ml-6 space-y-1 border-l border-slate-200 pl-3">
                        {domain.fields.map((field) => {
                          const fieldKey = `${domain.name}::${field.name}`
                          const isFieldExpanded = expandedFields.has(fieldKey)
                          return (
                            <div key={fieldKey}>
                              <div className="flex items-center gap-2 py-0.5">
                                <button
                                  onClick={() => toggleField(fieldKey)}
                                  className="flex items-center gap-1 hover:text-alma-600"
                                >
                                  {isFieldExpanded ? (
                                    <ChevronDown className="h-3.5 w-3.5" />
                                  ) : (
                                    <ChevronRight className="h-3.5 w-3.5" />
                                  )}
                                </button>
                                <button
                                  onClick={() => selectHierarchyNode(field.name)}
                                  className={`flex-1 text-left hover:text-alma-600 ${
                                    hierarchyFilter === field.name ? 'text-alma-700' : 'text-slate-700'
                                  }`}
                                >
                                  {field.name}
                                </button>
                                <Badge variant="outline" className="text-xs">
                                  {field.paper_count}
                                </Badge>
                              </div>
                              {isFieldExpanded && field.subfields.length > 0 && (
                                <div className="ml-6 space-y-0.5 border-l border-slate-100 pl-3">
                                  {field.subfields.map((subfield) => (
                                    <div key={subfield.name} className="flex items-center gap-2 py-0.5">
                                      <button
                                        onClick={() => selectHierarchyNode(subfield.name)}
                                        className={`flex-1 text-left text-xs hover:text-alma-600 ${
                                          hierarchyFilter === subfield.name ? 'text-alma-700' : 'text-slate-600'
                                        }`}
                                      >
                                        {subfield.name}
                                      </button>
                                      <Badge variant="outline" className="text-xs">
                                        {subfield.paper_count}
                                      </Badge>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          )
                        })}
                      </div>
                    )}
                  </div>
                )
              })}
            </div>
          </CardContent>
        </Card>
      )}

      {topicsQuery.isLoading ? (
        <LoadingState message="Loading topics..." />
      ) : topicsQuery.isError ? (
        <ErrorState message="Failed to load topics." />
      ) : topics.length === 0 ? (
        <div className="py-16 text-center">
          <Layers className="mx-auto h-12 w-12 text-slate-300" />
          <p className="mt-4 text-sm font-medium text-slate-500">No topics available yet</p>
          <p className="mt-1 text-xs text-slate-400">
            Import/enrich papers first, or create canonical topics manually.
          </p>
        </div>
      ) : (
        <div className="space-y-3">
          {hierarchyFilter && (
            <div className="mb-2 text-sm text-slate-600">
              Showing topics related to: <span className="font-medium text-alma-700">{hierarchyFilter}</span>
            </div>
          )}
          {topics
            .filter((topic) => {
              if (!hierarchyFilter) return true
              // Show all topics if no filter, otherwise show only topics whose canonical name
              // or aliases contain the filter string (case-insensitive partial match)
              const lowerFilter = hierarchyFilter.toLowerCase()
              return (
                topic.canonical.toLowerCase().includes(lowerFilter) ||
                topic.aliases.some((alias) => alias.toLowerCase().includes(lowerFilter))
              )
            })
            .map((topic) => (
            <Card key={topic.canonical} className="transition-shadow hover:shadow-md">
              <CardContent className="p-4">
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0 flex-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <h3 className="font-medium text-alma-800">{topic.canonical}</h3>
                      <Badge variant="secondary">{topic.paper_count} papers</Badge>
                    </div>
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {topic.aliases.length === 0 ? (
                        <span className="text-xs text-slate-400">No aliases</span>
                      ) : (
                        topic.aliases.map((alias) => (
                          <Badge key={alias} variant="outline" className="text-xs pr-1">
                            <span>{alias}</span>
                            <button
                              type="button"
                              className="ml-1 rounded-sm text-slate-500 hover:text-red-600"
                              onClick={() => setDeleteAlias(alias)}
                              title="Delete alias"
                            >
                              <X className="h-3 w-3" />
                            </button>
                          </Badge>
                        ))
                      )}
                    </div>
                  </div>
                  <div className="flex shrink-0 items-center gap-1">
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => {
                        setAliasCanonical(topic.canonical)
                        setAliasName('')
                        setAliasOpen(true)
                      }}
                      title="Add alias"
                    >
                      <Plus className="h-4 w-4 text-alma-600" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => {
                        setRenameTopic(topic.canonical)
                        setRenameTo(topic.canonical)
                      }}
                      title="Rename canonical topic"
                    >
                      <Edit3 className="h-4 w-4 text-slate-500" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => {
                        setGroupSource(topic.canonical)
                        setGroupTarget(canonicalNames.find((n) => n !== topic.canonical) ?? '')
                        setGroupOpen(true)
                      }}
                      title="Group under another canonical"
                    >
                      <ArrowUpDown className="h-4 w-4 text-slate-500" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon"
                      onClick={() => {
                        setDeleteTopic(topic.canonical)
                        setDeleteReplacement(canonicalNames.find((n) => n !== topic.canonical) ?? '')
                      }}
                      title="Delete canonical topic"
                    >
                      <Trash2 className="h-4 w-4 text-red-500" />
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          ))}
        </div>
      )}

      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Canonical Topic</DialogTitle>
            <DialogDescription>Add a new canonical topic name.</DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-2">
            <label className="text-sm font-medium text-slate-700">Canonical name</label>
            <Input
              value={newTopicName}
              onChange={(e) => setNewTopicName(e.target.value)}
              placeholder="e.g., Brain-Computer Interfaces"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setCreateOpen(false)}>Cancel</Button>
            <Button
              onClick={() => createTopicMutation.mutate(newTopicName.trim())}
              disabled={!newTopicName.trim() || createTopicMutation.isPending}
            >
              {createTopicMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={aliasOpen} onOpenChange={setAliasOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Topic Alias</DialogTitle>
            <DialogDescription>Map an alias to a canonical topic.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div className="space-y-1.5">
              <label className="text-sm font-medium text-slate-700">Alias</label>
              <Input
                value={aliasName}
                onChange={(e) => setAliasName(e.target.value)}
                placeholder="e.g., BCI"
              />
            </div>
            <div className="space-y-1.5">
              <label className="text-sm font-medium text-slate-700">Canonical topic</label>
              <Select value={aliasCanonical} onValueChange={setAliasCanonical}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {canonicalNames.map((name) => (
                    <SelectItem key={name} value={name}>{name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setAliasOpen(false)}>Cancel</Button>
            <Button
              onClick={() => createAliasMutation.mutate({ alias: aliasName.trim(), canonical: aliasCanonical })}
              disabled={!aliasName.trim() || !aliasCanonical || createAliasMutation.isPending}
            >
              {createAliasMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Save Alias
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={!!renameTopic}
        onOpenChange={(open) => {
          if (!open) {
            setRenameTopic(null)
            setRenameTo('')
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Rename Canonical Topic</DialogTitle>
            <DialogDescription>Rename "{renameTopic}" and keep it as an alias.</DialogDescription>
          </DialogHeader>
          <div className="space-y-2 py-2">
            <label className="text-sm font-medium text-slate-700">New name</label>
            <Input
              value={renameTo}
              onChange={(e) => setRenameTo(e.target.value)}
              placeholder="New canonical topic name"
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setRenameTopic(null)}>Cancel</Button>
            <Button
              onClick={() => renameTopic && renameMutation.mutate({ topic: renameTopic, newName: renameTo.trim() })}
              disabled={!renameTopic || !renameTo.trim() || renameMutation.isPending}
            >
              {renameMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Rename
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={groupOpen}
        onOpenChange={(open) => {
          if (!open) {
            setGroupOpen(false)
            setGroupSource('')
            setGroupTarget('')
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Group Topics</DialogTitle>
            <DialogDescription>Map one canonical topic under another.</DialogDescription>
          </DialogHeader>
          <div className="space-y-3 py-2">
            <div className="space-y-1.5">
              <label className="text-sm font-medium text-slate-700">Source topic</label>
              <Select value={groupSource} onValueChange={setGroupSource}>
                <SelectTrigger>
                  <SelectValue placeholder="Select source" />
                </SelectTrigger>
                <SelectContent>
                  {canonicalNames.map((name) => (
                    <SelectItem key={name} value={name}>{name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1.5">
              <label className="text-sm font-medium text-slate-700">Target canonical</label>
              <Select value={groupTarget} onValueChange={setGroupTarget}>
                <SelectTrigger>
                  <SelectValue placeholder="Select target" />
                </SelectTrigger>
                <SelectContent>
                  {canonicalNames.filter((name) => name !== groupSource).map((name) => (
                    <SelectItem key={name} value={name}>{name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setGroupOpen(false)}>Cancel</Button>
            <Button
              onClick={() => groupMutation.mutate({ source: groupSource, target: groupTarget })}
              disabled={!groupSource || !groupTarget || groupSource === groupTarget || groupMutation.isPending}
            >
              {groupMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Group
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={!!deleteTopic}
        onOpenChange={(open) => {
          if (!open) {
            setDeleteTopic(null)
            setDeleteReplacement('')
          }
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete Canonical Topic</DialogTitle>
            <DialogDescription>
              Delete mapping for "{deleteTopic}". Optionally move aliases to another canonical first.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-1.5 py-2">
            <label className="text-sm font-medium text-slate-700">Replacement (optional)</label>
            <Select
              value={deleteReplacement || 'none'}
              onValueChange={(value) => setDeleteReplacement(value === 'none' ? '' : value)}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="none">No replacement</SelectItem>
                {canonicalNames.filter((name) => name !== deleteTopic).map((name) => (
                  <SelectItem key={name} value={name}>{name}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeleteTopic(null)}>Cancel</Button>
            <Button
              variant="destructive"
              onClick={() => deleteTopic && deleteTopicMutation.mutate({ topic: deleteTopic, replacement: deleteReplacement || undefined })}
              disabled={!deleteTopic || deleteTopicMutation.isPending}
            >
              {deleteTopicMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <ConfirmDialog
        open={!!deleteAlias}
        onOpenChange={(open) => {
          if (!open) setDeleteAlias(null)
        }}
        title="Delete Alias"
        description={`Delete alias "${deleteAlias ?? ''}"?`}
        onConfirm={() => deleteAlias && deleteAliasMutation.mutate(deleteAlias)}
        isPending={deleteAliasMutation.isPending}
      />
    </div>
  )
}
