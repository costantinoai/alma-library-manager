import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { BookOpen, GripVertical, Plus, RefreshCw, Save, Search, Tag, Trash2, UserRound, X } from 'lucide-react'
import {
  DndContext,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
} from '@dnd-kit/core'
import {
  SortableContext,
  arrayMove,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable'
import { CSS } from '@dnd-kit/utilities'

import {
  createFeedMonitor,
  deleteFeedMonitor,
  listFeedMonitors,
  refreshFeedMonitor,
  reorderFeedMonitors,
  updateFeedMonitor,
  type FeedMonitor,
  type VenueSearchResult,
} from '@/api/client'
import { VenueAutocomplete } from '@/components/shared/VenueAutocomplete'
import { AsyncButton } from '@/components/settings/primitives'
import { DisclosurePanel } from '@/components/ui/disclosure-panel'
import { PageSection } from '@/components/ui/page-section'
import { Label } from '@/components/ui/label'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Badge } from '@/components/ui/badge'
import { Checkbox } from '@/components/ui/checkbox'
import { EmptyState } from '@/components/ui/empty-state'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { StatusBadge } from '@/components/ui/status-badge'
import { monitorHealthTone } from '@/components/ui/status-badge-tones'
import { useToast, errorToast } from '@/hooks/useToast'
import { describeJobLaunch } from '@/lib/activity'
import { navigateTo } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { cn, formatMonitorTypeLabel, formatTimestamp } from '@/lib/utils'

type FeedMonitorCreateType = 'query' | 'topic' | 'venue'

function monitorQuery(monitor: FeedMonitor): string {
  const raw = monitor.config?.query
  return typeof raw === 'string' ? raw : monitor.monitor_key
}

function createMonitorPlaceholder(type: FeedMonitorCreateType): string {
  if (type === 'query') {
    return 'e.g. (manifold OR topology) AND representations NOT images'
  }
  return 'e.g. protein design'
}

function createMonitorHelp(type: FeedMonitorCreateType): string {
  if (type === 'query') {
    return 'Keyword monitors use strict boolean logic over title and abstract only.'
  }
  if (type === 'venue') {
    return 'Journal monitors match every new paper published in a journal (by exact OpenAlex source), optionally filtered by keywords. Papers land in Feed → Journals.'
  }
  return 'Topic monitors cast a broader retrieval net, then Feed applies stricter matching before insertion.'
}

function monitorOwnerPage(monitor: FeedMonitor): 'authors' | 'feed' {
  return monitor.monitor_type === 'author' ? 'authors' : 'feed'
}

function MonitorRow({ monitor }: { monitor: FeedMonitor }) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const editableDefinition = monitor.monitor_type !== 'author'
  const ownerPage = monitorOwnerPage(monitor)
  const [label, setLabel] = useState(monitor.label)
  const [query, setQuery] = useState(monitorQuery(monitor))
  const [enabled, setEnabled] = useState(Boolean(monitor.enabled))
  const savedQuery = useMemo(() => monitorQuery(monitor), [monitor])

  useEffect(() => {
    setLabel(monitor.label)
    setQuery(savedQuery)
    setEnabled(Boolean(monitor.enabled))
  }, [monitor.label, monitor.enabled, savedQuery])

  const dirty =
    enabled !== Boolean(monitor.enabled) ||
    (editableDefinition &&
      (label.trim() !== monitor.label.trim() || query.trim() !== monitorQuery(monitor).trim()))

  const saveMutation = useMutation({
    mutationFn: () => {
      const payload: { enabled?: boolean; label?: string; query?: string } = {}
      if (enabled !== Boolean(monitor.enabled)) payload.enabled = enabled
      if (editableDefinition) {
        if (label.trim() !== monitor.label.trim()) payload.label = label.trim()
        if (query.trim() !== monitorQuery(monitor).trim()) payload.query = query.trim()
      }
      return updateFeedMonitor(monitor.id, payload)
    },
    onSuccess: async (updated) => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({
        title: 'Monitor updated',
        description: updated.enabled
          ? 'Feed monitor is active.'
          : 'Feed monitor is paused and will be skipped on refresh.',
      })
    },
    onError: () => errorToast('Could not update monitor'),
  })

  const refreshMutation = useMutation({
    mutationFn: () => refreshFeedMonitor(monitor.id),
    onSuccess: async (result) => {
      await invalidateQueries(
        queryClient,
        ['feed-monitors'],
        ['feed-inbox'],
        ['activity-operations'],
      )
      const status = String(result?.status ?? result?.operation?.status ?? '')
      // A small monitor can refresh inline and come back `completed`; only a
      // queued run is a launch.
      toast(
        status === 'completed' || status === 'noop'
          ? { title: 'Monitor refreshed', description: result?.message || 'Recent matching papers were checked.' }
          : describeJobLaunch({ ...result, status }, 'Monitor refresh', {
              startedDetail: 'Feed data will update automatically when it finishes.',
            }),
      )
    },
    onError: () => errorToast('Could not refresh monitor'),
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteFeedMonitor(monitor.id),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({ title: 'Monitor removed', description: 'The saved Feed rule was deleted.' })
    },
    onError: () => errorToast('Could not delete monitor'),
  })

  const busy = saveMutation.isPending || refreshMutation.isPending || deleteMutation.isPending
  const result = monitor.last_result ?? {}
  const papersFound = typeof result.papers_found === 'number' ? result.papers_found : null
  const itemsCreated = typeof result.items_created === 'number' ? result.items_created : null
  const monitoredQuery = monitorQuery(monitor)

  return (
    <div data-monitor-id={monitor.id}>
    <DisclosurePanel
      title={monitor.label}
      description={editableDefinition && monitoredQuery !== monitor.label ? monitoredQuery : undefined}
      meta={<span className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
        <StatusBadge tone={monitorHealthTone(monitor.health)} size="sm">{monitor.health}</StatusBadge>
        <span>{enabled ? 'Active' : 'Paused'}</span>
        {dirty && <span className="text-warning-700">Unsaved changes</span>}
        {busy && <span role="status">Updating…</span>}
        {(monitor.last_error || monitor.health_reason) && <span className="text-warning-700">{monitor.last_error || monitor.health_reason}</span>}
        {(saveMutation.isError || refreshMutation.isError || deleteMutation.isError) && <span role="alert" className="text-critical-700">Update failed. Open to retry.</span>}
      </span>}
    >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0 flex-1 space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline">{formatMonitorTypeLabel(monitor.monitor_type)} Monitor</Badge>
            <StatusBadge tone={monitorHealthTone(monitor.health)}>{monitor.health}</StatusBadge>
            <label className="inline-flex items-center gap-2 rounded-full border border-[var(--color-border)] px-2.5 py-1 text-[11px] font-medium text-slate-700">
              <Checkbox
                checked={enabled}
                disabled={busy}
                onCheckedChange={(checked) => setEnabled(checked === true)}
              />
              {enabled ? 'active' : 'paused'}
            </label>
          </div>

          {editableDefinition ? (
            <div className="grid gap-3">
              <Input
                value={label}
                onChange={(event) => setLabel(event.target.value)}
                aria-label="Monitor name"
                placeholder="Display label"
                disabled={busy}
              />
              <Textarea
          className="min-h-[72px]"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                aria-label="Monitor rule"
                rows={monitor.monitor_type === 'query' ? 3 : 2}
                disabled={busy}
                placeholder={
                  monitor.monitor_type === 'query'
                    ? 'Examples: manifold AND representations\n(protein OR antibody) AND design NOT vaccine'
                    : 'Describe the topic to monitor'
                }
              />
            </div>
          ) : (
            <div className="rounded-md border border-[var(--color-border)] bg-surface-2 px-3 py-2 text-sm text-slate-700">
              <div className="flex flex-wrap items-center gap-2 font-medium text-alma-800">
                <UserRound className="h-4 w-4 text-slate-500" />
                <span>{monitor.author_name || monitor.label}</span>
              </div>
              <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-slate-500">
                {monitor.openalex_id && <span>OpenAlex: {monitor.openalex_id}</span>}
                {monitor.scholar_id && <span>Scholar: {monitor.scholar_id}</span>}
                {!monitor.openalex_id && (
                  <span>
                    Author monitor stays visible here so it can be paused even before identifier
                    repair is complete.
                  </span>
                )}
              </div>
            </div>
          )}
        </div>

        <div className="flex flex-wrap gap-2">
          <AsyncButton
            type="button"
            size="sm"
            variant="outline"
            icon={<RefreshCw className="h-3.5 w-3.5" />}
            pending={refreshMutation.isPending}
            disabled={busy && !refreshMutation.isPending}
            onClick={() => refreshMutation.mutate()}
          >
            Refresh
          </AsyncButton>
          <AsyncButton
            type="button"
            size="sm"
            variant="outline"
            onClick={() => ownerPage === 'authors' ? navigateTo('authors') : navigateTo('feed', { monitor: monitor.id })}
          >
            {ownerPage === 'authors' ? 'Manage authors' : 'View matches'}
          </AsyncButton>
          <AsyncButton
            type="button"
            size="sm"
            variant="outline"
            icon={<Save className="h-3.5 w-3.5" />}
            pending={saveMutation.isPending}
            disabled={busy || !dirty || (editableDefinition && !query.trim())}
            onClick={() => saveMutation.mutate()}
          >
            Save
          </AsyncButton>
          {editableDefinition && (
            <AsyncButton
              type="button"
              size="sm"
              variant="outline"
              icon={<Trash2 className="h-3.5 w-3.5" />}
              pending={deleteMutation.isPending}
              disabled={busy && !deleteMutation.isPending}
              onClick={() => deleteMutation.mutate()}
            >
              Delete
            </AsyncButton>
          )}
        </div>
      </div>

      <div className="mt-3 flex flex-wrap gap-x-4 gap-y-2 text-xs text-slate-500">
        {editableDefinition && monitoredQuery && <span>Rule: {monitoredQuery}</span>}
        {monitor.updated_at && <span>Updated: {formatTimestamp(monitor.updated_at)}</span>}
        {monitor.last_checked_at && <span>Checked: {formatTimestamp(monitor.last_checked_at)}</span>}
        {monitor.last_success_at && (
          <span>Last success: {formatTimestamp(monitor.last_success_at)}</span>
        )}
        {papersFound != null && <span>Papers: {papersFound}</span>}
        {itemsCreated != null && <span>New items: {itemsCreated}</span>}
      </div>

      {(monitor.last_error || monitor.health_reason) && (
        <p className="mt-2 text-xs text-warning-700">{monitor.last_error || monitor.health_reason}</p>
      )}
    </DisclosurePanel>
    </div>
  )
}

/** A followed-journal row: enable/pause, edit the keyword filter, refresh,
 * unfollow. A legacy name-only venue (pre source-id) shows a re-link prompt.
 * Draggable (grip handle) to reorder — the order drives the Feed → Journals tab. */
function VenueMonitorRow({ monitor, draggable }: { monitor: FeedMonitor; draggable: boolean }) {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: monitor.id,
    disabled: !draggable,
  })
  const sortableStyle = {
    transform: CSS.Transform.toString(transform),
    transition,
    zIndex: isDragging ? 10 : undefined,
  }
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const config = (monitor.config ?? {}) as {
    query?: string
    source_id?: string
    filter_keywords?: string[]
    needs_resolution?: boolean
  }
  const needsResolution = Boolean(config.needs_resolution) || !config.source_id
  const savedKeywords = (config.filter_keywords ?? []).join(', ')
  const [enabled, setEnabled] = useState(Boolean(monitor.enabled))
  const [keywords, setKeywords] = useState(savedKeywords)
  const [relinkOpen, setRelinkOpen] = useState(false)

  useEffect(() => {
    setEnabled(Boolean(monitor.enabled))
    setKeywords(((monitor.config as { filter_keywords?: string[] } | null)?.filter_keywords ?? []).join(', '))
  }, [monitor.enabled, monitor.config])

  const dirty = enabled !== Boolean(monitor.enabled) || keywords.trim() !== savedKeywords.trim()

  const saveMutation = useMutation({
    mutationFn: () => {
      const body: { enabled?: boolean; config?: Record<string, unknown> } = {}
      if (enabled !== Boolean(monitor.enabled)) body.enabled = enabled
      if (keywords.trim() !== savedKeywords.trim()) {
        body.config = { filter_keywords: keywords.split(',').map((k) => k.trim()).filter(Boolean) }
      }
      return updateFeedMonitor(monitor.id, body)
    },
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({ title: 'Journal updated', description: 'The filter is applied on the next refresh.' })
    },
    onError: () => errorToast('Could not update journal'),
  })

  const relinkMutation = useMutation({
    mutationFn: (venue: VenueSearchResult) =>
      updateFeedMonitor(monitor.id, {
        enabled: true,
        config: { query: venue.display_name, source_id: venue.source_id },
      }),
    onSuccess: async () => {
      setRelinkOpen(false)
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({ title: 'Journal re-linked', description: 'Matching resumes on the next refresh.' })
    },
    onError: () => errorToast('Could not re-link journal'),
  })

  const refreshMutation = useMutation({
    mutationFn: () => refreshFeedMonitor(monitor.id),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'], ['activity-operations'])
      toast({ title: 'Journal refresh queued', description: 'Track it in Activity.' })
    },
    onError: () => errorToast('Could not refresh journal'),
  })

  const deleteMutation = useMutation({
    mutationFn: () => deleteFeedMonitor(monitor.id),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({ title: 'Journal unfollowed', description: 'It no longer collects papers.' })
    },
    onError: () => errorToast('Could not unfollow journal'),
  })

  const busy =
    saveMutation.isPending || refreshMutation.isPending || deleteMutation.isPending || relinkMutation.isPending

  return (
    <div
      ref={setNodeRef}
      style={sortableStyle}
      className={cn(
        'flex items-start gap-2',
        isDragging && 'opacity-80 shadow-paper-md',
      )}
    >
          {draggable && (
            <button
              type="button"
              className="mt-0.5 shrink-0 cursor-grab text-slate-300 hover:text-slate-500 active:cursor-grabbing"
              aria-label={`Reorder ${config.query || monitor.label}`}
              {...attributes}
              {...listeners}
            >
              <GripVertical className="h-4 w-4" />
            </button>
          )}
      <DisclosurePanel className="min-w-0 flex-1" title={String(config.query || monitor.label)}
        description={keywords ? `Keywords: ${keywords}` : 'All new papers in this journal'}
        meta={<span className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
          <StatusBadge tone={monitorHealthTone(monitor.health)} size="sm">{needsResolution ? 'Needs re-linking' : monitor.health}</StatusBadge>
          <span>{enabled ? 'Active' : 'Paused'}</span>
          {dirty && <span className="text-warning-700">Unsaved changes</span>}
          {busy && <span role="status">Updating…</span>}
          {(saveMutation.isError || refreshMutation.isError || deleteMutation.isError || relinkMutation.isError) && <span role="alert">Update failed. Open to retry.</span>}
        </span>}
      >
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 flex-1 items-start gap-2">
          <div className="min-w-0 flex-1 space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <Badge variant="outline" className="border-accent-edge bg-accent-soft text-alma-folio">
              <BookOpen className="mr-1 h-3 w-3" /> Journal
            </Badge>
            {needsResolution ? (
              <StatusBadge tone="warning">needs re-linking</StatusBadge>
            ) : (
              <StatusBadge tone={monitorHealthTone(monitor.health)}>{monitor.health}</StatusBadge>
            )}
            {!needsResolution && (
              <label className="inline-flex items-center gap-2 rounded-full border border-[var(--color-border)] px-2.5 py-1 text-[11px] font-medium text-slate-700">
                <Checkbox
                  checked={enabled}
                  disabled={busy}
                  onCheckedChange={(checked) => setEnabled(checked === true)}
                />
                {enabled ? 'active' : 'paused'}
              </label>
            )}
          </div>
          <div className="flex flex-wrap items-center gap-2 text-sm font-medium text-alma-800">
            <span className="truncate">{config.query || monitor.label}</span>
            {config.source_id && (
              <span className="text-xs font-normal tabular-nums text-slate-400">{config.source_id}</span>
            )}
          </div>
          {needsResolution ? (
            <div className="space-y-2">
              <p className="text-xs text-warning-700">
                This journal predates source-id matching. Re-link it to an OpenAlex source to re-enable
                matching.
              </p>
              {relinkOpen ? (
                <VenueAutocomplete
                  onSelect={(venue) => relinkMutation.mutate(venue)}
                  disabled={busy}
                  placeholder={`Find “${config.query || monitor.label}”…`}
                />
              ) : (
                <AsyncButton type="button" size="sm" variant="outline" onClick={() => setRelinkOpen(true)}>
                  Re-link
                </AsyncButton>
              )}
            </div>
          ) : (
            <Input
              value={keywords}
              onChange={(event) => setKeywords(event.target.value)}
              placeholder="Keyword filter, comma-separated — empty follows the whole journal"
              disabled={busy}
              className="h-8 text-xs"
            />
          )}
          </div>
        </div>

        <div className="flex flex-wrap gap-2">
          {!needsResolution && (
            <AsyncButton
              type="button"
              size="sm"
              variant="outline"
              icon={<RefreshCw className="h-3.5 w-3.5" />}
              pending={refreshMutation.isPending}
              disabled={busy && !refreshMutation.isPending}
              onClick={() => refreshMutation.mutate()}
            >
              Refresh
            </AsyncButton>
          )}
          {!needsResolution && (
            <AsyncButton
              type="button"
              size="sm"
              variant="outline"
              icon={<Save className="h-3.5 w-3.5" />}
              pending={saveMutation.isPending}
              disabled={busy || !dirty}
              onClick={() => saveMutation.mutate()}
            >
              Save
            </AsyncButton>
          )}
          <AsyncButton
            type="button"
            size="sm"
            variant="outline"
            icon={<Trash2 className="h-3.5 w-3.5" />}
            pending={deleteMutation.isPending}
            disabled={busy && !deleteMutation.isPending}
            onClick={() => deleteMutation.mutate()}
          >
            Unfollow
          </AsyncButton>
        </div>
      </div>
      </DisclosurePanel>
    </div>
  )
}

export function FeedMonitorTermsCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [newType, setNewType] = useState<FeedMonitorCreateType>('query')
  const [newLabel, setNewLabel] = useState('')
  const [newQuery, setNewQuery] = useState('')
  // Journal (venue) create: a resolved source is required, plus an optional
  // comma-separated keyword filter.
  const [selectedVenue, setSelectedVenue] = useState<VenueSearchResult | null>(null)
  const [venueKeywords, setVenueKeywords] = useState('')

  const monitorsQuery = useQuery({
    queryKey: ['feed-monitors', 'settings'],
    queryFn: listFeedMonitors,
    staleTime: 30_000,
    retry: 1,
  })

  const createMutation = useMutation({
    mutationFn: () => {
      if (newType === 'venue') {
        if (!selectedVenue) throw new Error('Pick a journal first')
        return createFeedMonitor({
          monitor_type: 'venue',
          query: selectedVenue.display_name,
          source_id: selectedVenue.source_id,
          filter_keywords: venueKeywords
            .split(',')
            .map((k) => k.trim())
            .filter(Boolean),
        })
      }
      return createFeedMonitor({
        monitor_type: newType,
        label: newLabel.trim() || undefined,
        query: newQuery.trim(),
      })
    },
    onSuccess: async () => {
      setNewLabel('')
      setNewQuery('')
      setSelectedVenue(null)
      setVenueKeywords('')
      await invalidateQueries(queryClient, ['feed-monitors'], ['feed-inbox'])
      toast({
        title:
          newType === 'query'
            ? 'Keyword monitor added'
            : newType === 'venue'
              ? 'Journal followed'
              : 'Topic monitor added',
        description: 'The new Feed rule is now active.',
      })
    },
    onError: () => errorToast('Could not create monitor'),
  })

  const monitors = useMemo(() => monitorsQuery.data ?? [], [monitorsQuery.data])
  const authorMonitors = useMemo(
    () => monitors.filter((monitor) => monitor.monitor_type === 'author'),
    [monitors],
  )
  const keywordMonitors = useMemo(
    () => monitors.filter((monitor) => monitor.monitor_type === 'query'),
    [monitors],
  )
  const topicMonitors = useMemo(
    () => monitors.filter((monitor) => monitor.monitor_type === 'topic'),
    [monitors],
  )
  const venueMonitors = useMemo(
    () =>
      [...monitors.filter((monitor) => monitor.monitor_type === 'venue')].sort(
        (a, b) => (a.position ?? 9999) - (b.position ?? 9999),
      ),
    [monitors],
  )
  const otherMonitors = useMemo(
    () => monitors.filter((monitor) => !['author', 'query', 'topic', 'venue'].includes(monitor.monitor_type)),
    [monitors],
  )

  const venueSensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }),
  )
  const reorderVenuesMutation = useMutation({
    mutationFn: reorderFeedMonitors,
    // Optimistic: stamp the new positions so the sort reflects immediately.
    onMutate: (orderedIds: string[]) => {
      const posById = new Map(orderedIds.map((id, index) => [id, index]))
      queryClient.setQueryData<FeedMonitor[]>(['feed-monitors', 'settings'], (prev) =>
        prev?.map((m) => (posById.has(m.id) ? { ...m, position: posById.get(m.id)! } : m)),
      )
    },
    onError: () => errorToast('Could not reorder journals'),
  })
  const handleVenueDragEnd = (event: DragEndEvent) => {
    const { active, over } = event
    if (!over || active.id === over.id) return
    const ids = venueMonitors.map((m) => m.id)
    const oldIndex = ids.indexOf(String(active.id))
    const newIndex = ids.indexOf(String(over.id))
    if (oldIndex < 0 || newIndex < 0) return
    reorderVenuesMutation.mutate(arrayMove(ids, oldIndex, newIndex))
  }

  return (
    <div className="space-y-6">
      <PageSection id="feed-add-source" title="Add a source" icon={Plus}
        description="Follow a keyword rule, a topic, or a journal.">
      <div className="space-y-3">
        <div className="grid items-end gap-3 lg:grid-cols-[170px_minmax(0,1fr)_auto]">
          <div className="space-y-1.5"><Label htmlFor="feed-source-type">Source type</Label>
          <Select
            value={newType}
            onValueChange={(value) => setNewType(value as FeedMonitorCreateType)}
            disabled={createMutation.isPending}
          >
            <SelectTrigger id="feed-source-type">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="query">Keyword rule</SelectItem>
              <SelectItem value="topic">Topic</SelectItem>
              <SelectItem value="venue">Journal</SelectItem>
            </SelectContent>
          </Select>
          </div>
          {newType === 'venue' ? (
            <div className="space-y-2">
              {selectedVenue ? (
                <div className="flex items-center gap-2 rounded-sm border border-accent-edge bg-accent-soft px-3 py-2 text-sm">
                  <BookOpen className="h-4 w-4 shrink-0 text-alma-folio" aria-hidden />
                  <span className="min-w-0 flex-1 truncate font-medium text-alma-folio">
                    {selectedVenue.display_name}
                  </span>
                  <span className="shrink-0 text-xs tabular-nums text-slate-500">
                    {selectedVenue.works_count.toLocaleString()} works
                  </span>
                  <button
                    type="button"
                    onClick={() => setSelectedVenue(null)}
                    className="shrink-0 text-slate-400 hover:text-slate-600"
                    aria-label="Clear selected journal"
                  >
                    <X className="h-3.5 w-3.5" />
                  </button>
                </div>
              ) : (
                <VenueAutocomplete onSelect={setSelectedVenue} disabled={createMutation.isPending} />
              )}
              <Input
                value={venueKeywords}
                onChange={(event) => setVenueKeywords(event.target.value)}
                placeholder="Optional keywords, comma-separated — only papers matching these"
                disabled={createMutation.isPending || !selectedVenue}
              />
            </div>
          ) : (
            <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_minmax(0,0.9fr)]">
              <div className="space-y-1.5"><Label htmlFor="feed-source-rule">Match papers about</Label>
              <Input
                id="feed-source-rule"
                value={newQuery}
                onChange={(event) => setNewQuery(event.target.value)}
                placeholder={createMonitorPlaceholder(newType)}
                disabled={createMutation.isPending}
              />
              </div>
              <div className="space-y-1.5"><Label htmlFor="feed-source-name">Name <span className="font-normal text-slate-400">(optional)</span></Label>
              <Input
                id="feed-source-name"
                value={newLabel}
                onChange={(event) => setNewLabel(event.target.value)}
                placeholder="Display label"
                disabled={createMutation.isPending}
              />
              </div>
            </div>
          )}
          <AsyncButton
            type="button"
            icon={newType === 'venue' ? <BookOpen className="h-4 w-4" /> : <Plus className="h-4 w-4" />}
            pending={createMutation.isPending}
            disabled={newType === 'venue' ? !selectedVenue : !newQuery.trim()}
            onClick={() => createMutation.mutate()}
          >
            {newType === 'venue' ? 'Follow journal' : 'Add Monitor'}
          </AsyncButton>
        </div>
        <p className="mt-3 text-xs text-slate-500">{createMonitorHelp(newType)}</p>
      </div>

      </PageSection>

      {monitorsQuery.isLoading ? (
        <p className="text-sm text-slate-500">Loading Feed monitors...</p>
      ) : monitorsQuery.isError ? (
        <Alert variant="negative">
          <AlertDescription>Failed to load Feed monitors.</AlertDescription>
        </Alert>
      ) : monitors.length === 0 ? (
        <EmptyState title="No Feed monitors yet" description="Add a topic or keyword monitor above." />
      ) : (
        <div className="space-y-5">
          {topicMonitors.length > 0 && (
            <MonitorSection icon={Tag} title="Topics">
              {topicMonitors.map((monitor) => (
                <MonitorRow key={monitor.id} monitor={monitor} />
              ))}
            </MonitorSection>
          )}
          {keywordMonitors.length > 0 && (
            <MonitorSection icon={Search} title="Keyword rules">
              {keywordMonitors.map((monitor) => (
                <MonitorRow key={monitor.id} monitor={monitor} />
              ))}
            </MonitorSection>
          )}
          {venueMonitors.length > 0 && (
            <MonitorSection icon={BookOpen} title="Journals">
              <DndContext
                sensors={venueSensors}
                collisionDetection={closestCenter}
                onDragEnd={handleVenueDragEnd}
              >
                <SortableContext
                  items={venueMonitors.map((m) => m.id)}
                  strategy={verticalListSortingStrategy}
                >
                  {venueMonitors.map((monitor) => (
                    <VenueMonitorRow
                      key={monitor.id}
                      monitor={monitor}
                      draggable={venueMonitors.length > 1}
                    />
                  ))}
                </SortableContext>
              </DndContext>
            </MonitorSection>
          )}
          {otherMonitors.length > 0 && (
            <MonitorSection icon={Tag} title="Other sources">
              {otherMonitors.map((monitor) => (
                <MonitorRow key={monitor.id} monitor={monitor} />
              ))}
            </MonitorSection>
          )}
          {authorMonitors.length > 0 && (
            <PageSection id="feed-author-monitors" title="Followed authors" icon={UserRound} count={authorMonitors.length}
              description="Pause or refresh individual authors. Follow more from the Authors page."
              collapsible defaultOpen={false}>
              <div className="space-y-2">{authorMonitors.map(monitor => <MonitorRow key={monitor.id} monitor={monitor} />)}</div>
            </PageSection>
          )}
        </div>
      )}
    </div>
  )
}

function MonitorSection({
  icon,
  title,
  children,
}: {
  icon: typeof Search
  title: string
  children: React.ReactNode
}) {
  return (
    <PageSection id={`feed-sources-${title.toLowerCase().replace(/ /g, '-')}`} title={title} icon={icon}>
      <div className="space-y-2">{children}</div>
    </PageSection>
  )
}
