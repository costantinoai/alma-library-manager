import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Heart,
  BookOpen,
  FolderOpen,
  Tags,
  Layers,
  UploadCloud,
} from 'lucide-react'

import { getLibraryWorkflowSummary, type Publication, updateReadingStatus } from '@/api/client'
import { PaperCard } from '@/components/shared'
import { PaperDetailPanel } from '@/components/discovery'
import { SavedTab } from '@/components/library/SavedTab'
import { ReadingListTab } from '@/components/library/ReadingListTab'
import { CollectionsTab } from '@/components/library/CollectionsTab'
import { TagsTab } from '@/components/library/TagsTab'
import { TopicsTab } from '@/components/library/TopicsTab'
import { ImportsTab } from '@/components/library/ImportsTab'
import { Badge } from '@/components/ui/badge'
import { StatusBadge } from '@/components/ui/status-badge'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { type TabId, type TabDefinition } from '@/components/library/types'
import { buildHashRoute, navigateTo, useHashRoute } from '@/lib/hashRoute'
import { invalidateQueries } from '@/lib/queryHelpers'
import { cn } from '@/lib/utils'

const TABS: TabDefinition[] = [
  { id: 'saved', label: 'Saved', icon: Heart },
  { id: 'reading', label: 'Reading List', icon: BookOpen },
  { id: 'collections', label: 'Collections', icon: FolderOpen },
  { id: 'tags', label: 'Tags', icon: Tags },
  { id: 'topics', label: 'Topics', icon: Layers },
  { id: 'imports', label: 'Imports', icon: UploadCloud },
]

const DEFAULT_TAB: TabId = 'saved'
const VALID_TABS = new Set<TabId>(['saved', 'reading', 'collections', 'tags', 'topics', 'imports'])
const READING_ACTIONS: Array<{ value: 'reading' | 'done'; label: string }> = [
  { value: 'reading', label: 'Reading' },
  { value: 'done', label: 'Done' },
]

/**
 * Library landing uses the shared PaperCard (compact mode) so the "Reading
 * Workflow" and "Needs attention" lists read as the same primitive as the
 * tabs below. A small reading-status Select is provided via `quickActions`
 * so the landing can still triage without leaving the page.
 */
function LandingPaperRow({
  paper,
  onSetReadingStatus,
  onOpenDetails,
  reasonsSlot,
}: {
  paper: Publication
  onSetReadingStatus: (paperId: string, readingStatus: 'reading' | 'done') => void
  onOpenDetails: (paper: Publication) => void
  /** Optional inline reasons block (used by Needs Attention rows). */
  reasonsSlot?: React.ReactNode
}) {
  return (
    <div className="relative space-y-1.5">
      <PaperCard
        paper={paper}
        compact
        onDetails={() => onOpenDetails(paper)}
        onPivot={() => navigateTo('discovery', { seed: paper.id, seedTitle: paper.title })}
        quickActions={
          <div className="flex items-center gap-2 text-xs text-slate-500">
            <span className="font-medium">Reading</span>
            <Select
              value={paper.reading_status ?? ''}
              onValueChange={(value) => onSetReadingStatus(paper.id, value as 'reading' | 'done')}
            >
              <SelectTrigger className="h-8 w-36 text-xs">
                <SelectValue placeholder="Set status" />
              </SelectTrigger>
              <SelectContent>
                {READING_ACTIONS.map((action) => (
                  <SelectItem key={action.value} value={action.value}>
                    {action.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        }
      />
      {/* Warning bubbles for needs-attention reasons overlay the
          paper card at lower-right, on top of the card surface. The
          chip strip is absolutely positioned inside the `relative`
          wrapper so the bubbles read as flags ON the row (not as a
          separate footer). Each chip carries the full `detail`
          string in its title attribute for hover-to-reveal context. */}
      {reasonsSlot ? (
        <div className="pointer-events-none absolute bottom-2 right-2 z-10 flex flex-wrap justify-end gap-1.5 [&>*]:pointer-events-auto">
          {reasonsSlot}
        </div>
      ) : null}
    </div>
  )
}

export function LibraryPage() {
  const route = useHashRoute()
  const queryClient = useQueryClient()
  const routeTab = route.params.get('tab')?.trim() as TabId | undefined
  const [activeTab, setActiveTab] = useState<TabId>(VALID_TABS.has(routeTab ?? DEFAULT_TAB) ? (routeTab ?? DEFAULT_TAB) : DEFAULT_TAB)
  const [selectedPaper, setSelectedPaper] = useState<Publication | null>(null)
  const [detailOpen, setDetailOpen] = useState(false)

  useEffect(() => {
    const nextTab = VALID_TABS.has(routeTab ?? DEFAULT_TAB) ? (routeTab ?? DEFAULT_TAB) : DEFAULT_TAB
    setActiveTab(nextTab)
  }, [routeTab])

  const workflowQuery = useQuery({
    queryKey: ['library-workflow-summary'],
    queryFn: getLibraryWorkflowSummary,
    staleTime: 30_000,
    retry: 1,
  })

  const readingStatusMutation = useMutation({
    mutationFn: ({ paperId, readingStatus }: { paperId: string; readingStatus: 'reading' | 'done' | 'excluded' | null }) =>
      updateReadingStatus(paperId, readingStatus),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['library-workflow-summary'], ['reading-queue'], ['papers'], ['library-saved'])
    },
  })

  const workflow = workflowQuery.data

  const needsAttentionCount = workflow?.needs_attention_count ?? 0
  const totalLibrary = workflow?.summary.total_library
  const readingCount = workflow?.summary.reading_count
  const collectionsTotal = workflow?.summary.collections_total
  const uncollected = workflow?.summary.uncollected_count ?? 0

  return (
    <div className="space-y-6">
      {/* Page header — inline metadata line carries the three landing
          numbers (papers · reading · collections) so they cost zero
          extra rows. Folio-blue accent on Currently Reading > 0 marks
          the only number that's actionable; everything else stays
          alma-800. The reading workflow lives entirely on the Reading
          List tab now (per beta feedback 2026-05-06 — see
          tasks/01_BETA_FEEDBACK.md Workstream B). */}
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div className="space-y-1">
          <h1 className="font-brand text-2xl font-semibold text-alma-800">Library</h1>
          <p className="flex flex-wrap items-center gap-x-2 gap-y-0.5 text-sm text-slate-500">
            <span>
              <strong className="font-medium text-alma-800 tabular-nums">
                {totalLibrary ?? '—'}
              </strong>{' '}
              papers
            </span>
            <span aria-hidden className="text-slate-300">·</span>
            <span>
              <strong
                className={cn(
                  'font-medium tabular-nums',
                  (readingCount ?? 0) > 0 ? 'text-alma-folio' : 'text-alma-800',
                )}
              >
                {readingCount ?? '—'}
              </strong>{' '}
              reading
            </span>
            <span aria-hidden className="text-slate-300">·</span>
            <span>
              <strong className="font-medium text-alma-800 tabular-nums">
                {collectionsTotal ?? '—'}
              </strong>{' '}
              collections
              {uncollected > 0 && (
                <span className="text-slate-400">
                  {' '}· {uncollected} uncategorised
                </span>
              )}
            </span>
          </p>
        </div>
        {/* Right-side header slot reserved for future Library-wide actions. */}
      </header>

      {/* Needs Attention — collapsed by default. Each row carries an
          inline reasons strip explaining WHY the paper is flagged + a
          suggested action verb so the user can act without thinking. */}
      <details className="group rounded-sm border border-[var(--color-border)] bg-alma-chrome shadow-paper-sheet">
        <summary className="flex cursor-pointer select-none items-center justify-between gap-3 px-4 py-3 text-left">
          <div className="flex flex-col gap-0.5">
            <div className="flex items-center gap-2">
              <span className="font-brand text-sm font-semibold text-alma-800">Needs attention</span>
              {needsAttentionCount > 0 && (
                <Badge variant="outline" className={needsAttentionCount > 0 ? 'text-amber-700' : ''}>
                  {needsAttentionCount}
                </Badge>
              )}
            </div>
            <span className="text-xs text-slate-500">
              {needsAttentionCount === 0
                ? 'Every saved paper is cleanly identified.'
                : 'Library papers with concrete metadata gaps — each row says why and what to do.'}
            </span>
          </div>
          <span className="text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:hidden">Show</span>
          <span className="hidden text-[11px] font-bold uppercase tracking-[0.16em] text-slate-500 group-open:inline">Hide</span>
        </summary>
        <div className="space-y-3 border-t border-[var(--color-border)] px-3 pb-3 pt-3">
          {(workflow?.needs_attention ?? []).length === 0 ? (
            <p className="text-sm text-slate-400">No metadata gaps right now.</p>
          ) : (
            workflow?.needs_attention.map((paper) => (
              <LandingPaperRow
                key={paper.id}
                paper={paper}
                onSetReadingStatus={(paperId, readingStatus) => readingStatusMutation.mutate({ paperId, readingStatus })}
                onOpenDetails={(p) => { setSelectedPaper(p); setDetailOpen(true) }}
                reasonsSlot={
                  paper.attention_reasons && paper.attention_reasons.length > 0
                    ? paper.attention_reasons.map((reason) => (
                        <StatusBadge
                          key={reason.code}
                          tone="warning"
                          size="sm"
                          // `detail` is the concrete fact ("Resolution
                          // status: not_openalex_resolved", "Abstract
                          // is only 8 chars"); it stays in the title
                          // attribute so the chip itself reads at a
                          // glance and the full evidence is one hover
                          // away.
                          title={reason.detail ?? undefined}
                        >
                          {reason.label}
                        </StatusBadge>
                      ))
                    : undefined
                }
              />
            ))
          )}
        </div>
      </details>

      {/* ── Tab bar ──────────────────────────────────────────────────────
          Segmented-chip strip (matches the Feed control-bar pattern) so
          the Library and Feed surfaces read as the same product.
      ─────────────────────────────────────────────────────────────────── */}
      <div
        className="inline-flex w-full items-center gap-0.5 overflow-x-auto rounded-sm border border-[var(--color-border)] bg-parchment-100/80 p-1 shadow-sm"
        role="tablist"
        aria-label="Library sections"
      >
        {TABS.map((tab) => {
          const isActive = activeTab === tab.id
          return (
            <button
              key={tab.id}
              role="tab"
              aria-selected={isActive}
              onClick={() => {
                setActiveTab(tab.id)
                window.location.hash = buildHashRoute('library', { tab: tab.id })
              }}
              className={`inline-flex shrink-0 items-center gap-1.5 whitespace-nowrap rounded-sm px-3 py-1.5 text-sm font-medium transition-colors ${
                isActive
                  ? 'bg-alma-chrome text-alma-800 shadow-paper-sm ring-1 ring-[var(--color-border)]'
                  : 'text-slate-600 hover:bg-alma-chrome/60 hover:text-alma-800'
              }`}
            >
              <tab.icon className={`h-4 w-4 ${isActive ? 'text-alma-folio' : 'text-slate-400'}`} />
              {tab.label}
            </button>
          )
        })}
      </div>

      {activeTab === 'saved' && (
        <SavedTab
          onOpenDetails={(p) => { setSelectedPaper(p); setDetailOpen(true) }}
        />
      )}
      {activeTab === 'reading' && <ReadingListTab />}
      {activeTab === 'collections' && <CollectionsTab />}
      {activeTab === 'tags' && <TagsTab />}
      {activeTab === 'topics' && <TopicsTab />}
      {activeTab === 'imports' && <ImportsTab />}

      <PaperDetailPanel paper={selectedPaper} open={detailOpen} onOpenChange={setDetailOpen} />
    </div>
  )
}
