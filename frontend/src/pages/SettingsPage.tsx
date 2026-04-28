import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertCircle, CheckCircle, Cable, Database, Save, Sparkles } from 'lucide-react'

import { api, type Settings } from '@/api/client'
import { AsyncButton } from '@/components/settings/primitives'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { Separator } from '@/components/ui/separator'
import { Skeleton } from '@/components/ui/skeleton'
import { BackendCard } from '@/components/settings/BackendCard'
import { OpenAlexCard } from '@/components/settings/OpenAlexCard'
import { IdentifierResolutionCard } from '@/components/settings/IdentifierResolutionCard'
import { ChannelsCard } from '@/components/settings/ChannelsCard'
import { DiscoveryWeightsCard } from '@/components/settings/DiscoveryWeightsCard'
import { FeedMonitorTermsCard } from '@/components/settings/FeedMonitorTermsCard'
import { AIConfigCard } from '@/components/settings/AIConfigCard'
import { DataManagementCard } from '@/components/settings/DataManagementCard'
import { LibraryManagementCard } from '@/components/settings/LibraryManagementCard'
import { CorpusMaintenanceCard } from '@/components/settings/CorpusMaintenanceCard'
import { CorpusExplorerCard } from '@/components/settings/CorpusExplorerCard'
import { AboutCard } from '@/components/settings/AboutCard'
import { OperationalStatusCard } from '@/components/settings/OperationalStatusCard'
import { invalidateQueries } from '@/lib/queryHelpers'
import { cn } from '@/lib/utils'

/**
 * Settings page — two-column layout (sticky TOC + scroll-spy on the left,
 * grouped cards on the right).
 *
 * The 11 cards split into three intents:
 *   - **Connections**        — upstream data sources and destinations. The
 *                              global "Save connection settings" button
 *                              lives at the bottom of this group and only
 *                              persists the settings those cards bind to.
 *   - **Intelligence tuning** — discovery weights / monitor terms / AI
 *                              provider. These cards self-save.
 *   - **Data & system**      — ops, import/export, about. Informational
 *                              or destructive utilities.
 *
 * Keeping the groups small (3–4 cards each) prevents the "endless settings
 * scroll" and makes the scope of the save button legible.
 */

type SectionId = 'connections' | 'intelligence' | 'system'
type AnchorId =
  | 'backend'
  | 'openalex'
  | 'id-resolution'
  | 'channels'
  | 'discovery-weights'
  | 'feed-monitors'
  | 'ai-config'
  | 'operational-status'
  | 'data-management'
  | 'library-management'
  | 'corpus-maintenance'
  | 'corpus-explorer'
  | 'about'

interface TocEntry {
  id: AnchorId
  label: string
  section: SectionId
}

const SECTIONS: { id: SectionId; label: string; caption: string; icon: typeof Cable }[] = [
  { id: 'connections', label: 'Connections', caption: 'Upstream sources and delivery channels', icon: Cable },
  { id: 'intelligence', label: 'Intelligence', caption: 'Discovery weights, monitor terms, AI provider', icon: Sparkles },
  { id: 'system', label: 'Data & system', caption: 'Ops, import/export, about', icon: Database },
]

const TOC: TocEntry[] = [
  { id: 'backend', label: 'Backend', section: 'connections' },
  { id: 'openalex', label: 'OpenAlex', section: 'connections' },
  { id: 'id-resolution', label: 'Identifier resolution', section: 'connections' },
  { id: 'channels', label: 'Delivery channels', section: 'connections' },
  { id: 'discovery-weights', label: 'Discovery weights', section: 'intelligence' },
  { id: 'feed-monitors', label: 'Feed monitor terms', section: 'intelligence' },
  { id: 'ai-config', label: 'AI provider', section: 'intelligence' },
  { id: 'operational-status', label: 'Operational status', section: 'system' },
  { id: 'data-management', label: 'Data management', section: 'system' },
  { id: 'library-management', label: 'Library maintenance', section: 'system' },
  { id: 'corpus-maintenance', label: 'Corpus maintenance', section: 'system' },
  { id: 'corpus-explorer', label: 'Corpus explorer', section: 'system' },
  { id: 'about', label: 'About', section: 'system' },
]

export function SettingsPage() {
  const queryClient = useQueryClient()

  const settingsQuery = useQuery({
    queryKey: ['settings'],
    queryFn: () => api.get<Settings>('/settings'),
    retry: 1,
  })

  const [formData, setFormData] = useState<Settings>({
    backend: 'openalex',
    openalex_email: '',
    openalex_api_key: '',
    slack_token: '',
    slack_channel: '',
    check_interval_hours: 24,
    id_resolution_semantic_scholar_enabled: true,
    id_resolution_orcid_enabled: true,
    id_resolution_scholar_scrape_auto_enabled: false,
    id_resolution_scholar_scrape_manual_enabled: true,
  })

  const [saveSuccess, setSaveSuccess] = useState(false)
  const [activeAnchor, setActiveAnchor] = useState<AnchorId>('backend')
  const contentRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (settingsQuery.data) setFormData(settingsQuery.data)
  }, [settingsQuery.data])

  const saveMutation = useMutation({
    mutationFn: (data: Settings) => api.put<Settings>('/settings', data),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ['settings'], ['openalex-usage'])
      setSaveSuccess(true)
      setTimeout(() => setSaveSuccess(false), 3000)
    },
  })

  // ── Scroll-spy: keep TOC in sync with the card currently in view ─────
  useEffect(() => {
    if (!contentRef.current) return
    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((e) => e.isIntersecting)
          .sort((a, b) => (a.target.getBoundingClientRect().top - b.target.getBoundingClientRect().top))
        if (visible[0]) {
          const id = visible[0].target.getAttribute('data-anchor') as AnchorId | null
          if (id) setActiveAnchor(id)
        }
      },
      { rootMargin: '-15% 0px -70% 0px', threshold: [0, 1] },
    )
    const nodes = contentRef.current.querySelectorAll<HTMLElement>('[data-anchor]')
    nodes.forEach((n) => observer.observe(n))
    return () => observer.disconnect()
  }, [settingsQuery.data])

  const tocBySection = useMemo(() => {
    const map = new Map<SectionId, TocEntry[]>()
    for (const entry of TOC) {
      const list = map.get(entry.section) ?? []
      list.push(entry)
      map.set(entry.section, list)
    }
    return map
  }, [])

  function handleSave(): void {
    saveMutation.mutate(formData)
  }

  function jumpTo(id: AnchorId) {
    const node = contentRef.current?.querySelector<HTMLElement>(`[data-anchor="${id}"]`)
    if (!node) return
    node.scrollIntoView({ behavior: 'smooth', block: 'start' })
    setActiveAnchor(id)
  }

  // ── Loading / error ──
  if (settingsQuery.isLoading) {
    return (
      <div className="mx-auto max-w-6xl space-y-4">
        {Array.from({ length: 4 }, (_, i) => (
          <div key={i} className="rounded-sm border border-[var(--color-border)] bg-alma-paper p-4 shadow-paper-sm">
            <Skeleton className="h-4 w-1/3" />
            <Skeleton className="mt-3 h-3 w-2/3" />
            <Skeleton className="mt-4 h-10 w-full" />
          </div>
        ))}
      </div>
    )
  }
  if (settingsQuery.isError) {
    return (
      <div className="flex items-center justify-center gap-2 rounded-lg border border-red-200 bg-red-50 px-4 py-8">
        <AlertCircle className="h-5 w-5 text-red-500" />
        <span className="text-sm text-red-700">Failed to load settings. Is the backend running?</span>
      </div>
    )
  }

  return (
    <div className="mx-auto max-w-6xl">
      <div className="grid gap-8 lg:grid-cols-[220px_minmax(0,1fr)]">
        {/* ── Sticky TOC ─────────────────────────────────────────────── */}
        <aside className="hidden lg:block">
          <nav className="sticky top-6 space-y-5" aria-label="Settings sections">
            {SECTIONS.map((section) => {
              const entries = tocBySection.get(section.id) ?? []
              return (
                <div key={section.id}>
                  <EyebrowLabel tone="muted" className="mb-2 flex items-center gap-1.5">
                    <section.icon className="h-3.5 w-3.5" />
                    {section.label}
                  </EyebrowLabel>
                  <ul className="space-y-0.5 border-l border-slate-200 pl-2">
                    {entries.map((entry) => {
                      const active = activeAnchor === entry.id
                      return (
                        <li key={entry.id}>
                          <button
                            type="button"
                            onClick={() => jumpTo(entry.id)}
                            className={cn(
                              '-ml-[calc(0.5rem+1px)] flex w-full items-center rounded-r-md border-l-2 py-1 pl-3 pr-2 text-left text-sm transition-colors',
                              active
                                ? 'border-alma-500 bg-alma-50/50 font-medium text-alma-800'
                                : 'border-transparent text-slate-600 hover:border-[var(--color-border)] hover:text-alma-800',
                            )}
                          >
                            {entry.label}
                          </button>
                        </li>
                      )
                    })}
                  </ul>
                </div>
              )
            })}
          </nav>
        </aside>

        {/* ── Grouped content ───────────────────────────────────────── */}
        <div ref={contentRef} className="space-y-10">
          {/* -- Connections -- */}
          <SettingsSection id="connections" title="Connections" caption="Upstream sources and delivery channels.">
            <Anchor id="backend">
              <BackendCard
                backend={formData.backend}
                onBackendChange={(backend) => setFormData((prev) => ({ ...prev, backend }))}
              />
            </Anchor>
            <Anchor id="openalex">
              <OpenAlexCard
                formData={formData}
                onFormDataChange={setFormData}
                onSave={handleSave}
                isSaving={saveMutation.isPending}
                saveSuccess={saveSuccess}
              />
            </Anchor>
            <Anchor id="id-resolution">
              <IdentifierResolutionCard formData={formData} onFormDataChange={setFormData} />
            </Anchor>
            <Anchor id="channels">
              <ChannelsCard formData={formData} onFormDataChange={setFormData} />
            </Anchor>

            {/* Connection-settings save footer.
                The legacy global "Save Settings" button only ever persisted
                the Backend / OpenAlex / Identifier Resolution / Channels form
                state — the other cards (Discovery Weights, Feed Monitor
                Terms, AI Config) already self-save. Scoping the button to
                this section's footer makes the behaviour honest. */}
            <div className="flex flex-wrap items-center justify-end gap-3 pt-1">
              {saveSuccess && (
                <span className="inline-flex items-center gap-1.5 text-sm text-green-600">
                  <CheckCircle className="h-4 w-4" />
                  Connection settings saved
                </span>
              )}
              {saveMutation.isError && (
                <span className="inline-flex items-center gap-1.5 text-sm text-red-600">
                  <AlertCircle className="h-4 w-4" />
                  Failed to save
                </span>
              )}
              <AsyncButton
                icon={<Save className="h-4 w-4" />}
                pending={saveMutation.isPending}
                onClick={handleSave}
              >
                Save connection settings
              </AsyncButton>
            </div>
          </SettingsSection>

          {/* -- Intelligence -- */}
          <SettingsSection id="intelligence" title="Intelligence" caption="Discovery weights, monitor terms, and AI provider. These cards self-save.">
            <Anchor id="discovery-weights"><DiscoveryWeightsCard /></Anchor>
            <Anchor id="feed-monitors"><FeedMonitorTermsCard /></Anchor>
            <Anchor id="ai-config"><AIConfigCard /></Anchor>
          </SettingsSection>

          {/* -- Data & system -- */}
          <SettingsSection id="system" title="Data & system" caption="Operational status, data import/export, library maintenance.">
            <Anchor id="operational-status"><OperationalStatusCard /></Anchor>
            <Anchor id="data-management"><DataManagementCard /></Anchor>
            <Anchor id="library-management"><LibraryManagementCard /></Anchor>
            <Anchor id="corpus-maintenance"><CorpusMaintenanceCard /></Anchor>
            <Anchor id="corpus-explorer"><CorpusExplorerCard /></Anchor>
            <Anchor id="about"><AboutCard /></Anchor>
          </SettingsSection>
        </div>
      </div>
    </div>
  )
}

function SettingsSection({
  id,
  title,
  caption,
  children,
}: {
  id: SectionId
  title: string
  caption: string
  children: React.ReactNode
}) {
  return (
    <section aria-labelledby={`section-${id}`} className="space-y-4">
      <div className="pb-3">
        <h2 id={`section-${id}`} className="font-brand text-lg font-semibold tracking-tight text-alma-800">
          {title}
        </h2>
        <p className="mt-0.5 text-sm text-slate-500">{caption}</p>
        <Separator className="mt-3" />
      </div>
      <div className="space-y-5">{children}</div>
    </section>
  )
}

/** Wrap each card so the scroll-spy IntersectionObserver has a target. */
function Anchor({ id, children }: { id: AnchorId; children: React.ReactNode }) {
  return (
    <div data-anchor={id} id={id} className="scroll-mt-6">
      {children}
    </div>
  )
}
