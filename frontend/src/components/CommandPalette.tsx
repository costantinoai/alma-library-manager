import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import {
  BarChart3,
  Bell,
  BookMarked,
  Compass,
  FileText,
  Folder,
  Plus,
  RefreshCw,
  Rss,
  Settings as SettingsIcon,
  Tag,
  Upload,
  UserCircle2,
  Users,
} from 'lucide-react'

import { globalSearch, type SearchResult } from '@/api/client'
import {
  Command,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from '@/components/ui/command'
import { Dialog, DialogContent } from '@/components/ui/dialog'
import { Kbd } from '@/components/ui/kbd'
import { useDebounce } from '@/hooks/useDebounce'

interface CommandPaletteProps {
  isOpen: boolean
  onClose: () => void
  onNavigate: (url: string) => void
}

// ── Static action sets ──────────────────────────────────────────────────
// Rendered above the async query results so an empty ⌘K is immediately
// useful. "Pages" is pure navigation; "Quick actions" reuse the existing
// `?action=` hash convention that the target page listens to.
interface CommandAction {
  id: string
  name: string
  icon: React.ComponentType<{ className?: string }>
  url: string
  /** Optional extra tokens for cmdk's fuzzy match (keywords / aliases). */
  keywords?: string[]
}

const PAGES: CommandAction[] = [
  { id: 'nav-feed', name: 'Feed', icon: Rss, url: '#/feed', keywords: ['inbox', 'new papers'] },
  { id: 'nav-discovery', name: 'Discovery', icon: Compass, url: '#/discovery', keywords: ['recommendations', 'suggestions'] },
  { id: 'nav-library', name: 'Library', icon: BookMarked, url: '#/library', keywords: ['saved', 'papers', 'collections'] },
  { id: 'nav-corpus', name: 'Corpus explorer', icon: FileText, url: '#/settings?anchor=corpus-explorer', keywords: ['database', 'all papers', 'tracked', 'diagnostic', 'settings'] },
  { id: 'nav-authors', name: 'Authors', icon: UserCircle2, url: '#/authors', keywords: ['followed', 'people'] },
  { id: 'nav-alerts', name: 'Alerts', icon: Bell, url: '#/alerts', keywords: ['digests', 'rules'] },
  { id: 'nav-insights', name: 'Insights', icon: BarChart3, url: '#/insights', keywords: ['stats', 'metrics', 'charts'] },
  { id: 'nav-settings', name: 'Settings', icon: SettingsIcon, url: '#/settings', keywords: ['preferences', 'config'] },
]

const QUICK_ACTIONS: CommandAction[] = [
  { id: 'action-refresh-feed', name: 'Refresh Feed', icon: RefreshCw, url: '#/feed?action=refresh', keywords: ['update', 'fetch', 'new'] },
  { id: 'action-refresh-discovery', name: 'Refresh Discovery', icon: RefreshCw, url: '#/discovery?action=refresh', keywords: ['update', 'recompute'] },
  { id: 'action-create-collection', name: 'Create Collection', icon: Plus, url: '#/library?action=new-collection', keywords: ['add', 'group'] },
  { id: 'action-import-papers', name: 'Import Papers', icon: Upload, url: '#/library?action=import', keywords: ['bibtex', 'zotero', 'upload'] },
]

function getCategoryIcon(type: SearchResult['type']) {
  switch (type) {
    case 'paper':
      return FileText
    case 'author':
      return Users
    case 'collection':
      return Folder
    case 'topic':
      return Tag
    default:
      return FileText
  }
}

export function CommandPalette({ isOpen, onClose, onNavigate }: CommandPaletteProps) {
  const [query, setQuery] = useState('')
  const debouncedQuery = useDebounce(query, 300)

  // Server-side search. cmdk's own filtering is disabled below via
  // `shouldFilter={false}` so the static PAGES/QUICK_ACTIONS groups stay
  // visible while we wait for the backend, and the async groups appear
  // exactly as the API returns them (no client-side re-sort or hide).
  const searchQuery = useQuery({
    queryKey: ['global-search', debouncedQuery],
    queryFn: () => globalSearch(debouncedQuery),
    enabled: isOpen && debouncedQuery.trim().length >= 2,
    staleTime: 30_000,
  })

  // Reset query on close so re-opening is clean.
  useEffect(() => {
    if (!isOpen) setQuery('')
  }, [isOpen])

  const handleSelect = (url: string) => {
    onNavigate(url)
    onClose()
  }

  const results = searchQuery.data ?? { papers: [], authors: [], collections: [], topics: [] }
  const hasQuery = debouncedQuery.trim().length >= 2
  const isSearching = hasQuery && searchQuery.isFetching
  const totalAsyncResults =
    results.papers.length + results.authors.length + results.collections.length + results.topics.length
  const showEmptyResults = hasQuery && !isSearching && totalAsyncResults === 0

  return (
    <Dialog open={isOpen} onOpenChange={(v) => { if (!v) onClose() }}>
      <DialogContent className="overflow-hidden p-0 sm:max-w-2xl">
        <Command
          shouldFilter={false}
          loop
          className="[&_[cmdk-group-heading]]:px-3 [&_[cmdk-group-heading]]:py-2 [&_[cmdk-group-heading]]:text-[11px] [&_[cmdk-group-heading]]:font-semibold [&_[cmdk-group-heading]]:uppercase [&_[cmdk-group-heading]]:tracking-wider [&_[cmdk-group-heading]]:text-slate-400"
        >
          <CommandInput
            value={query}
            onValueChange={setQuery}
            placeholder="Search papers, authors, collections, topics — or jump to a page…"
          />

          <CommandList className="max-h-[70vh]">
            {/*
             * Static groups (Pages + Quick actions) only show on cold-
             * open. As soon as the user starts typing, they collapse so
             * search results dominate the visible area instead of being
             * pushed below 13 fixed rows. Solves the "half the screen
             * is options, results are at the bottom" problem.
             */}
            {!hasQuery && (
              <>
                <CommandGroup heading="Pages">
                  {PAGES.map((route) => {
                    const Icon = route.icon
                    return (
                      <CommandItem
                        key={route.id}
                        value={`${route.id} ${route.name} ${(route.keywords ?? []).join(' ')}`}
                        onSelect={() => handleSelect(route.url)}
                      >
                        <Icon className="text-slate-500" />
                        <span>{route.name}</span>
                      </CommandItem>
                    )
                  })}
                </CommandGroup>

                <CommandSeparator />

                <CommandGroup heading="Quick actions">
                  {QUICK_ACTIONS.map((action) => {
                    const Icon = action.icon
                    return (
                      <CommandItem
                        key={action.id}
                        value={`${action.id} ${action.name} ${(action.keywords ?? []).join(' ')}`}
                        onSelect={() => handleSelect(action.url)}
                      >
                        <Icon className="text-slate-500" />
                        <span>{action.name}</span>
                      </CommandItem>
                    )
                  })}
                </CommandGroup>
              </>
            )}

            {/* Async server results (only shown once the user types ≥2
                chars). Sections collapse individually when empty so we
                never render a stub heading above no items. */}
            {hasQuery && results.papers.length > 0 && (
              <>
                <CommandSeparator />
                <CommandGroup heading="Papers">
                  {results.papers.map((r) => <SearchResultItem key={r.id} result={r} onSelect={handleSelect} />)}
                </CommandGroup>
              </>
            )}
            {hasQuery && results.authors.length > 0 && (
              <>
                <CommandSeparator />
                <CommandGroup heading="Authors">
                  {results.authors.map((r) => <SearchResultItem key={r.id} result={r} onSelect={handleSelect} />)}
                </CommandGroup>
              </>
            )}
            {hasQuery && results.collections.length > 0 && (
              <>
                <CommandSeparator />
                <CommandGroup heading="Collections">
                  {results.collections.map((r) => <SearchResultItem key={r.id} result={r} onSelect={handleSelect} />)}
                </CommandGroup>
              </>
            )}
            {hasQuery && results.topics.length > 0 && (
              <>
                <CommandSeparator />
                <CommandGroup heading="Topics">
                  {results.topics.map((r) => <SearchResultItem key={r.id} result={r} onSelect={handleSelect} />)}
                </CommandGroup>
              </>
            )}

            {isSearching && totalAsyncResults === 0 && (
              <div className="py-4 text-center text-sm text-slate-500">Searching…</div>
            )}
            {showEmptyResults && (
              <div className="py-4 text-center text-sm text-slate-500">
                No matches for <span className="font-medium text-slate-700">“{debouncedQuery}”</span>.
              </div>
            )}
          </CommandList>

          {/* Footer — keyboard hints using the Kbd primitive for a single
              canonical shortcut-chip style across the app. */}
          <div className="flex items-center justify-between gap-4 border-t border-[var(--color-border)] bg-parchment-50 px-4 py-2 text-xs text-slate-500">
            <div className="flex items-center gap-3">
              <span className="inline-flex items-center gap-1"><Kbd>↑</Kbd><Kbd>↓</Kbd><span className="ml-1">navigate</span></span>
              <span className="inline-flex items-center gap-1"><Kbd>↵</Kbd><span className="ml-1">select</span></span>
              <span className="inline-flex items-center gap-1"><Kbd>esc</Kbd><span className="ml-1">close</span></span>
            </div>
            {isSearching && <span className="text-slate-400">Searching…</span>}
          </div>
        </Command>
      </DialogContent>
    </Dialog>
  )
}

function SearchResultItem({
  result,
  onSelect,
}: {
  result: SearchResult
  onSelect: (url: string) => void
}) {
  const Icon = getCategoryIcon(result.type)
  return (
    <CommandItem
      value={`${result.type}-${result.id}-${result.name}`}
      onSelect={() => onSelect(result.url)}
    >
      <Icon className="text-slate-500" />
      <div className="min-w-0 flex-1">
        <div className="truncate font-medium text-alma-800">{result.name}</div>
        {result.subtitle && (
          <div className="truncate text-xs text-slate-500">{result.subtitle}</div>
        )}
      </div>
    </CommandItem>
  )
}
