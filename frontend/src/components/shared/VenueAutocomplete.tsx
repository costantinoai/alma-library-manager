import { useEffect, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { BookOpen, Loader2 } from 'lucide-react'

import { venueSearch, type VenueSearchResult } from '@/api/client'
import { Input } from '@/components/ui/input'

function formatWorks(value: number): string {
  if (!value) return '—'
  if (value >= 1000) return `${(value / 1000).toFixed(1)}k works`
  return `${value} works`
}

interface VenueAutocompleteProps {
  onSelect: (venue: VenueSearchResult) => void
  placeholder?: string
  disabled?: boolean
  autoFocus?: boolean
}

/** Debounced OpenAlex journal search with a results dropdown. Picking a result
 * hands the caller the resolved source id + metadata (never a free-text name).
 * Shared by Settings (add a journal monitor) and Discovery (search venues). */
export function VenueAutocomplete({ onSelect, placeholder, disabled, autoFocus }: VenueAutocompleteProps) {
  const [query, setQuery] = useState('')
  const [debounced, setDebounced] = useState('')
  const [open, setOpen] = useState(false)
  const containerRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    const t = setTimeout(() => setDebounced(query.trim()), 300)
    return () => clearTimeout(t)
  }, [query])

  // Close the dropdown on an outside click.
  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', onClick)
    return () => document.removeEventListener('mousedown', onClick)
  }, [])

  const search = useQuery({
    queryKey: ['venue-search', debounced],
    queryFn: () => venueSearch(debounced),
    enabled: debounced.length >= 2,
    staleTime: 60_000,
    retry: false,
  })
  const results = search.data?.results ?? []
  const showDropdown = open && debounced.length >= 2

  return (
    <div ref={containerRef} className="relative">
      <Input
        value={query}
        disabled={disabled}
        autoFocus={autoFocus}
        onChange={(e) => {
          setQuery(e.target.value)
          setOpen(true)
        }}
        onFocus={() => setOpen(true)}
        placeholder={placeholder ?? 'Search a journal by name…'}
      />
      {showDropdown && (
        <div className="absolute z-30 mt-1 max-h-64 w-full overflow-y-auto rounded-sm border border-[var(--color-border)] bg-surface-3 shadow-paper-md">
          {search.isFetching && results.length === 0 && (
            <div className="flex items-center gap-1.5 px-3 py-2 text-xs text-slate-400">
              <Loader2 className="h-3 w-3 animate-spin" /> Searching journals…
            </div>
          )}
          {!search.isFetching && results.length === 0 && (
            <div className="px-3 py-2 text-xs text-slate-400">No journals matched “{debounced}”.</div>
          )}
          {results.map((venue) => (
            <button
              key={venue.source_id}
              type="button"
              onClick={() => {
                onSelect(venue)
                setQuery('')
                setDebounced('')
                setOpen(false)
              }}
              className="flex w-full items-center gap-2 px-3 py-2 text-left text-sm text-slate-700 transition-colors hover:bg-accent-soft hover:text-alma-folio"
            >
              <BookOpen className="h-3.5 w-3.5 shrink-0 text-alma-folio" aria-hidden />
              <span className="min-w-0 flex-1 truncate font-medium">{venue.display_name}</span>
              <span className="shrink-0 text-xs tabular-nums text-slate-400">
                {formatWorks(venue.works_count)}
              </span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
