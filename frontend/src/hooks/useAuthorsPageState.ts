import { useEffect, useState } from 'react'

import type { Author } from '@/api/client'

export type AuthorsSortOption = 'name' | 'citations' | 'publications' | 'h-index'

export type ScholarCandidate = {
  scholar_id: string
  display_name: string
  score: number
  affiliation?: string
  source?: string
  scholar_url?: string
}

type ResolveIdentifiersState = {
  isOpen: boolean
  author: Author | null
  openalexId: string
  scholarId: string
  manualScholarCandidates: ScholarCandidate[]
}

type AuthorsPageFiltersState = {
  search: string
  sort: AuthorsSortOption
  expandedAuthorId: string | null
  showBackgroundAuthors: boolean
  followedOnly: boolean
}

const INITIAL_RESOLVE_IDENTIFIERS_STATE: ResolveIdentifiersState = {
  isOpen: false,
  author: null,
  openalexId: '',
  scholarId: '',
  manualScholarCandidates: [],
}

const INITIAL_FILTERS_STATE: AuthorsPageFiltersState = {
  search: '',
  sort: 'name',
  expandedAuthorId: null,
  showBackgroundAuthors: false,
  followedOnly: false,
}

export function useAuthorsPageState({
  routeFilter,
  routeFollowedOnly,
}: {
  routeFilter: string
  routeFollowedOnly: boolean
}) {
  const [filters, setFilters] = useState<AuthorsPageFiltersState>(INITIAL_FILTERS_STATE)
  // The Add Author form state (name, identifiers, validation) now lives
  // inside AddAuthorDialog via react-hook-form. We only track the dialog's
  // open/closed state up here so the page's "+ Add Author" button can
  // trigger it.
  const [addAuthorOpen, setAddAuthorOpen] = useState(false)
  const [resolveIds, setResolveIds] = useState<ResolveIdentifiersState>(INITIAL_RESOLVE_IDENTIFIERS_STATE)

  useEffect(() => {
    if (!routeFilter) return
    setFilters((prev) => (prev.search === routeFilter ? prev : { ...prev, search: routeFilter }))
  }, [routeFilter])

  useEffect(() => {
    if (!routeFollowedOnly) return
    setFilters((prev) => (prev.followedOnly ? prev : { ...prev, followedOnly: true }))
  }, [routeFollowedOnly])

  const resolveIdentifiersPayload = {
    openalex_id: resolveIds.openalexId.trim() || undefined,
    scholar_id: resolveIds.scholarId.trim() || undefined,
  }

  return {
    filters: {
      ...filters,
      setSearch: (search: string) => setFilters((prev) => ({ ...prev, search })),
      setSort: (sort: AuthorsSortOption) => setFilters((prev) => ({ ...prev, sort })),
      toggleExpandedAuthor: (authorId: string) =>
        setFilters((prev) => ({
          ...prev,
          expandedAuthorId: prev.expandedAuthorId === authorId ? null : authorId,
        })),
      clearExpandedAuthor: () => setFilters((prev) => ({ ...prev, expandedAuthorId: null })),
      toggleBackgroundAuthors: () =>
        setFilters((prev) => ({ ...prev, showBackgroundAuthors: !prev.showBackgroundAuthors })),
      toggleFollowedOnly: () =>
        setFilters((prev) => ({ ...prev, followedOnly: !prev.followedOnly })),
    },
    addAuthor: {
      isOpen: addAuthorOpen,
      setOpen: setAddAuthorOpen,
    },
    resolveIds: {
      ...resolveIds,
      payload: resolveIdentifiersPayload,
      setOpen: (isOpen: boolean) =>
        setResolveIds((prev) => (
          isOpen
            ? { ...prev, isOpen }
            : { ...prev, isOpen: false, manualScholarCandidates: [] }
        )),
      openForAuthor: (author: Author) =>
        setResolveIds({
          isOpen: true,
          author,
          openalexId: author.openalex_id ?? '',
          scholarId: author.scholar_id ?? '',
          manualScholarCandidates: [],
        }),
      setOpenalexId: (openalexId: string) => setResolveIds((prev) => ({ ...prev, openalexId })),
      setScholarId: (scholarId: string) => setResolveIds((prev) => ({ ...prev, scholarId })),
      setManualScholarCandidates: (manualScholarCandidates: ScholarCandidate[]) =>
        setResolveIds((prev) => ({ ...prev, manualScholarCandidates })),
      reset: () => setResolveIds(INITIAL_RESOLVE_IDENTIFIERS_STATE),
    },
  }
}
