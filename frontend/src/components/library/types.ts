import { type LucideIcon } from 'lucide-react'

export type TabId = 'saved' | 'reading' | 'collections' | 'tags' | 'topics' | 'imports' | 'analytics'

/**
 * Sort options for the Saved Library list.
 *
 * - `signal` — paper_signal composite ranking (distinct from `rating`,
 *   which is the user's 0-5 star curation). Uses `global_signal_score`
 *   on the `Publication` response; backend backfills lazily when this
 *   sort is chosen.
 */
export type SavedSortOption = 'date' | 'rating' | 'signal' | 'title'

export interface AllPapersFilters {
  search: string
  yearFrom: string
  yearTo: string
  minCitations: string
  sort: 'recent' | 'citations' | 'title' | 'rating'
}

export interface CollectionItemData {
  id: string
  added_at: string
  title?: string
  authors?: string
  year?: number
  url?: string
  cited_by_count?: number
}

export interface TabDefinition {
  id: TabId
  label: string
  icon: LucideIcon
}

export { PRESET_COLORS } from '@/lib/palette'
