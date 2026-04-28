import { type LucideIcon } from 'lucide-react'

export type TabId = 'saved' | 'reading' | 'collections' | 'tags' | 'topics' | 'imports'

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

export const PRESET_COLORS = [
  '#3B82F6', // blue
  '#8B5CF6', // violet
  '#EC4899', // pink
  '#EF4444', // red
  '#F59E0B', // amber
  '#10B981', // emerald
  '#06B6D4', // cyan
  '#6366F1', // indigo
  '#84CC16', // lime
  '#F97316', // orange
]
