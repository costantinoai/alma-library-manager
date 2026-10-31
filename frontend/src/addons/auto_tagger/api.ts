import { api } from '@/api/client'

export interface TagSuggestion {
  paper_id: string
  tag: string
  tag_id?: string
  confidence: number
  source: 'embedding' | 'tfidf' | 'topic' | 'rule'
  accepted: boolean
  created_at?: string
}

export interface TagSuggestionsResponse {
  suggestions: TagSuggestion[]
  paper_title?: string
}

/** Get tag suggestions for a specific paper */
export function getTagSuggestions(paperId: string): Promise<TagSuggestionsResponse> {
  return api.get<TagSuggestionsResponse>(`/tags/suggestions/${paperId}`)
}

/** Accept a tag suggestion (creates real tag assignment) */
export function acceptTagSuggestion(
  paperId: string,
  tag: string,
): Promise<{ success: boolean }> {
  return api.post<{ success: boolean }>(
    `/tags/suggestions/${paperId}/accept`,
    { tag },
  )
}

/** Dismiss a tag suggestion */
export function dismissTagSuggestion(
  paperId: string,
  tag: string,
): Promise<void> {
  return api.delete<void>(
    `/tags/suggestions/${paperId}/${encodeURIComponent(tag)}`,
  )
}

export function bulkGenerateTagSuggestions(): Promise<{
  job_id: string
  status?: string
  operation_id?: string
  activity_url?: string
  operation_key?: string
  message?: string
}> {
  return api.post('/tags/suggestions/generate')
}
