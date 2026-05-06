import { repairDisplayText } from '@/lib/utils'

const BASE_URL = '/api/v1'

class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
  }
}

/**
 * Field names whose values may carry LaTeX-leaked `dotless-ı + combining
 * accent` sequences (see `repairDisplayText`). We normalise every match in
 * place during JSON deserialisation so individual rendering sites can stay
 * naïve about the upstream-data quirk.
 *
 * Limited to known-text fields so we never accidentally rewrite IDs, URLs,
 * keys, or other ASCII metadata. Backend-side fix lives in Phase 2 author
 * hydration; this is the belt that closes the visible bug today.
 */
const REPAIRABLE_FIELDS: ReadonlySet<string> = new Set([
  'title',
  'authors',
  'display_name',
  'name',
  'affiliation',
])

function repairDeep(value: unknown): unknown {
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = repairDeep(value[i])
    return value
  }
  if (value && typeof value === 'object') {
    const obj = value as Record<string, unknown>
    for (const key of Object.keys(obj)) {
      const v = obj[key]
      if (typeof v === 'string' && REPAIRABLE_FIELDS.has(key)) {
        obj[key] = repairDisplayText(v)
      } else if (v && typeof v === 'object') {
        obj[key] = repairDeep(v)
      }
    }
  }
  return value
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(`${BASE_URL}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options?.headers },
    ...options,
  })
  if (!response.ok) {
    const errorType = response.headers.get('content-type') || ''
    let message = response.statusText
    if (errorType.includes('application/json')) {
      const error = await response.json().catch(() => ({ detail: response.statusText }))
      message =
        error?.detail ||
        error?.message ||
        (Array.isArray(error?.errors) ? error.errors.join(', ') : '') ||
        response.statusText
    } else {
      const text = await response.text().catch(() => '')
      if (text) message = text
    }
    throw new ApiError(response.status, message)
  }

  if (response.status === 204 || response.status === 205) {
    return undefined as T
  }

  const contentType = response.headers.get('content-type') || ''
  if (contentType.includes('application/json')) {
    const data = await response.json()
    return repairDeep(data) as T
  }

  const text = await response.text()
  return (text ? (text as unknown as T) : (undefined as T))
}

export const api = {
  baseURL: BASE_URL,
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'POST', body: body ? JSON.stringify(body) : undefined }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'PUT', body: body ? JSON.stringify(body) : undefined }),
  patch: <T>(path: string, body?: unknown) =>
    request<T>(path, { method: 'PATCH', body: body ? JSON.stringify(body) : undefined }),
  delete: <T>(path: string) => request<T>(path, { method: 'DELETE' }),
}

// ── Activity job polling ──

/**
 * Response envelope returned by Activity-backed endpoints.
 * Matches `alma.api.scheduler.activity_envelope` on the backend.
 */
export interface JobEnvelope {
  status: 'queued' | 'scheduled' | 'running' | 'cancelling' | 'completed' | 'failed' | 'cancelled' | 'already_running'
  job_id: string
  operation_id?: string
  operation_key?: string
  activity_url?: string
  message?: string
  total?: number
  [extra: string]: unknown
}

/** Status envelope returned by `GET /activity/{job_id}`. */
export interface JobStatus<TResult = unknown> {
  job_id: string
  status: JobEnvelope['status']
  operation_key?: string
  message?: string
  error?: string
  started_at?: string
  finished_at?: string
  processed?: number
  total?: number
  result?: TResult
}

/** Return true when the response looks like a queued-job envelope (has a job_id + status). */
export function isJobEnvelope(value: unknown): value is JobEnvelope {
  const v = value as Partial<JobEnvelope> | null | undefined
  return typeof v?.job_id === 'string' && v.job_id.length > 0 && typeof v.status === 'string'
}

export interface WaitForJobOptions {
  intervalMs?: number
  timeoutMs?: number
  signal?: AbortSignal
}

/**
 * Poll `GET /activity/{job_id}` until the job reaches a terminal state and
 * return its `result` payload. Rejects on `failed` / `cancelled` / timeout.
 *
 * The default poll interval is 500ms (fast enough for interactive search,
 * cheap because each poll is a single indexed lookup) and the default timeout
 * is 120s (covers slow OpenAlex searches + discovery engine runs).
 */
export async function waitForJob<T>(jobId: string, opts: WaitForJobOptions = {}): Promise<T> {
  const intervalMs = Math.max(100, opts.intervalMs ?? 500)
  const timeoutMs = Math.max(1_000, opts.timeoutMs ?? 120_000)
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    if (opts.signal?.aborted) throw new Error('Aborted')
    const st = await api.get<JobStatus<T>>(`/activity/${encodeURIComponent(jobId)}`)
    if (st.status === 'completed') {
      if (st.result === undefined || st.result === null) {
        throw new Error(`Job ${jobId} completed with no result payload`)
      }
      return st.result
    }
    if (st.status === 'failed' || st.status === 'cancelled') {
      throw new Error(st.error || st.message || `Job ${jobId} ${st.status}`)
    }
    await new Promise<void>((resolve) => setTimeout(resolve, intervalMs))
  }
  throw new Error(`Timeout waiting for job ${jobId}`)
}

// ── Type definitions for API responses ──

export interface Author {
  id: string
  name: string
  added_at?: string
  author_type?: 'followed' | 'background' | string
  publication_count?: number
  affiliation?: string
  email_domain?: string
  citedby?: number
  h_index?: number
  interests?: string[]
  url_picture?: string
  works_count?: number
  last_fetched_at?: string
  orcid?: string
  openalex_id?: string
  scholar_id?: string
  id_resolution_status?: string
  id_resolution_reason?: string
  id_resolution_updated_at?: string
  id_resolution_method?: string | null
  id_resolution_confidence?: number | null
  monitor_health?: 'ready' | 'degraded' | 'disabled' | string | null
  monitor_health_reason?: string | null
  monitor_last_checked_at?: string | null
  monitor_last_success_at?: string | null
  monitor_last_status?: string | null
  monitor_last_error?: string | null
  monitor_last_result?: Record<string, unknown> | null
  monitor_papers_found?: number | null
  monitor_items_created?: number | null
  background_corpus_state?: string | null
  background_corpus_detail?: string | null
  background_corpus_last_success_at?: string | null
  background_corpus_age_days?: number | null
  background_corpus_publications?: number | null
  background_corpus_coverage_ratio?: number | null
}

/**
 * T7: one piece of evidence backing an author suggestion. Examples:
 *   {kind: 'specter_cosine', label: 'SPECTER 0.83', value: 0.83}
 *   {kind: 'cited_in_saved', label: 'cited in 4 saved', count: 4}
 *   {kind: 'coauthor', label: 'co-author of Andrea C.', subject: 'Andrea C.'}
 */
export interface AuthorSuggestionSignal {
  kind: string
  label: string
  count?: number | null
  value?: number | null
  subject?: string | null
}

export interface AuthorSuggestion {
  key: string
  name: string
  suggestion_type: 'library_core' | 'collaborator' | 'adjacent' | string
  score: number
  openalex_id?: string | null
  existing_author_id?: string | null
  known_author_type?: string | null
  shared_paper_count: number
  shared_followed_count: number
  local_paper_count: number
  recent_paper_count: number
  shared_followed_authors: string[]
  shared_topics: string[]
  shared_venues: string[]
  sample_titles: string[]
  /** T7: priority-ordered evidence chips (backend-computed). Up to 4
   *  entries. Absent on legacy cached rows that pre-date the rollout. */
  signals?: AuthorSuggestionSignal[]
  /** Same-human dedup: when the backend collapses multiple OpenAlex
   *  IDs that share a normalized display name (split profiles after a
   *  name spelling change, institution move, or ORCID drift), the
   *  surviving row carries the dropped IDs here so the dossier can
   *  surface a "this person has N OpenAlex profiles" hint. */
  alt_openalex_ids?: string[]
  negative_signal?: number
  last_removed_at?: string | null
  /** Number of independent buckets that surfaced this author. Drives
   *  the band-relative consensus bonus (+12 / +17 / +21 / +24 for
   *  2 / 3 / 4 / 5 buckets) on the per-bucket score. */
  consensus_count?: number
  consensus_buckets?: string[]
  /** Signed score adjustment from projected paper feedback (saves /
   *  ratings / dismisses fanned out via the projection layer). */
  paper_signal_adjustment?: number
  /** Per-bucket outcome-calibration multiplier (1.0 = neutral / fresh
   *  DB). Provenance only — already folded into `score`. */
  bucket_calibration_multiplier?: number
}

export interface AuthorFollowFromPaperResult {
  author: Author
  created: boolean
  already_followed: boolean
  matched_via?: string | null
}

export interface AuthorDossier {
  author: Author
  summary: {
    total_publications: number
    library_publications: number
    background_publications: number
    first_year?: number | null
    latest_year?: number | null
    tracked_corpus_ready: boolean
    tracked_corpus_state?: string | null
    background_coverage_ratio?: number | null
  }
  history: Array<{
    year: number
    total: number
    library: number
    background: number
  }>
  top_topics: Array<{ topic: string; count: number }>
  top_venues: Array<{ venue: string; count: number }>
  top_collaborators: Array<{ author_id?: string | null; name: string; count: number }>
  recent_publications: Publication[]
  background_publications: Publication[]
  recommended_actions: Array<{
    id: string
    label: string
    detail: string
  }>
  backfill?: {
    state: string
    stale: boolean
    thin: boolean
    background_publications: number
    works_count: number
    coverage_ratio?: number | null
    expected_background_floor?: number
    last_success_at?: string | null
    age_days?: number | null
    detail?: string | null
    recent_runs?: Array<{
      job_id: string
      status: string
      message?: string | null
      started_at?: string | null
      finished_at?: string | null
    }>
  } | null
}

export interface Publication {
  id: string
  title: string
  authors: string
  year: number | null
  journal?: string
  cited_by_count: number
  abstract?: string
  url?: string
  doi?: string
  rating?: number
  notes?: string
  status?: string
  added_at?: string
  updated_at?: string
  added_from?: string
  reading_status?: string | null
  openalex_id?: string
  work_type?: string
  language?: string
  is_oa?: boolean
  oa_status?: string
  oa_url?: string
  fwci?: number
  keywords?: string[]
  publication_date?: string
  openalex_resolution_status?: string
  openalex_resolution_reason?: string
  openalex_resolution_updated_at?: string
  /** paper_signal composite ranking (0..1) — blends rating + topic
   *  alignment + SPECTER2 similarity + author alignment + feedback
   *  learning + recency. Populated lazily when Library is sorted by
   *  "Ranking"; `0` means "not yet scored" (not "zero signal"). */
  global_signal_score?: number
  /** S2's 1-2 sentence AI summary of the paper. Dense coverage in CS
   *  + biomedicine, sparse elsewhere. PaperCard renders it italic
   *  under the abstract when present; hidden when absent (no
   *  placeholder, matches sparse-field policy). */
  tldr?: string | null
  /** S2's learned "this citation mattered" count — supplements
   *  `cited_by_count`. 0 means either genuinely zero influential
   *  citations or S2 hasn't classified the paper yet. */
  influential_citation_count?: number
}

export interface Stats {
  total_authors: number
  total_publications: number
  total_citations: number
  active_jobs: number
  configured_plugins: number
}

export interface Settings {
  backend: 'scholar' | 'openalex'
  openalex_email?: string
  openalex_api_key?: string
  fetch_full_history?: boolean
  from_year?: number
  api_call_delay?: string
  database?: string
  slack_config_path?: string
  slack_token?: string
  slack_channel?: string
  check_interval_hours?: number
  id_resolution_semantic_scholar_enabled?: boolean
  id_resolution_orcid_enabled?: boolean
  id_resolution_scholar_scrape_auto_enabled?: boolean
  id_resolution_scholar_scrape_manual_enabled?: boolean
}

export interface OpenAlexUsage {
  source?: string
  request_count: number
  retry_count?: number | null
  rate_limited_events?: number | null
  calls_saved_by_cache?: number | null
  credits_used?: number | null
  credits_remaining?: number | null
  credits_limit?: number | null
  resets_in_seconds?: number | null
  reset_at?: string | null
  summary: string
}

// ── Library types ──

export interface Collection {
  id: string
  name: string
  description?: string
  color: string
  created_at: string
  item_count: number
  last_added_at?: string | null
  avg_citations?: number | null
  avg_rating?: number | null
  activity_status?: 'fresh' | 'active' | 'stale' | 'dormant' | null
}

export interface Tag {
  id: string
  name: string
  color: string
}

export interface TopicSummary {
  canonical: string
  paper_count: number
  aliases: string[]
}

export interface TopicHierarchyNode {
  name: string
  paper_count: number
}

export interface TopicFieldNode extends TopicHierarchyNode {
  subfields: TopicHierarchyNode[]
}

export interface TopicDomainNode extends TopicHierarchyNode {
  fields: TopicFieldNode[]
}

export interface TopicHierarchyResponse {
  domains: TopicDomainNode[]
}

export interface FollowedAuthor {
  author_id: string
  followed_at: string
  notify_new_papers: boolean
  name?: string
}

// ── Library API helpers ──

export function listSavedPapers(params?: {
  search?: string
  order?: 'date' | 'rating' | 'signal' | 'title' | 'authors' | 'journal' | 'citations' | 'added_at'
  orderDir?: 'asc' | 'desc'
  limit?: number
  offset?: number
}): Promise<Publication[]> {
  const qs = new URLSearchParams()
  if (params?.search) qs.set('search', params.search)
  if (params?.order) qs.set('order', params.order)
  if (params?.orderDir) qs.set('order_dir', params.orderDir)
  if (params?.limit != null) qs.set('limit', String(params.limit))
  if (params?.offset != null) qs.set('offset', String(params.offset))
  const q = qs.toString()
  return api.get<Publication[]>(`/library/saved${q ? `?${q}` : ''}`)
}

export function addToLibrary(paperId: string, rating = 0): Promise<Publication> {
  return api.post<Publication>('/library/saved', { paper_id: paperId, rating })
}

export function removeFromLibrary(paperId: string): Promise<void> {
  return api.delete<void>(`/library/saved/${paperId}`)
}

/**
 * T6: one row in the Prior / Derivative Works panel on
 * PaperDetailPanel. Trimmed projection of `papers` — the dialog
 * doesn't need the full Publication shape for the compact list rows.
 * `paper_id` is the local UUID when the related work lives in our
 * corpus; `null` for S2 rows that haven't been imported.
 * `is_influential` is meaningful on derivative rows; always false on
 * prior rows (MVP).
 */
export interface RelatedWork {
  paper_id?: string | null
  title: string
  authors?: string | null
  year?: number | null
  doi?: string | null
  url?: string | null
  journal?: string | null
  abstract?: string | null
  tldr?: string | null
  cited_by_count?: number
  influential_citation_count?: number
  openalex_id?: string | null
  semantic_scholar_id?: string | null
  status?: string | null
  rating?: number | null
  is_influential?: boolean
  source?: 'local' | 's2_remote'
}

export interface RelatedWorksResponse {
  direction: 'prior' | 'derivative'
  source_paper_id: string
  works: RelatedWork[]
  local_count?: number
  remote_count?: number
}

export function getPriorWorks(paperId: string, limit = 30): Promise<RelatedWorksResponse> {
  return api.get<RelatedWorksResponse>(
    `/papers/${encodeURIComponent(paperId)}/prior-works?limit=${limit}`,
  )
}

export function getDerivativeWorks(paperId: string, limit = 30): Promise<RelatedWorksResponse> {
  return api.get<RelatedWorksResponse>(
    `/papers/${encodeURIComponent(paperId)}/derivative-works?limit=${limit}`,
  )
}

export function updateSavedPaper(
  paperId: string,
  body: {
    notes?: string
    rating?: number
    /** Soft-edit fields used by the PaperDetailPanel `...` menu —
     *  fix malformed imports without re-resolving from OpenAlex. */
    title?: string
    authors?: string
    abstract?: string
  },
): Promise<Publication> {
  const qs = new URLSearchParams()
  if (body.notes !== undefined) qs.set('notes', body.notes)
  if (body.rating !== undefined) qs.set('rating', String(body.rating))
  if (body.title !== undefined) qs.set('title', body.title)
  if (body.authors !== undefined) qs.set('authors', body.authors)
  if (body.abstract !== undefined) qs.set('abstract', body.abstract)
  const q = qs.toString()
  return api.put<Publication>(`/library/saved/${paperId}${q ? `?${q}` : ''}`)
}

// Bulk operations
export function bulkClearRating(paperIds: string[]): Promise<{ affected: number }> {
  return api.post('/library/bulk/clear-rating', { paper_ids: paperIds })
}

export function bulkRemoveFromLibrary(paperIds: string[]): Promise<{ affected: number }> {
  return api.post('/library/bulk/remove', { paper_ids: paperIds })
}

export function bulkAddToCollection(paperIds: string[], collectionId: string): Promise<{ affected: number }> {
  return api.post('/library/bulk/add-to-collection', { paper_ids: paperIds, collection_id: collectionId })
}

export function listCollections(): Promise<Collection[]> {
  return api.get<Collection[]>('/library/collections')
}

export function listTags(): Promise<Tag[]> {
  return api.get<Tag[]>('/library/tags')
}

export function listTopics(): Promise<TopicSummary[]> {
  return api.get<TopicSummary[]>('/library/topics')
}

export function listFollowedAuthors(): Promise<FollowedAuthor[]> {
  return api.get<FollowedAuthor[]>('/library/followed-authors')
}

export function followAuthor(authorId: string, notifyNewPapers = true): Promise<FollowedAuthor> {
  return api.post<FollowedAuthor>('/library/followed-authors', {
    author_id: authorId,
    notify_new_papers: notifyNewPapers,
  })
}

export function followAuthorFromPaper(body: {
  paper_id: string
  author_name: string
}): Promise<AuthorFollowFromPaperResult> {
  return api.post<AuthorFollowFromPaperResult>('/authors/follow-from-paper', body)
}

export function unfollowAuthor(authorId: string): Promise<void> {
  return api.delete<void>(`/library/followed-authors/${encodeURIComponent(authorId)}`)
}

export function listAuthorSuggestions(limit = 5): Promise<AuthorSuggestion[]> {
  return api.get<AuthorSuggestion[]>(`/authors/suggestions?limit=${limit}`)
}

export function rejectAuthorSuggestion(
  openalexId: string,
  suggestionBucket?: string | null,
): Promise<void> {
  return api.post<void>('/authors/suggestions/reject', {
    openalex_id: openalexId,
    suggestion_bucket: suggestionBucket ?? null,
  })
}

/** Fire-and-forget log for outcome calibration: records that the user
 *  followed an author surfaced by the rail. The actual follow write
 *  goes through `followAuthor` / `POST /authors`; this is the
 *  attribution log only. */
export function trackFollowedAuthorSuggestion(
  openalexId: string,
  suggestionBucket?: string | null,
): Promise<void> {
  return api.post<void>('/authors/suggestions/track-follow', {
    openalex_id: openalexId,
    suggestion_bucket: suggestionBucket ?? null,
  })
}

/**
 * D12 AUTH-SUG-3/4 — refresh the two network-backed suggestion caches
 * (OpenAlex co-author expansion + Semantic Scholar paper recommendations).
 * Returns one envelope per source; sources whose cache is fresh come
 * back with `status: 'fresh'` and no job id.
 */
export interface AuthorNetworkRefreshJob {
  source: 'openalex_related' | 's2_related'
  status: 'queued' | 'already_running' | 'fresh'
  job_id: string | null
  operation_key: string | null
  message: string
}

export function refreshAuthorSuggestionNetwork(
  force = false,
): Promise<{ jobs: AuthorNetworkRefreshJob[] }> {
  return api.post<{ jobs: AuthorNetworkRefreshJob[] }>(
    '/authors/suggestions/refresh-network',
    { force },
  )
}

/**
 * D12 Phase B — enqueue the corpus author works + SPECTER2 vector
 * backfill. `authorOpenalexId` null runs the batch variant (every
 * resolved author whose centroid is missing or older than 14 days).
 */
export function backfillAuthorWorks(opts: {
  authorOpenalexId?: string | null
  fullRefetch?: boolean
  limit?: number | null
} = {}): Promise<JobEnvelope> {
  return api.post<JobEnvelope>('/authors/backfill-works', {
    author_openalex_id: opts.authorOpenalexId ?? null,
    full_refetch: opts.fullRefetch ?? false,
    limit: opts.limit ?? null,
  })
}

export interface AuthorSignal {
  score: number
  library_papers: number
  total_papers: number
  avg_rating: number | null
}

export interface AuthorTopTopic {
  term: string
  papers: number
}

export interface AuthorBackfillState {
  state?: string | null
  detail?: string | null
  last_success_at?: string | null
  coverage_ratio?: number | null
  works_count?: number | null
}

export interface AuthorDetail {
  author: Author
  signal: AuthorSignal | null
  top_topics: AuthorTopTopic[]
  backfill: AuthorBackfillState | null
}

export function getAuthorDetail(authorId: string): Promise<AuthorDetail> {
  return api.get<AuthorDetail>(`/authors/${encodeURIComponent(authorId)}/detail`)
}

export interface OpenAlexWork {
  openalex_id?: string
  id?: string
  title?: string
  authors?: string | string[]
  year?: number | null
  publication_date?: string | null
  journal?: string
  doi?: string
  url?: string | null
  cited_by_count?: number | null
  abstract?: string
  already_in_db: boolean
  local_paper_id?: string
  local_status?: string
  local_rating?: number
}

export interface AuthorOpenAlexWorksPage {
  results: OpenAlexWork[]
  next_cursor: string | null
  total: number | null
  openalex_id: string
}

export function listAuthorOpenAlexWorks(
  authorId: string,
  opts?: { cursor?: string; perPage?: number },
): Promise<AuthorOpenAlexWorksPage> {
  const qs = new URLSearchParams()
  qs.set('cursor', opts?.cursor ?? '*')
  if (opts?.perPage) qs.set('per_page', String(opts.perPage))
  return api.get<AuthorOpenAlexWorksPage>(
    `/authors/${encodeURIComponent(authorId)}/openalex-works?${qs.toString()}`,
  )
}

export function saveOpenAlexWork(body: {
  openalex_id?: string | null
  doi?: string | null
  action: 'add' | 'like' | 'love' | 'dislike'
}): Promise<{ paper_id?: string; rating?: number; status?: string }> {
  return api.post('/library/import/search/save', body)
}

export function lookupAuthorByName(name: string): Promise<Author> {
  return api.get<Author>(`/authors/lookup?name=${encodeURIComponent(name)}`)
}

// ── Reading Status ──

export interface ReadingQueueResponse {
  reading: Publication[]
  done: Publication[]
  excluded: Publication[]
}

/** A single actionable reason why a paper is in the "Needs Attention" list.
 *  `code` is a stable enum the UI switches on for icons / button copy;
 *  `label` is the human-readable description; `action` is the suggested
 *  fix verb (or null when there's no canonical action). */
export interface AttentionReason {
  code: 'enrichment_stuck' | 'no_identifier' | 'no_abstract' | 'no_authors'
  label: string
  /** Concrete second-line context — actual failing field value, status
   *  string, or measured length. Renders muted under `label` so the user
   *  knows *what* is wrong (e.g. "Resolution status: not_openalex_resolved",
   *  "Missing: DOI + OpenAlex ID", "Abstract is only 8 chars"). */
  detail?: string | null
  action: 'rerun_enrichment' | 'find_identifier' | null
}

/** A paper in the Needs Attention list, augmented with the per-row
 *  attention_reasons array so the UI can render WHY each paper is
 *  flagged + an actionable fix verb. */
export interface NeedsAttentionPaper extends Publication {
  attention_reasons: AttentionReason[]
}

export interface LibraryWorkflowSummary {
  summary: {
    total_library: number
    avg_rating: number
    reading_count: number
    done_count: number
    excluded_count: number
    reading_list_count?: number
    collections_total: number
    uncollected_count: number
  }
  acquisition: {
    from_feed: number
    from_discovery: number
    from_import: number
    from_manual_or_other: number
  }
  source_mix: Array<{ source: string; count: number }>
  reading_mix: Array<{ status: string; count: number }>
  recent_additions: Publication[]
  next_up: Publication[]
  /** Library papers with concrete metadata gaps (no canonical identifier,
   *  missing abstract / authors, or OpenAlex enrichment stuck). Each
   *  paper carries its own `attention_reasons` array so the UI can show
   *  WHY it needs attention and offer an action button. */
  needs_attention: NeedsAttentionPaper[]
  needs_attention_count: number
  health?: {
    collection_coverage_pct: number
    tag_coverage_pct: number
    topic_coverage_pct: number
    rated_pct: number
    cleanup_flags: {
      uncollected: number
      untagged: number
      untopiced: number
    }
  }
  cleanup_guidance?: string[]
  structure?: {
    top_collections: Array<{ name: string; count: number }>
    top_tags: Array<{ name: string; count: number }>
    top_topics: Array<{ term: string; count: number }>
  }
}

export function updateReadingStatus(
  paperId: string,
  readingStatus: 'reading' | 'done' | 'excluded' | null
): Promise<Publication> {
  return api.patch<Publication>(`/library/papers/${paperId}/reading-status`, {
    reading_status: readingStatus,
  })
}

export function getReadingQueue(): Promise<ReadingQueueResponse> {
  return api.get<ReadingQueueResponse>('/library/reading-queue')
}

export function getLibraryWorkflowSummary(): Promise<LibraryWorkflowSummary> {
  return api.get<LibraryWorkflowSummary>('/library/workflow-summary')
}

export function getAuthorDossier(authorId: string): Promise<AuthorDossier> {
  return api.get<AuthorDossier>(`/authors/${encodeURIComponent(authorId)}/dossier`)
}

export function queueAuthorHistoryBackfill(authorId: string): Promise<{ status?: string; job_id?: string; message?: string }> {
  return api.post(`/authors/${encodeURIComponent(authorId)}/history-backfill`)
}

export function listPapers(params?: {
  scope?: 'all' | 'library' | 'background' | 'followed_corpus'
  status?: 'tracked' | 'library' | 'dismissed' | 'removed'
  addedFrom?: string
  openalexResolutionStatus?: string
  hasTopics?: boolean
  hasTags?: boolean
  authorId?: string
  search?: string
  semantic?: boolean
  year?: number
  minYear?: number
  maxYear?: number
  minCitations?: number
  order?: 'citations' | 'recent' | 'title' | 'rating' | 'authors' | 'journal' | 'status' | 'added_at'
  orderDir?: 'asc' | 'desc'
  limit?: number
  offset?: number
}): Promise<Publication[]> {
  const qs = new URLSearchParams()
  if (params?.scope) qs.set('scope', params.scope)
  if (params?.status) qs.set('status', params.status)
  if (params?.addedFrom) qs.set('added_from', params.addedFrom)
  if (params?.openalexResolutionStatus) qs.set('openalex_resolution_status', params.openalexResolutionStatus)
  if (params?.hasTopics != null) qs.set('has_topics', String(params.hasTopics))
  if (params?.hasTags != null) qs.set('has_tags', String(params.hasTags))
  if (params?.authorId) qs.set('author_id', params.authorId)
  if (params?.search) qs.set('search', params.search)
  if (params?.semantic) qs.set('semantic', 'true')
  if (params?.year != null) qs.set('year', String(params.year))
  if (params?.minYear != null) qs.set('min_year', String(params.minYear))
  if (params?.maxYear != null) qs.set('max_year', String(params.maxYear))
  if (params?.minCitations != null) qs.set('min_citations', String(params.minCitations))
  if (params?.order) qs.set('order', params.order)
  if (params?.orderDir) qs.set('order_dir', params.orderDir)
  if (params?.limit != null) qs.set('limit', String(params.limit))
  if (params?.offset != null) qs.set('offset', String(params.offset))
  const q = qs.toString()
  return api.get<Publication[]>(`/papers${q ? `?${q}` : ''}`)
}

export interface SemanticPaperSearchItem {
  paper: Publication
  score: number
  match_type: 'semantic' | string
  embedding_model: string
  query_model: string
}

export interface SemanticPaperSearchResponse {
  query: string
  scope: 'library' | 'all' | string
  count: number
  items: SemanticPaperSearchItem[]
  embedding_model: string
  query_model: string
}

export async function semanticSearchPapers(body: {
  query: string
  scope?: 'library' | 'all'
  limit?: number
}): Promise<SemanticPaperSearchResponse> {
  const envelope = await api.post<JobEnvelope>('/papers/semantic-search', body)
  return waitForJob<SemanticPaperSearchResponse>(envelope.job_id)
}

// ── Global Search ──

export interface SearchResult {
  id: string
  name: string
  type: 'paper' | 'author' | 'collection' | 'topic'
  url: string
  subtitle?: string
  status?: string
}

export interface GlobalSearchResponse {
  papers: SearchResult[]
  authors: SearchResult[]
  collections: SearchResult[]
  topics: SearchResult[]
}

export function globalSearch(query: string): Promise<GlobalSearchResponse> {
  return api.get<GlobalSearchResponse>(`/search?q=${encodeURIComponent(query)}`)
}

// ── Alerts types ──

export interface AlertRule {
  id: string
  name: string
  rule_type: 'author' | 'collection' | 'keyword' | 'topic' | 'similarity' | 'discovery_lens' | 'feed_monitor' | 'branch' | 'library_workflow'
  rule_config: Record<string, unknown>
  channels: string[]
  enabled: boolean
  created_at: string
}

export interface Alert {
  id: string
  name: string
  channels: string[]
  schedule: 'manual' | 'daily' | 'weekly' | string
  schedule_config?: Record<string, unknown>
  format: string
  enabled: boolean
  created_at: string
  last_evaluated_at?: string
  rules?: AlertRule[]
}

export interface AlertEvaluationResult {
  alert_id: string
  alert_name: string
  digest_id?: string
  digest_name?: string
  matched_rules?: number
  papers_found: number
  papers_new: number
  papers_sent: number
  papers_failed?: number
  channels: string[]
  channel_results?: Record<string, { status: string; error?: string | null }>
  trigger_source?: string
  dry_run: boolean
  papers?: Record<string, unknown>[]
}

export interface AlertHistory {
  id: string
  rule_id?: string
  alert_id?: string
  channel: string
  paper_id?: string
  publications?: string[]
  publication_count?: number
  sent_at: string
  status: 'sent' | 'failed' | 'pending' | 'empty' | 'skipped' | string
  message_preview?: string
  error_message?: string
}

export interface AlertAutomationTemplate {
  key: string
  category: 'author' | 'collection' | 'feed_monitor' | 'branch' | 'library_workflow' | string
  title: string
  description: string
  rationale?: string | null
  metrics: Record<string, number | string | boolean | null>
  rule: {
    name: string
    rule_type: AlertRule['rule_type']
    rule_config: Record<string, unknown>
    channels: string[]
    enabled: boolean
  }
  alert: {
    name: string
    channels: string[]
    schedule: string
    schedule_config?: Record<string, unknown>
    format: string
    enabled: boolean
  }
}

export interface FeedInboxPaper {
  id: string
  title: string
  authors?: string | null
  year?: number | null
  journal?: string | null
  abstract?: string | null
  url?: string | null
  doi?: string | null
  publication_date?: string | null
  status?: string
  rating?: number
  notes?: string | null
  added_at?: string | null
  added_from?: string | null
  reading_status?: string | null
  openalex_id?: string | null
  cited_by_count?: number
}

export interface FeedMatchedMonitor {
  monitor_id?: string | null
  monitor_type?: 'author' | 'query' | 'topic' | 'venue' | 'preprint' | 'branch' | string | null
  monitor_label?: string | null
}

export type FeedItemStatus = 'new' | 'add' | 'like' | 'love' | 'dislike'
export type FeedAction = 'add' | 'like' | 'love' | 'dislike'

export interface FeedInboxItem {
  id: string
  paper_id: string
  author_id: string
  author_name?: string | null
  matched_author_ids?: string[]
  matched_authors?: string[]
  matched_monitors?: FeedMatchedMonitor[]
  monitor_id?: string | null
  monitor_type?: 'author' | 'query' | 'topic' | 'venue' | 'preprint' | 'branch' | string | null
  monitor_label?: string | null
  fetched_at: string
  status: FeedItemStatus
  is_new?: boolean
  signal_value: number
  score_breakdown?: Record<string, any> | null
  paper?: FeedInboxPaper | null
}

export interface FeedMonitor {
  id: string
  monitor_type: 'author' | 'query' | 'topic' | 'venue' | 'preprint' | 'branch' | string
  monitor_key: string
  label: string
  enabled: boolean
  author_id?: string | null
  author_name?: string | null
  openalex_id?: string | null
  scholar_id?: string | null
  orcid?: string | null
  config?: Record<string, unknown> | null
  created_at?: string | null
  updated_at?: string | null
  last_checked_at?: string | null
  last_success_at?: string | null
  last_status?: string | null
  last_error?: string | null
  last_result?: Record<string, unknown> | null
  health: 'ready' | 'degraded' | 'disabled' | string
  health_reason?: string | null
}

// ── Discovery types ──

export interface ScoreSignalDetail {
  value: number
  weight: number
  weighted: number
  description?: string
}

export interface ScoreBreakdown {
  source_relevance?: ScoreSignalDetail
  topic_score?: ScoreSignalDetail
  text_similarity?: ScoreSignalDetail
  author_affinity?: ScoreSignalDetail
  journal_affinity?: ScoreSignalDetail
  recency_boost?: ScoreSignalDetail
  citation_quality?: ScoreSignalDetail
  feedback_adj?: ScoreSignalDetail
  preference_affinity?: ScoreSignalDetail
  usefulness_boost?: ScoreSignalDetail
  final_score?: number
  source_type?: string
  source_key?: string
  text_similarity_mode?: 'none' | 'semantic' | 'lexical' | 'hybrid'
  semantic_similarity_raw?: number
  lexical_similarity_raw?: number
  topic_match_mode?: 'none' | 'semantic' | 'keyword'
}

export interface RecommendationExplain {
  id: string
  title: string
  score: number
  source_type: string
  source_key: string
  breakdown: ScoreBreakdown | null
}

export interface Recommendation {
  id: string
  source_type: string
  source_key: string
  source_label?: string
  /** Source bucket for a sampled feedback-learning candidate. */
  source_bucket?: 'suggestion' | 'library' | 'corpus'
  source_api?: string | null
  paper_id?: string
  lens_id?: string
  branch_id?: string | null
  branch_label?: string | null
  branch_mode?: string | null
  recommended_title: string
  recommended_authors?: string
  recommended_abstract?: string
  recommended_url?: string
  recommended_doi?: string
  recommended_year?: number | null
  recommended_journal?: string
  score: number
  score_breakdown?: ScoreBreakdown | null
  seen: boolean
  liked: boolean
  dismissed: boolean
  created_at: string
}

export interface Lens {
  id: string
  name: string
  context_type: 'library_global' | 'collection' | 'topic_keyword' | 'tag'
  context_config?: Record<string, unknown> | null
  weights?: Record<string, number> | null
  branch_controls?: {
    temperature?: number | null
    pinned?: string[]
    muted?: string[]
    boosted?: string[]
  } | null
  created_at: string
  last_refreshed_at?: string | null
  is_active: boolean
  signal_count: number
  recommendation_count: number
  last_suggestion_set_id?: string | null
  last_ranker_version?: string | null
  last_retrieval_summary?: Record<string, unknown> | null
}

export interface LensSignal {
  id: number
  lens_id: string
  paper_id: string
  paper_title?: string
  signal_value: number
  source: string
  created_at: string
}

export interface LensRecommendation {
  id: string
  suggestion_set_id?: string | null
  lens_id?: string | null
  paper_id: string
  rank?: number | null
  score: number
  score_breakdown?: Record<string, unknown> | null
  user_action?: string | null
  action_at?: string | null
  source_type?: string | null
  source_api?: string | null
  source_key?: string | null
  branch_id?: string | null
  branch_label?: string | null
  branch_mode?: string | null
  created_at: string
  paper?: Publication | null
}

export interface LensBranchSeedSample {
  paper_id?: string | null
  title: string
  year?: number | null
  rating: number
}

export interface LensBranchItem {
  id: string
  label: string
  seed_count: number
  branch_score: number
  core_topics: string[]
  explore_topics: string[]
  direction_hint?: string | null
  sample_papers: LensBranchSeedSample[]
  control_state?: 'normal' | 'pinned' | 'boosted' | 'muted' | null
  is_pinned?: boolean
  is_boosted?: boolean
  is_muted?: boolean
  is_active?: boolean
  recommendation_count?: number
  avg_score?: number
  positive_rate?: number
  dismiss_rate?: number
  engagement_rate?: number
  unseen?: number
  unique_sources?: number
  auto_weight?: number
  auto_weight_reason?: string | null
}

export interface LensBranchPreview {
  lens_id: string
  lens_name?: string | null
  context_type: Lens['context_type']
  seed_count: number
  temperature: number
  generated_at: string
  branches: LensBranchItem[]
}

export interface ManualDiscoveryItem {
  openalex_id: string
  title: string
  authors: string
  abstract?: string
  year?: number | null
  publication_date?: string | null
  journal?: string
  doi?: string
  url?: string
  cited_by_count?: number
  paper_id?: string | null
  paper_status?: string | null
  in_library: boolean
  like_score: number
  score_breakdown?: Record<string, unknown>
}

export interface ManualDiscoverySearchResponse {
  query: string
  total: number
  items: ManualDiscoveryItem[]
}

// ── Discovery Settings types ──

export interface DiscoveryWeights {
  source_relevance: number
  topic_score: number
  text_similarity: number
  author_affinity: number
  journal_affinity: number
  recency_boost: number
  citation_quality: number
  feedback_adj: number
  preference_affinity: number
  usefulness_boost: number
}

export interface DiscoveryStrategies {
  related_works: boolean
  topic_search: boolean
  followed_authors: boolean
  coauthor_network: boolean
  citation_chain: boolean
  semantic_scholar: boolean
  branch_explorer: boolean
  taste_topics: boolean
  taste_authors: boolean
  taste_venues: boolean
  recent_wins: boolean
}

export interface DiscoveryLimits {
  max_results: number
  max_candidates_per_strategy: number
  recency_window_years: number
  feedback_decay_days_full: number
  feedback_decay_days_half: number
}

export interface DiscoverySchedule {
  refresh_interval_hours: number
  graph_maintenance_interval_hours: number
}

export interface DiscoveryCache {
  similarity_ttl_hours: number
}

export interface DiscoverySourcePolicy {
  enabled: boolean
  weight: number
}

export interface DiscoverySources {
  openalex: DiscoverySourcePolicy
  semantic_scholar: DiscoverySourcePolicy
  crossref: DiscoverySourcePolicy
  arxiv: DiscoverySourcePolicy
  biorxiv: DiscoverySourcePolicy
}

export interface DiscoveryBranchSettings {
  temperature: number
  max_clusters: number
  max_active_for_retrieval: number
  query_core_variants: number
  query_explore_variants: number
}

export interface DiscoveryMonitorDefaults {
  author_per_refresh: number
  search_limit: number
  search_temperature: number
  recency_years: number
  include_preprints: boolean
  semantic_scholar_bulk: boolean
}

export interface DiscoverySettings {
  weights: DiscoveryWeights
  strategies: DiscoveryStrategies
  limits: DiscoveryLimits
  schedule: DiscoverySchedule
  cache: DiscoveryCache
  sources: DiscoverySources
  branches: DiscoveryBranchSettings
  monitor_defaults: DiscoveryMonitorDefaults
  embedding_model: string
  recommendation_mode?: string
}

// ── Insights types ──

export interface InsightsData {
  summary: {
    total_publications: number
    total_citations: number
    total_authors: number
    total_countries: number
    total_topics: number
    total_institutions: number
    avg_citations_per_paper: number
    avg_papers_per_author: number
  }
  publications_by_year: Array<{
    year: number
    count: number
    citations: number
    avg_citations: number
  }>
  countries: Array<{ country_code: string; count: number }>
  top_institutions: Array<{
    institution_name: string
    country_code: string
    count: number
  }>
  top_topics: Array<{
    term: string
    count: number
    avg_citations: number
  }>
  top_journals: Array<{
    journal: string
    count: number
    citations: number
    avg_citations: number
  }>
  authors: Array<{
    id: string
    name: string
    papers: number
    citations: number
    h_index: number
    top_topic: string | null
  }>
  recommendations: {
    total: number
    seen: number
    liked: number
    dismissed: number
    engagement_rate: number
    by_lens?: Array<{
      lens_id: string
      count: number
      avg_score?: number
    }>
    by_source_type: Array<{
      source_type: string
      count: number
      avg_score: number
    }>
  }
  embeddings: {
    total_vectors: number
    model: string
    coverage_pct: number
  }
  library: {
    total_saved: number
    avg_rating: number
    total_collections: number
    total_followed_authors: number
  }
  // Stale-while-revalidate envelope flags. Backed by the materialised-view
  // layer (alma.application.materialized_views): when the underlying data
  // changes, the next GET enqueues a background rebuild and returns the
  // prior payload with `stale: true`. The page can show a "Refreshing…"
  // hint without blocking on recomputation.
  stale?: boolean
  rebuilding?: boolean
  computed_at?: string | null
}

export interface InsightsDiagnostics {
  generated_at?: string | null
  feed: {
    summary: {
      total_monitors: number
      ready_monitors: number
      degraded_monitors: number
      disabled_monitors: number
      author_monitors: number
      topic_monitors: number
      query_monitors: number
    }
    monitors: Array<{
      id: string
      label: string
      monitor_type: string
      author_id?: string | null
      author_name?: string | null
      health: string
      health_reason?: string | null
      last_checked_at?: string | null
      last_success_at?: string | null
      last_status?: string | null
      last_error?: string | null
      papers_found: number
      items_created: number
      yield_rate?: number | null
    }>
    recent_refreshes: Array<{
      job_id: string
      status: string
      finished_at?: string | null
      items_created: number
      papers_found: number
      monitors_total: number
      monitors_degraded: number
    }>
    scorecards?: Array<{
      id: string
      label: string
      score: number
      status: string
      summary: string
      detail: string
    }>
  }
  discovery: {
    summary: {
      total: number
      active_unseen: number
    }
    source_quality: Array<{
      source_type: string
      source_api: string
      count: number
      avg_score: number
      liked: number
      dismissed: number
      seen: number
      engagement_rate: number
    }>
    branch_quality: Array<{
      branch_id?: string | null
      branch_label: string
      count: number
      avg_score: number
      liked: number
      saved: number
      dismissed: number
      unseen: number
      engagement_rate: number
      positive_rate: number
      dismiss_rate: number
      recent_share: number
      dominant_mode: 'core' | 'explore' | string
      core_count: number
      explore_count: number
      unique_sources: number
      source_mix: Array<{
        source_type: string
        count: number
      }>
      quality_state: 'strong' | 'cool' | 'underexplored' | 'narrow' | 'monitor' | string
      tuning_hint: string
    }>
    branch_trends: Array<{
      branch_id?: string | null
      branch_label: string
      recent_7d_total: number
      prior_7d_total: number
      recent_7d_positive_rate: number
      prior_7d_positive_rate: number
      delta_positive_rate: number
      daily: Array<{
        date: string
        total: number
        positive: number
        dismissed: number
        positive_rate: number
      }>
    }>
    cold_start_topic_validation?: {
      total_runs: number
      validated_runs: number
      state_counts: Record<string, number>
      recent: Array<{
        lens_id: string
        lens_name: string
        created_at: string
        state: string
        seed_count: number
        external_results: number
        query?: string | null
      }>
    }
    source_diagnostics: Array<{
      source: string
      operations: number
      requests: number
      ok: number
      http_errors: number
      transport_errors: number
      retries: number
      avg_latency_ms: number
      status_counts: Record<string, number>
      top_endpoints: Array<{ path: string; count: number }>
      last_error?: string | null
    }>
    openalex_usage: {
      refreshes: number
      request_count: number
      retry_count: number
      rate_limited_events: number
      calls_saved_by_cache: number
      credits_used: number
      credits_remaining?: number | null
    }
    recent_refreshes: Array<{
      job_id: string
      status: string
      finished_at?: string | null
      new_recommendations: number
      total_recommendations: number
    }>
    scorecards?: Array<{
      id: string
      label: string
      score: number
      status: string
      summary: string
      detail: string
    }>
  }
  library: {
    workflow: {
      total_library: number
      reading_count: number
      done_count: number
      excluded_count: number
    }
    scorecards?: Array<{
      id: string
      label: string
      score: number
      status: string
      summary: string
      detail: string
    }>
  }
  authors: {
    summary: {
      total_rows: number
      tracked_authors: number
      provenance_only_authors: number
      ready_tracked: number
      degraded_tracked: number
      disabled_tracked: number
      bridge_gap_count: number
      background_corpus_papers?: number
      fresh_backfills?: number
      running_backfills?: number
      pending_backfills?: number
      stale_backfills?: number
      thin_backfills?: number
    }
    degraded: Array<{
      author_id?: string | null
      author_name?: string | null
      health_reason?: string | null
      last_error?: string | null
      last_checked_at?: string | null
    }>
    suggestions: Array<{
      key: string
      name: string
      suggestion_type: string
      score: number
      shared_followed_count: number
      negative_signal: number
    }>
    corpus_health?: Array<{
      author_id?: string | null
      author_name?: string | null
      state: string
      detail?: string | null
      background_publications: number
      coverage_ratio?: number | null
      last_success_at?: string | null
    }>
  }
  alerts: {
    summary: {
      total_alerts: number
      enabled_alerts: number
      total_rules: number
      active_alerts_30d: number
      sent_runs_30d: number
      failed_runs_30d: number
      empty_runs_30d: number
      skipped_runs_30d: number
      papers_sent_30d: number
      avg_papers_per_sent: number
    }
    top_alerts: Array<{
      alert_id?: string | null
      alert_name: string
      total_runs: number
      sent_runs: number
      failed_runs: number
      empty_runs: number
      skipped_runs: number
      papers_sent: number
      usefulness_score: number
    }>
    long_horizon?: {
      days: number
      summary: {
        active_alerts: number
        sent_runs: number
        failed_runs: number
        empty_runs: number
        skipped_runs: number
        papers_sent: number
        usefulness_score: number
        recent_30d_usefulness_score: number
        delta_vs_30d: number
      }
      weekly_trend: Array<{
        date: string
        sent: number
        failed: number
        empty: number
        skipped: number
        total: number
        publication_count: number
      }>
    }
  }
  feedback_learning: {
    summary: {
      total_interactions: number
      week_interactions: number
      streak_days: number
      topic_coverage: number
      source_diversity_7d: number
      recommendation_engagement_rate: number
      xp: number
      level: number
      background_corpus_papers?: number
      background_corpus_authors?: number
    }
    top_topics: Array<{ name?: string; topic?: string; score?: number; weight?: number; count?: number }>
    top_authors: Array<{ name?: string; author?: string; score?: number; weight?: number; count?: number }>
    next_actions: string[]
  }
  ai: {
    summary: {
      total_papers: number
      embeddings_ready: boolean
      embedding_provider: string
      embedding_model: string
      dominant_embedding_dimension: number
      embedding_dimension_variants: number
      embedding_coverage_pct: number
      missing_embeddings: number
      stale_embeddings: number
      up_to_date_embeddings: number
      recent_recommendations_analyzed: number
      hybrid_text_rate: number
      semantic_only_rate: number
      lexical_only_rate: number
      embedding_candidate_ready_rate: number
      low_similarity_rate: number
      compressed_similarity_rate: number
      avg_text_similarity: number
      avg_semantic_raw: number
      avg_semantic_support_raw: number
      avg_lexical_raw: number
      avg_lexical_term_raw: number
    }
    mode_breakdown: Record<string, number>
    capabilities: Array<{
      id: string
      label: string
      enabled: boolean
      ready: boolean
      usage_rate?: number | null
    }>
    recommendations: Array<{
      id: string
      label: string
      detail: string
      severity: string
    }>
  }
  operational: {
    summary: {
      issues_total: number
      critical_count: number
      warning_count: number
      healthy_checks: number
      embeddings_ready: boolean
      slack_configured: boolean
      degraded_monitors: number
      disabled_sources: number
      unhealthy_plugins: number
      recent_failed_operations_24h: number
    }
    states: Array<{
      id: string
      label: string
      severity: 'critical' | 'warning' | 'info' | string
      detail: string
      page: string
      params?: Record<string, string>
      targets?: Array<{
        id: string
        label: string
        kind: 'author' | 'monitor' | 'source' | 'alert' | 'plugin' | string
        action: 'repair_author' | 'backfill_author' | 'refresh_monitor' | 'enable_source' | 'evaluate_alert' | 'test_plugin' | 'compute_embeddings' | 'compute_stale_embeddings' | 'clear_similarity_cache' | string
        author_id?: string | null
        monitor_id?: string | null
        source?: string | null
        alert_id?: string | null
        plugin_name?: string | null
      }>
    }>
    plugins: Array<{
      name: string
      display_name: string
      is_configured: boolean
      is_healthy?: boolean | null
    }>
    disabled_sources: string[]
  }
  trends: {
    window_days: number
    feed_refresh_daily: Array<{
      date: string
      runs: number
      items_created: number
      papers_found: number
    }>
    discovery_refresh_daily: Array<{
      date: string
      runs: number
      new_recommendations: number
      total_recommendations: number
    }>
    recommendation_actions_daily: Array<{
      date: string
      seen: number
      liked: number
      dismissed: number
      saved: number
    }>
    alert_history_daily: Array<{
      date: string
      sent: number
      failed: number
      empty: number
      skipped: number
      total: number
    }>
    alert_history_weekly_90d?: Array<{
      date: string
      sent: number
      failed: number
      empty: number
      skipped: number
      total: number
      publication_count: number
    }>
    author_follows_daily?: Array<{
      date: string
      follows: number
    }>
    feedback_learning_daily?: Array<{
      date: string
      interactions: number
      feed_actions: number
      topic_tunes: number
      ratings: number
    }>
  }
  evaluation: {
    scorecards: Array<{
      id: string
      label: string
      score: number
      status: string
      summary: string
      detail: string
    }>
    recommended_actions: Array<{
      id: string
      title: string
      detail: string
      page: string
      params?: Record<string, string>
      priority: 'high' | 'medium' | 'low' | string
    }>
    automation_opportunities: AlertAutomationTemplate[]
  }
}

// ── Library Management types ──

export interface DbInfo {
  path: string
  size_bytes: number
  authors_count?: number
  publications_count?: number
  topics_count?: number
  institutions_count?: number
}

export interface BackupInfo {
  name: string
  created_at: string
  size_bytes: number
}

export interface LibraryInfo {
  database: DbInfo
  backups: BackupInfo[]
}

// ── AI types ──

export interface LocalModelInfo {
  key: string
  display_name: string
  description: string
  dimension: number
  hf_id: string
}

export interface AIProviderInfo {
  name: string
  display_name?: string
  model_display_name?: string
  provider_type?: string
  icon?: string
  description?: string
  canonical_model?: string
  dimension: number
  available: boolean
  active: boolean
  reason?: string
  local_models?: LocalModelInfo[]
  selected_model?: string
  device?: 'cuda' | 'cpu' | null
}

export function getInsightsDiagnostics(): Promise<InsightsDiagnostics> {
  return api.get<InsightsDiagnostics>('/insights/diagnostics')
}

// ── Per-section diagnostics endpoints ────────────────────────────────────
//
// The Diagnostics tab is split into eight cached sections on the
// backend (see `alma.api.routes.insights_diagnostics`). Each section
// is a fingerprint-based materialised view: a cache hit returns in
// ~1 ms, and the SWR envelope adds `stale` / `rebuilding` /
// `computed_at` so the UI can show a "Refreshing…" indicator while a
// background rebuild completes.
//
// The legacy `getInsightsDiagnostics()` is kept for callers that still
// want the full payload at once (Settings → Operational status,
// Alerts → automation chips). The Diagnostics tab itself uses these
// section fetchers so each card streams in independently with its
// own skeleton.

export const DIAGNOSTICS_SECTION_KEYS = [
  'feed',
  'discovery',
  'ai',
  'authors',
  'alerts',
  'feedback',
  'operational',
  'evaluation',
] as const

export type DiagnosticsSectionKey = (typeof DIAGNOSTICS_SECTION_KEYS)[number]

/** SWR metadata appended to every section payload by the MV layer. */
export type DiagnosticsSectionMeta = {
  stale?: boolean
  rebuilding?: boolean
  computed_at?: string | null
}

export type DiagnosticsFeedSection = DiagnosticsSectionMeta & {
  summary: InsightsDiagnostics['feed']['summary']
  monitors: InsightsDiagnostics['feed']['monitors']
  recent_refreshes: InsightsDiagnostics['feed']['recent_refreshes']
  feed_refresh_trend: InsightsDiagnostics['trends']['feed_refresh_daily']
}

export type DiagnosticsDiscoverySection = DiagnosticsSectionMeta & {
  summary: InsightsDiagnostics['discovery']['summary']
  source_quality: InsightsDiagnostics['discovery']['source_quality']
  branch_quality: InsightsDiagnostics['discovery']['branch_quality']
  branch_trends: InsightsDiagnostics['discovery']['branch_trends']
  cold_start_topic_validation?: InsightsDiagnostics['discovery']['cold_start_topic_validation']
  source_diagnostics: InsightsDiagnostics['discovery']['source_diagnostics']
  openalex_usage: InsightsDiagnostics['discovery']['openalex_usage']
  recent_refreshes: InsightsDiagnostics['discovery']['recent_refreshes']
  discovery_refresh_trend: InsightsDiagnostics['trends']['discovery_refresh_daily']
  recommendation_action_trend: InsightsDiagnostics['trends']['recommendation_actions_daily']
}

export type DiagnosticsAiSection = DiagnosticsSectionMeta & InsightsDiagnostics['ai']

export type DiagnosticsAuthorsSection = DiagnosticsSectionMeta & {
  summary: InsightsDiagnostics['authors']['summary']
  degraded: InsightsDiagnostics['authors']['degraded']
  suggestions: InsightsDiagnostics['authors']['suggestions']
  corpus_health?: InsightsDiagnostics['authors']['corpus_health']
  author_follow_trend: NonNullable<InsightsDiagnostics['trends']['author_follows_daily']>
}

export type DiagnosticsAlertsSection = DiagnosticsSectionMeta & {
  summary: InsightsDiagnostics['alerts']['summary']
  top_alerts: InsightsDiagnostics['alerts']['top_alerts']
  long_horizon?: InsightsDiagnostics['alerts']['long_horizon']
  alert_history_trend: InsightsDiagnostics['trends']['alert_history_daily']
  alert_history_weekly_90d?: InsightsDiagnostics['trends']['alert_history_weekly_90d']
}

export type DiagnosticsFeedbackSection = DiagnosticsSectionMeta & {
  summary: InsightsDiagnostics['feedback_learning']['summary']
  top_topics: InsightsDiagnostics['feedback_learning']['top_topics']
  top_authors: InsightsDiagnostics['feedback_learning']['top_authors']
  next_actions: InsightsDiagnostics['feedback_learning']['next_actions']
  feedback_learning_trend: NonNullable<InsightsDiagnostics['trends']['feedback_learning_daily']>
}

export type DiagnosticsOperationalSection =
  DiagnosticsSectionMeta & InsightsDiagnostics['operational']

export type DiagnosticsEvaluationSection = DiagnosticsSectionMeta & {
  scorecards: InsightsDiagnostics['evaluation']['scorecards']
  recommended_actions: InsightsDiagnostics['evaluation']['recommended_actions']
  automation_opportunities: InsightsDiagnostics['evaluation']['automation_opportunities']
  library_workflow: InsightsDiagnostics['library']['workflow']
}

export type DiagnosticsSectionPayload = {
  feed: DiagnosticsFeedSection
  discovery: DiagnosticsDiscoverySection
  ai: DiagnosticsAiSection
  authors: DiagnosticsAuthorsSection
  alerts: DiagnosticsAlertsSection
  feedback: DiagnosticsFeedbackSection
  operational: DiagnosticsOperationalSection
  evaluation: DiagnosticsEvaluationSection
}

export function getDiagnosticsSection<K extends DiagnosticsSectionKey>(
  section: K,
): Promise<DiagnosticsSectionPayload[K]> {
  return api.get<DiagnosticsSectionPayload[K]>(
    `/insights/diagnostics/sections/${section}`,
  )
}

/**
 * Evaluate (and send) an alert digest. Returns the AlertEvaluationResult
 * after polling the Activity envelope to completion.
 *
 * The endpoint now runs the full evaluation -- including the Slack send --
 * on the scheduler thread pool, so the user can also watch progress in the
 * Activity tab via `operation_key="alerts.evaluate:<id>"`.
 */
export async function evaluateAlert(alertId: string): Promise<AlertEvaluationResult> {
  const resp = await api.post<JobEnvelope | AlertEvaluationResult>(
    `/alerts/${encodeURIComponent(alertId)}/evaluate`,
  )
  if (isJobEnvelope(resp)) {
    if (resp.status === 'already_running') {
      // Surface an interpretable error for callers; matches what the user
      // sees if they double-click "Evaluate".
      throw new Error('Alert is already being evaluated; check Activity for progress.')
    }
    return waitForJob<AlertEvaluationResult>(resp.job_id, { timeoutMs: 120_000 })
  }
  // Backwards-compat path (shouldn't trigger after Phase D).
  return resp as AlertEvaluationResult
}

export interface SlackTestResult {
  ok: boolean
  message: string
  target?: string
  error?: string
}

/**
 * Test a messaging plugin connection.
 *
 * For Slack the backend returns an Activity envelope and runs the actual
 * `chat.postMessage` on the scheduler thread pool — so the test exercises
 * the exact code path that delivers real alerts. We unwrap the envelope
 * via `waitForJob`. For other plugins the legacy synchronous shape is
 * preserved.
 */
export async function testPluginConnection(
  pluginName: string,
): Promise<SlackTestResult> {
  const resp = await api.post<JobEnvelope | { success: boolean; message: string; timestamp: string }>(
    `/plugins/${encodeURIComponent(pluginName)}/test`,
  )
  if (isJobEnvelope(resp)) {
    if (resp.status === 'already_running') {
      return { ok: false, message: 'A Slack test is already in progress; try again in a moment.' }
    }
    return waitForJob<SlackTestResult>(resp.job_id, { timeoutMs: 30_000 })
  }
  // Legacy shape (non-slack plugins).
  return {
    ok: Boolean((resp as { success?: boolean }).success),
    message: String((resp as { message?: string }).message ?? ''),
  }
}

export function runGraphReferenceBackfill(): Promise<{ operation?: Record<string, unknown>; result?: Record<string, unknown> }> {
  return api.post('/graphs/reference-backfill?background=true')
}

export function refreshClusterLabels(body: {
  graph_type: 'paper_map' | 'author_network'
  scope?: 'library' | 'corpus'
}): Promise<{ status?: string; job_id?: string; operation_key?: string; message?: string }> {
  return api.post('/graphs/cluster-labels/refresh', {
    graph_type: body.graph_type,
    scope: body.scope ?? 'library',
  })
}

// Default scope is `followed` (~tens of authors). `followed_plus_library`
// adds every co-author of every saved Library paper — captures the
// "adjacent author" signal Discovery uses without sweeping the long tail
// of placeholder rows. `corpus` is still accepted by the API but no
// longer exposed in the Settings UI (lifecycle decision 2026-04-26 —
// soft-removed authors stay in the table for Discovery's negative-
// signal reads, so a literal "every row" sweep is misleading).
export type CorpusScope =
  | 'followed'
  | 'needs_metadata'
  | 'followed_plus_library'
  | 'library'
  | 'corpus'

export function refreshAllAuthors(body: {
  scope: CorpusScope
}): Promise<{ status?: string; job_id?: string; operation_key?: string; message?: string }> {
  // Bulk form of the popup "Refresh author" button — same pipeline,
  // iterated over every author in the selected scope.
  const qs = new URLSearchParams({ scope: body.scope, background: 'true' })
  return api.post(`/authors/deep-refresh-all?${qs.toString()}`)
}

export function garbageCollectOrphanAuthors(body?: {
  dryRun?: boolean
}): Promise<{ status?: string; job_id?: string; operation_key?: string; message?: string }> {
  // Soft-remove every author who is not followed and has no live
  // paper attachment. Eager triggers (paper-remove, unfollow) cover
  // the steady-state cases — this endpoint catches up with historical
  // drift. `dryRun=true` returns a preview without writing.
  const qs = new URLSearchParams({
    background: 'true',
    dry_run: String(Boolean(body?.dryRun)),
  })
  return api.post(`/authors/garbage-collect-orphans?${qs.toString()}`)
}

export function dedupPreprints(body: {
  scope: 'library' | 'corpus'
  limit?: number
}): Promise<{ status?: string; job_id?: string; operation_key?: string; message?: string }> {
  const qs = new URLSearchParams({ scope: body.scope, background: 'true' })
  if (body.limit !== undefined) qs.set('limit', String(body.limit))
  return api.post(`/papers/dedup-preprints?${qs.toString()}`)
}

export function rehydrateCorpusMetadata(body?: {
  limit?: number
  force?: boolean
}): Promise<{ status?: string; job_id?: string; operation_key?: string; message?: string }> {
  const qs = new URLSearchParams({ force: String(Boolean(body?.force)) })
  if (body?.limit !== undefined) qs.set('limit', String(body.limit))
  return api.post(`/papers/rehydrate-metadata?${qs.toString()}`)
}

export function rehydrateAuthorMetadata(body?: {
  limit?: number
  force?: boolean
}): Promise<{ status?: string; job_id?: string; operation_key?: string; message?: string }> {
  const qs = new URLSearchParams({
    background: 'true',
    force: String(Boolean(body?.force)),
  })
  if (body?.limit !== undefined) qs.set('limit', String(body.limit))
  return api.post(`/authors/rehydrate-metadata?${qs.toString()}`)
}

export interface AuthorAlternateProfile {
  author_id: string
  openalex_id: string
  display_name: string
}

export interface AuthorNeedsAttentionRow {
  author_id: string
  author_name: string
  openalex_id?: string | null
  status: string
  method?: string | null
  confidence: number
  reason_code: string
  reason: string
  /** Concrete second-line context — which resolver step ran, with what
   *  confidence, and whether at least an OpenAlex ID exists. Renders
   *  muted under `reason` so the user understands *why* manual help is
   *  needed before clicking the action. */
  reason_detail?: string | null
  /** For `reason_code='split_profiles'`: the alternate OpenAlex
   *  profiles the user has already followed for the same canonical
   *  name. The Review-profiles dialog renders these as a list with
   *  external links + merge / not-duplicate actions. */
  alt_profiles?: AuthorAlternateProfile[]
  /** For `reason_code='merge_conflict'`: opaque conflict-row id,
   *  the disagreeing field, and both candidate values. The
   *  resolution dialog reads these to render a "pick which value
   *  is correct" prompt. */
  conflict_id?: string
  conflict_field?: string
  conflict_primary_value?: string
  conflict_alt_value?: string
  alt_openalex_id?: string
  suggested_action: { code: string; label: string; hint: string }
  updated_at?: string | null
}

/** Merge alt OpenAlex profiles into a primary author. Each entry in
 *  `altAuthorIds` is an `authors.id` value (matches the
 *  `alt_profiles[].author_id` shape from /authors/needs-attention).
 *  Backend reassigns publication_authors, drops alt followed/monitor
 *  rows, soft-removes the alts, and records aliases so suggestion
 *  rail dedup never resurfaces them. */
export interface MergeProfilesResponse {
  primary_author_id: string
  primary_openalex_id: string
  alts_processed: number
  alts_skipped: number
  papers_reassigned: number
  papers_dropped_as_dup: number
  alt_openalex_ids: string[]
  alt_author_ids: string[]
}

export function mergeAuthorProfiles(
  primaryAuthorId: string,
  altAuthorIds: string[],
): Promise<MergeProfilesResponse> {
  return api.post(
    `/authors/${encodeURIComponent(primaryAuthorId)}/merge-profiles`,
    { alt_author_ids: altAuthorIds },
  )
}

/** ORCID-driven preventive alias discovery. Looks up the primary's
 *  ORCID on OpenAlex, queries every author profile sharing it, and
 *  returns the alias list so the user can review + merge. Read-only;
 *  no DB writes. Empty `aliases[]` when the primary has no ORCID or
 *  the ORCID is uniquely held. */
export interface DiscoverAliasesResponse {
  primary_author_id: string
  primary_display_name: string
  primary_openalex_id: string
  orcid: string | null
  aliases: Array<{
    openalex_id: string
    display_name: string
    institution: string
    works_count: number
  }>
}

export function discoverAuthorAliases(
  authorId: string,
): Promise<DiscoverAliasesResponse> {
  return api.post(`/authors/${encodeURIComponent(authorId)}/discover-aliases`)
}

/** Manual sweep — walks every followed author with an OpenAlex ID,
 *  ORCID-discovers split profiles, auto-merges followed-vs-followed
 *  clusters and records orphan aliases. Activity-enveloped. */
export function dedupAuthorsByOrcid(): Promise<{
  job_id?: string
  status?: string
  message?: string
}> {
  return api.post('/authors/dedup-by-orcid?background=true')
}

/** Resolve a merge-conflict row by picking primary, alt, or dismissing.
 *  Picking 'alt' overwrites the primary author's hard-identifier
 *  column (orcid / scholar_id / semantic_scholar_id) with the alt's
 *  value. */
export function resolveMergeConflict(
  conflictId: string,
  choice: 'primary' | 'alt' | 'dismiss',
): Promise<{ conflict_id: string; status: string; primary_author_id: string }> {
  return api.post(`/authors/conflicts/${encodeURIComponent(conflictId)}/resolve`, { choice })
}

/** Manual paste of authoritative identifiers — used by the
 *  Authors Needs-Attention 'Add identifier' dialog when the
 *  automatic resolver can't pick a canonical OpenAlex profile. */
export function setAuthorIdentifiers(
  authorId: string,
  body: { orcid?: string; openalex_id?: string; scholar_id?: string },
): Promise<{
  author_id: string
  openalex_id: string | null
  orcid: string | null
  scholar_id: string | null
  id_resolution_status: string
}> {
  return api.post(`/authors/${encodeURIComponent(authorId)}/identifiers`, body)
}

export function listAuthorsNeedsAttention(
  limit: number = 50,
): Promise<{ total: number; items: AuthorNeedsAttentionRow[] }> {
  return api.get(`/authors/needs-attention?limit=${limit}`)
}

export function clearDiscoverySimilarityCache(): Promise<{ success: boolean; deleted: number; operation?: Record<string, unknown> }> {
  return api.post('/discovery/similarity-cache/clear')
}

export function applyInsightsBranchAction(body: {
  branch_id: string
  action: 'pin' | 'boost' | 'mute' | 'reset' | 'cool'
}): Promise<{ branch_id: string; action: string; matched_lenses: number; updated_lenses: string[] }> {
  return api.post('/insights/discovery/branch-action', body)
}

export interface ActivityOperationResponse {
  status?: string
  job_id?: string
  operation_id?: string
  operation_key?: string
  activity_url?: string
  message?: string
  operation?: Record<string, unknown>
  result?: Record<string, unknown>
  total?: number
  processed?: number
}

export interface RepairAuthorResponse extends ActivityOperationResponse {
  author_id?: string
  repaired_fields?: string[]
  openalex_id?: string | null
  scholar_id?: string | null
  refreshed?: boolean
  refresh_result?: Record<string, unknown> | null
  resolution?: Record<string, unknown>
}

export function repairAuthor(authorId: string): Promise<RepairAuthorResponse> {
  return api.post(`/authors/${encodeURIComponent(authorId)}/repair`)
}

export function refreshFeedMonitor(monitorId: string): Promise<ActivityOperationResponse> {
  return api.post(`/feed/monitors/${encodeURIComponent(monitorId)}/refresh`)
}

export function getDiscoverySettings(): Promise<DiscoverySettings> {
  return api.get<DiscoverySettings>('/discovery/settings')
}

export function updateDiscoverySettings(body: DiscoverySettings): Promise<DiscoverySettings> {
  return api.put<DiscoverySettings>('/discovery/settings', body)
}

export function getAlertTemplates(): Promise<AlertAutomationTemplate[]> {
  return api.get<AlertAutomationTemplate[]>('/alerts/templates')
}

export interface AIDependency {
  installed: boolean
  version: string | null
  selected_installed?: boolean
  selected_version?: string | null
  runtime_importable?: boolean
  runtime_version?: string | null
  runtime_matches_selected?: boolean
}

export interface AIDependencyEnvironment {
  type: 'system' | 'venv' | 'uv' | 'conda' | 'miniconda' | 'miniforge' | string
  path: string
  valid: boolean
  message?: string | null
  detected_type?: string | null
  resolved_path?: string | null
  selected_python_executable?: string | null
  selected_python_version?: string | null
  using_fallback?: boolean
  fallback_reason?: string | null
  effective_python_executable?: string | null
  effective_python_version?: string | null
  backend_python_executable?: string | null
  backend_python_version?: string | null
  selected_site_packages?: string[]
  active_site_packages?: string[]
  python_version_match?: boolean
}

export interface AIFeatureStatus {
  id: string
  group: string
  label: string
  status: 'ready' | 'available' | 'fallback' | 'blocked' | 'empty' | 'off' | string
  dependency: string
  detail: string
  action?: string | null
}

export interface AIFeatureGroup {
  id: string
  label: string
  items: AIFeatureStatus[]
}

export interface AIStatus {
  providers: AIProviderInfo[]
  embeddings: {
    total: number
    coverage_pct: number
    model: string
    configured_model?: string
    up_to_date?: number
    up_to_date_pct?: number
    missing?: number
    stale?: number
    canonical_model?: string
    canonical_total?: number
    canonical_coverage_pct?: number
    downloaded_total?: number
    downloaded_coverage_pct?: number
    local_total?: number
    local_coverage_pct?: number
    unknown_total?: number
    unknown_coverage_pct?: number
    coverage_scope?: 'corpus' | string
    coverage_by_status?: Record<string, {
      total: number
      up_to_date: number
      missing: number
      canonical_total: number
      downloaded_total: number
      local_total: number
      unknown_total: number
    }>
    models?: Array<{
      model: string
      vectors: number
      coverage_pct: number
      last_created_at?: string | null
      stale?: number
      active?: boolean
      source?: 'semantic_scholar' | 'local' | 'openai' | 'unknown' | 'mixed' | string
      sources?: Record<string, number>
    }>
    s2_backfill?: {
      model: string
      total_missing: number
      eligible_missing: number
      ineligible_missing: number
      terminal_unmatched?: number
      terminal_missing_vector?: number
      terminal_lookup_error?: number
      terminal_error?: number
      local_compute_candidates?: number
      local_compute_blocked_missing_text?: number
      by_status?: Record<string, {
        total_missing: number
        eligible_missing: number
        ineligible_missing: number
      }>
    }
  }
  capability_tiers?: {
    tier1_embeddings: {
      enabled: boolean
      ready: boolean
      available: boolean
      active_provider: string
      active_model?: string
    }
  }
  features?: {
    summary: Record<string, number>
    groups: AIFeatureGroup[]
    items: AIFeatureStatus[]
  }
  local_model: string
  dependencies: Record<string, AIDependency>
  dependency_environment: AIDependencyEnvironment
  dependency_check_warning?: string | null
  dependency_setup_suggestions?: string[]
}

export interface AIConfig {
  provider?: string
  local_model?: string
  openai_api_key?: string
  python_env_type?: 'system' | 'venv' | 'uv' | 'conda' | 'miniconda' | 'miniforge' | string
  python_env_path?: string
}

// ── Graph types ──

export interface GraphNode {
  id: string
  name: string
  x: number
  y: number
  cluster_id?: number
  color?: string
  size: number
  node_type?: string
  metadata: Record<string, unknown>
}

export interface GraphEdge {
  source: string
  target: string
  weight: number
}

export interface GraphData {
  nodes: GraphNode[]
  edges: GraphEdge[]
  metadata: Record<string, unknown>
}

// ── Import types ──

export interface ImportResult {
  total: number
  imported: number
  skipped: number
  failed: number
  errors: string[]
  items: Record<string, unknown>[]
}

/**
 * Activity envelope returned when an import is queued as a background job.
 * Matches the Feed/Authors background-job contract in `docs/BACKGROUND_JOBS.md`.
 */
export interface ImportOperationEnvelope {
  status: 'queued' | 'running' | 'already_running' | 'scheduled'
  job_id: string
  operation_id?: string
  operation_key?: string
  activity_url?: string
  message?: string
  total?: number
}

export type ImportResponse = ImportResult | ImportOperationEnvelope

/** Narrow an import response into the queued-envelope branch. */
export function isImportQueued(response: ImportResponse): response is ImportOperationEnvelope {
  return typeof (response as ImportOperationEnvelope).job_id === 'string'
    && (response as ImportOperationEnvelope).job_id.length > 0
}

export interface ZoteroCollection {
  key: string
  name: string
  num_items: number
  parent?: string | null
}

export interface UnresolvedImportedPublication {
  id: string
  title: string
  doi?: string
  url?: string
  openalex_id?: string
  year?: number | null
  authors?: string
  fetched_at?: string | null
  /**
   * Paper-level OpenAlex-resolution state. Phase C (P6) requires the
   * UI to distinguish "never tried" (`pending` / `pending_enrichment`
   * / empty) from "tried and no canonical OpenAlex hit"
   * (`not_openalex_resolved`). Per P1.5 the `failed` state is also
   * possible. Friendly mapping lives in `ImportsTab.tsx`.
   */
  openalex_resolution_status?:
    | ''
    | 'pending'
    | 'pending_enrichment'
    | 'unresolved'
    | 'not_openalex_resolved'
    | 'failed'
    | string  // tolerate unknown legacy values
  openalex_resolution_reason?: string
  openalex_resolution_updated_at?: string
}

export interface UnresolvedImportedListResponse {
  total: number
  items: UnresolvedImportedPublication[]
}

export interface ResolveImportedOpenAlexResponse {
  status: string
  job_id?: string
  total?: number
  message?: string
  summary?: Record<string, unknown>
}

// ── Similarity types ──

export interface SimilarityResultItem {
  /** Real `papers.id` when the candidate matches a local corpus row
   *  (dense fallback). Absent for network-sourced candidates. */
  paper_id?: string | null
  /** Which retrieval lane surfaced the candidate. Always present on
   *  post-T2 responses; may be absent on legacy cached rows. */
  source_type?: string
  source_key?: string
  title: string
  authors?: string
  url?: string
  doi?: string
  score: number
  score_breakdown?: ScoreBreakdown | null
  year?: number
}

export interface SimilarityChannelStat {
  name: string
  fetched: number
  skipped_as_existing: number
  error?: string | null
}

export interface SimilarityResponse {
  results: SimilarityResultItem[]
  cached: boolean
  cache_key?: string
  seed_count: number
  /** Per-channel retrieval stats (OpenAlex related / OpenAlex citing /
   *  Semantic Scholar recommend-for-paper). Empty for cached responses
   *  older than the T2 rollout. */
  channels?: SimilarityChannelStat[]
  /** True when every network channel returned zero usable candidates
   *  and the response was populated by the SPECTER2 centroid fallback.
   *  Discovery UI surfaces this so the user understands why the results
   *  look different from a typical seeded refresh. */
  dense_fallback_used?: boolean
}

export interface ResetFeedbackLearningResponse {
  success: boolean
  cleared: Record<string, number>
  total_rows_cleared: number
}

export function resetFeedbackLearning(): Promise<ResetFeedbackLearningResponse> {
  return api.post('/feedback/reset')
}

export interface ResetEmbeddingsResponse {
  success: boolean
  cleared: Record<string, number>
  total_rows_cleared: number
}

/** Wipes every cached SPECTER2 vector so the next AI run / S2 backfill
 *  repopulates from scratch. Library papers and feedback survive — only
 *  the embedding tables and their per-paper fetch markers are cleared. */
export function resetEmbeddings(): Promise<ResetEmbeddingsResponse> {
  return api.post('/library-mgmt/embeddings/reset')
}

/**
 * Track a lightweight interaction event (fire-and-forget).
 * These are passive signals stored for future analysis — they do NOT
 * update preference profiles or block the UI.
 */
export function trackInteraction(
  eventType: 'external_link_click' | 'abstract_engagement' | 'search_query',
  paperId?: string | null,
  context?: Record<string, unknown>,
): void {
  api.post('/feedback/track', {
    event_type: eventType,
    paper_id: paperId ?? null,
    context: context ?? null,
  }).catch(() => {}) // Silently ignore errors — tracking must never affect UX
}

// ── Tag Suggestion types ──

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

export interface TagMergeSuggestion {
  source_tag_id: string
  source_tag: string
  target_tag_id: string
  target_tag: string
  confidence: number
  reason: string
}

// ── Tag Suggestion API functions ──

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

export function getTagMergeSuggestions(
  limit = 25,
  minConfidence = 0.8,
): Promise<TagMergeSuggestion[]> {
  const qs = new URLSearchParams()
  qs.set('limit', String(limit))
  qs.set('min_confidence', String(minConfidence))
  return api.get<TagMergeSuggestion[]>(`/tags/merge-suggestions?${qs.toString()}`)
}

export function mergeTags(
  sourceTagId: string,
  targetTagId: string,
): Promise<{
  success: boolean
  source_tag_id: string
  source_tag: string
  target_tag_id: string
  target_tag: string
}> {
  return api.post('/tags/merge', {
    source_tag_id: sourceTagId,
    target_tag_id: targetTagId,
  })
}

/**
 * Discover papers similar to a set of paper IDs.
 *
 * Cached results are returned inline from the backend; a cache miss returns a
 * job envelope which we auto-resolve via `waitForJob` so callers see a uniform
 * `SimilarityResponse` either way.
 */
export async function discoverSimilar(
  paperIds: string[],
  limit = 20,
  force = false,
): Promise<SimilarityResponse> {
  const resp = await api.post<SimilarityResponse | JobEnvelope>('/discovery/similar', {
    paper_ids: paperIds,
    limit,
    force,
  })
  if (isJobEnvelope(resp)) {
    const result = await waitForJob<{ similarity: SimilarityResponse }>(resp.job_id)
    return result.similarity
  }
  return resp
}

export function listLenses(): Promise<Lens[]> {
  return api.get<Lens[]>('/lenses')
}

export function createLens(body: {
  name: string
  context_type: Lens['context_type']
  context_config?: Record<string, unknown>
  weights?: Record<string, number>
}): Promise<Lens> {
  return api.post<Lens>('/lenses', body)
}

export function updateLens(
  lensId: string,
  body: {
    name?: string
    context_config?: Record<string, unknown>
    weights?: Record<string, number>
    branch_controls?: {
      temperature?: number | null
      pinned?: string[]
      muted?: string[]
      boosted?: string[]
    }
    is_active?: boolean
  },
): Promise<Lens> {
  return api.put<Lens>(`/lenses/${encodeURIComponent(lensId)}`, body)
}

export function deleteLens(lensId: string): Promise<{ success: boolean }> {
  return api.delete<{ success: boolean }>(`/lenses/${encodeURIComponent(lensId)}`)
}

export function refreshLens(lensId: string, limit = 50): Promise<JobEnvelope> {
  return api.post<JobEnvelope>(
    `/lenses/${encodeURIComponent(lensId)}/refresh?limit=${encodeURIComponent(String(limit))}`,
  )
}

export function listLensRecommendations(
  lensId: string,
  params?: { limit?: number; offset?: number },
): Promise<LensRecommendation[]> {
  const qs = new URLSearchParams()
  if (params?.limit != null) qs.set('limit', String(params.limit))
  if (params?.offset != null) qs.set('offset', String(params.offset))
  const q = qs.toString()
  return api.get<LensRecommendation[]>(`/lenses/${encodeURIComponent(lensId)}/recommendations${q ? `?${q}` : ''}`)
}

export function previewLensBranches(
  lensId: string,
  params?: { max_branches?: number; temperature?: number },
): Promise<LensBranchPreview> {
  const qs = new URLSearchParams()
  if (params?.max_branches != null) qs.set('max_branches', String(params.max_branches))
  if (params?.temperature != null) qs.set('temperature', String(params.temperature))
  const q = qs.toString()
  return api.get<LensBranchPreview>(`/lenses/${encodeURIComponent(lensId)}/branches${q ? `?${q}` : ''}`)
}

export function explainRecommendation(recId: string): Promise<{
  id: string
  title: string
  score: number
  explanation?: string | null
  breakdown?: Record<string, unknown> | null
}> {
  return api.get(`/discovery/recommendations/${encodeURIComponent(recId)}/explain`)
}

export function listLensSignals(
  lensId: string,
  limit = 100,
): Promise<{ lens_id: string; signals: LensSignal[] }> {
  return api.get(`/lenses/${encodeURIComponent(lensId)}/signals?limit=${encodeURIComponent(String(limit))}`)
}

/**
 * Search OpenAlex via the manual-discovery endpoint.
 *
 * The backend now runs the OpenAlex round-trip as an Activity-tracked job so
 * the request returns immediately with a `JobEnvelope`; we auto-poll for the
 * completed result so callers still see a uniform `ManualDiscoverySearchResponse`.
 */
export async function manualDiscoverySearch(
  query: string,
  limit = 20,
): Promise<ManualDiscoverySearchResponse> {
  const envelope = await api.post<JobEnvelope>('/discovery/manual-search', { query, limit })
  if (!isJobEnvelope(envelope)) {
    return envelope as unknown as ManualDiscoverySearchResponse
  }
  return waitForJob<ManualDiscoverySearchResponse>(envelope.job_id)
}

export function manualDiscoveryAdd(body: {
  openalex_id?: string
  doi?: string
  link?: string
  title?: string
  query?: string
}): Promise<Publication> {
  return api.post('/discovery/manual-search/add', body)
}

// ── Import Phase C: online source search ──

export interface OnlineSearchItem {
  openalex_id: string
  title: string
  authors: string
  abstract: string
  year: number | null
  publication_date: string | null
  journal: string
  doi: string
  url: string
  cited_by_count: number
  paper_id: string | null
  paper_status: string | null
  in_library: boolean
  like_score?: number
  /** Per-signal contributions to `like_score`, computed by the same
   *  `score_candidate` path Discovery uses. Same shape as
   *  `ScoreBreakdown`. Used by the Find & Add results to render the
   *  per-result inline "why" chip row (no sort dropdown — ranking is
   *  always personal-fit; the chips explain it). */
  score_breakdown?: ScoreBreakdown | null
  /** External sources that returned this paper (e.g. ["openalex", "semantic_scholar"]). */
  sources?: string[]
  /** Topics array passed through for scoring + display. */
  topics?: unknown[]
}

export interface OnlineSearchResponse {
  query: string
  filters: { year_min: number | null; year_max: number | null }
  total: number
  items: OnlineSearchItem[]
}

export interface OnlineSearchSaveResponse {
  paper_id: string
  action: 'add' | 'like' | 'love' | 'dislike'
  rating: number
  status: string
  match_source: string
  added_from: string
  title: string
}

export function onlineImportSearch(body: {
  query: string
  limit?: number
  year_min?: number
  year_max?: number
}): Promise<OnlineSearchResponse> {
  return api.post('/library/import/search', body)
}

/**
 * One author candidate from `/library/import/search/authors` — used by
 * the Find & Add author scope (when the query starts with `author:`).
 * Lightweight summary so the UI can render the result list without
 * fetching the full dossier; the dossier is resolved on follow.
 */
export interface OnlineAuthorSearchResult {
  openalex_id: string
  name: string
  orcid?: string | null
  institution?: string | null
  works_count: number
  cited_by_count: number
  h_index: number
  i10_index: number
  top_topics: string[]
  already_followed: boolean
}

export function onlineAuthorSearch(body: {
  query: string
  limit?: number
}): Promise<OnlineAuthorSearchResult[]> {
  return api.post('/library/import/search/authors', body)
}

/**
 * Streamed Find & Add events from `/library/import/search/stream`.
 * Backend emits NDJSON; the consumer reads them in order:
 *
 * 1. `source_pending` for each enabled source → render skeletons.
 * 2. `source_partial` per source as its lane returns → swap that
 *    skeleton for cheap library-state-decorated cards.
 * 3. `source_timeout` / `source_error` for lanes that didn't make
 *    the 8 s deadline.
 * 4. `final` once all lanes resolved → swap the union of partials
 *    for the personal-fit ranked + dedup'd list.
 */
export type FindAndAddStreamEvent =
  | { type: 'source_pending'; source: string }
  | { type: 'source_partial'; source: string; items: OnlineSearchItem[]; ms?: number }
  | { type: 'source_timeout'; source: string; ms?: number }
  | { type: 'source_error'; source: string; error: string; ms?: number }
  | { type: 'final'; items: OnlineSearchItem[]; total: number }
  | { type: 'error'; error: string }

export async function* onlineImportSearchStream(
  body: {
    query: string
    limit?: number
    year_min?: number
    year_max?: number
  },
  signal?: AbortSignal,
): AsyncGenerator<FindAndAddStreamEvent, void, void> {
  const response = await fetch(`${BASE_URL}/library/import/search/stream`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
    signal,
  })
  if (!response.ok || !response.body) {
    throw new ApiError(response.status, response.statusText)
  }
  const reader = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  while (true) {
    const { value, done } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    let nl: number
    while ((nl = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, nl).trim()
      buffer = buffer.slice(nl + 1)
      if (!line) continue
      try {
        yield JSON.parse(line) as FindAndAddStreamEvent
      } catch (err) {
        console.warn('Failed to parse stream event line:', line, err)
      }
    }
  }
  // Flush any tail line that didn't end in a newline.
  const tail = buffer.trim()
  if (tail) {
    try {
      yield JSON.parse(tail) as FindAndAddStreamEvent
    } catch (err) {
      console.warn('Failed to parse tail stream event:', tail, err)
    }
  }
}

export function onlineImportSave(body: {
  action: 'add' | 'like' | 'love' | 'dislike'
  openalex_id?: string
  doi?: string
  link?: string
  title?: string
  query?: string
  /** Full candidate dict — fallback when OpenAlex alone cannot resolve the paper
   *  (e.g. a Semantic Scholar-only result). Pass the item as returned by
   *  `onlineImportSearch`. */
  candidate?: OnlineSearchItem
}): Promise<OnlineSearchSaveResponse> {
  return api.post('/library/import/search/save', body)
}

export function saveRecommendation(
  recId: string,
  rating?: number,
): Promise<{ id: string; save: boolean }> {
  return api.post(`/discovery/recommendations/${encodeURIComponent(recId)}/save`, {
    rating,
  })
}

export function likeRecommendation(
  recId: string,
  rating = 4,
): Promise<{ id: string; like: boolean }> {
  return api.post(`/discovery/recommendations/${encodeURIComponent(recId)}/like`, {
    rating,
  })
}

export function readRecommendation(recId: string): Promise<{ id: string; read: boolean }> {
  return api.post(`/discovery/recommendations/${encodeURIComponent(recId)}/read`)
}

export function dismissRecommendation(recId: string): Promise<{ id: string; dismiss: boolean }> {
  return api.post(`/discovery/recommendations/${encodeURIComponent(recId)}/dismiss`)
}

/**
 * Record a negative signal on a Discovery recommendation without
 * hiding the paper system-wide. Distinct from `dismissRecommendation`:
 *
 * - `dismiss` hides the Discovery suggestion and writes a stronger
 *   negative signal with long cooldown.
 * - `dislike` only writes the feedback event + 1-star rating. The
 *   recommendation stays visible; use Dismiss to hide it.
 */
export function dislikeRecommendation(recId: string): Promise<{ id: string; dislike: boolean }> {
  return api.post(`/discovery/recommendations/${encodeURIComponent(recId)}/dislike`)
}

export function listFeedInbox(params?: {
  status?: FeedItemStatus
  sort?: 'chronological' | 'relevance'
  limit?: number
  offset?: number
  /** Restrict to items within the last N days. Defaults to 60 server-side. */
  since_days?: number
}): Promise<{ items: FeedInboxItem[]; total: number }> {
  const qs = new URLSearchParams()
  if (params?.status) qs.set('status', params.status)
  if (params?.sort) qs.set('sort', params.sort)
  if (params?.limit != null) qs.set('limit', String(params.limit))
  if (params?.offset != null) qs.set('offset', String(params.offset))
  if (params?.since_days != null) qs.set('since_days', String(params.since_days))
  const q = qs.toString()
  return api.get(`/feed${q ? `?${q}` : ''}`)
}

export function refreshFeedInbox(): Promise<ActivityOperationResponse> {
  return api.post('/feed/refresh')
}

export interface FeedStatusResponse {
  last_refresh_at: string | null
  new_count?: number
}

export function getFeedStatus(): Promise<FeedStatusResponse> {
  return api.get('/feed/status')
}

export interface DiscoveryStatusResponse {
  last_refresh_at: string | null
}

export function getDiscoveryStatus(): Promise<DiscoveryStatusResponse> {
  return api.get('/discovery/status')
}

export function listFeedMonitors(): Promise<FeedMonitor[]> {
  return api.get('/feed/monitors')
}

export function createFeedMonitor(body: {
  monitor_type: 'query' | 'topic' | 'venue' | 'preprint' | 'branch'
  query: string
  label?: string
  config?: Record<string, unknown>
}): Promise<FeedMonitor> {
  return api.post('/feed/monitors', body)
}

export function updateFeedMonitor(
  monitorId: string,
  body: {
    query?: string
    label?: string
    enabled?: boolean
    config?: Record<string, unknown>
  },
): Promise<FeedMonitor> {
  return api.put(`/feed/monitors/${encodeURIComponent(monitorId)}`, body)
}

export function deleteFeedMonitor(monitorId: string): Promise<{ success: boolean; monitor_id: string }> {
  return api.delete(`/feed/monitors/${encodeURIComponent(monitorId)}`)
}

export function feedAdd(feedItemId: string): Promise<{ item: FeedInboxItem | null }> {
  return api.post(`/feed/${encodeURIComponent(feedItemId)}/add`)
}

export function feedLike(feedItemId: string): Promise<{ item: FeedInboxItem | null }> {
  return api.post(`/feed/${encodeURIComponent(feedItemId)}/like`)
}

export function feedLove(feedItemId: string): Promise<{ item: FeedInboxItem | null }> {
  return api.post(`/feed/${encodeURIComponent(feedItemId)}/love`)
}

export function feedDislike(feedItemId: string): Promise<{ item: FeedInboxItem | null }> {
  return api.post(`/feed/${encodeURIComponent(feedItemId)}/dislike`)
}

export function feedBulkAction(
  feedItemIds: string[],
  action: FeedAction,
): Promise<{ results: Array<{ feed_item_id: string; item: FeedInboxItem | null }> }> {
  return api.post('/feed/bulk-action', {
    feed_item_ids: feedItemIds,
    action,
  })
}

// ── Import API helpers ──

/**
 * Upload a .bib file for import.
 *
 * Defaults to background execution: the request returns a queued envelope
 * immediately and Activity tracks progress. Callers can force inline behavior
 * with ``background=false`` (used by tests and minimal environments).
 * Uses multipart/form-data so we bypass the default JSON headers.
 */
export async function importBibtexFile(
  file: File,
  collectionName?: string,
  background: boolean = true,
): Promise<ImportResponse> {
  const form = new FormData()
  form.append('file', file)
  if (collectionName) form.append('collection_name', collectionName)

  const response = await fetch(
    `${BASE_URL}/library/import/bibtex?background=${background ? 'true' : 'false'}`,
    {
      method: 'POST',
      body: form,
      // Do NOT set Content-Type -- browser sets multipart boundary automatically
    },
  )
  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: response.statusText }))
    throw new ApiError(response.status, error.detail || response.statusText)
  }
  return response.json()
}

/** Import BibTeX from a pasted text string. */
export function importBibtexText(
  content: string,
  collectionName?: string,
  background: boolean = true,
): Promise<ImportResponse> {
  return api.post<ImportResponse>(
    `/library/import/bibtex/text?background=${background ? 'true' : 'false'}`,
    {
      content,
      collection_name: collectionName,
    },
  )
}

/** Import papers from a Zotero library. */
export function importZotero(
  params: {
    library_id: string
    api_key?: string
    library_type?: string
    collection_key?: string | null
    collection_name?: string
  },
  background: boolean = true,
): Promise<ImportResponse> {
  return api.post<ImportResponse>(
    `/library/import/zotero?background=${background ? 'true' : 'false'}`,
    params,
  )
}

/** List collections in a Zotero library (for the UI picker). */
export function listZoteroCollections(params: {
  library_id: string
  api_key?: string
  library_type?: string
}): Promise<ZoteroCollection[]> {
  return api.post<ZoteroCollection[]>('/library/import/zotero/collections', {
    library_id: params.library_id,
    api_key: params.api_key,
    library_type: params.library_type || 'user',
  })
}

/** Upload a Zotero RDF export file for import. */
export async function importZoteroRdfFile(
  file: File,
  collectionName?: string,
  background: boolean = true,
): Promise<ImportResponse> {
  const form = new FormData()
  form.append('file', file)
  if (collectionName) form.append('collection_name', collectionName)

  const response = await fetch(
    `${BASE_URL}/library/import/zotero/rdf?background=${background ? 'true' : 'false'}`,
    {
      method: 'POST',
      body: form,
    },
  )
  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: response.statusText }))
    throw new ApiError(response.status, error.detail || response.statusText)
  }
  return response.json()
}

/** List imported publications that are not OpenAlex-resolved yet. */
export function listUnresolvedImportedPublications(limit = 200): Promise<UnresolvedImportedListResponse> {
  return api.get<UnresolvedImportedListResponse>(`/library/import/unresolved?limit=${limit}`)
}

/** Resolve unresolved imported publications via OpenAlex. */
export function resolveImportedPublicationsOpenAlex(params?: {
  unresolved_only?: boolean
  limit?: number
  background?: boolean
  items?: Array<{ paper_id: string }>
}): Promise<ResolveImportedOpenAlexResponse> {
  return api.post<ResolveImportedOpenAlexResponse>('/library/import/resolve-openalex', {
    unresolved_only: params?.unresolved_only ?? true,
    limit: params?.limit ?? 1000,
    background: params?.background ?? true,
    items: params?.items ?? [],
  })
}

/** Enrich unresolved imported publications via OpenAlex metadata. */
export function enrichImportedPublications(
  background = true,
): Promise<{ status?: string; job_id?: string; message?: string }> {
  return api.post(`/library/import/enrich?background=${background ? 'true' : 'false'}`)
}

// ── Reports types ──

export interface WeeklyBriefData {
  report_type: 'weekly_brief'
  period: { from: string; to: string }
  new_papers: number
  total_library: number
  rated_this_week: number
  trending_topics: Array<{ topic: string; papers: number }>
  active_authors: Array<{ name: string; new_papers: number }>
  recommendations: { total: number; liked: number; dismissed: number }
}

export interface CollectionIntelligenceData {
  report_type: 'collection_intelligence'
  total_collections: number
  collections: Array<{
    id: string
    name: string
    color: string
    paper_count: number
    avg_citations: number
    avg_rating: number
    last_added: string | null
    top_topics: Array<{ topic: string; papers: number }>
    year_range: { min: number | null; max: number | null }
    topic_diversity: number
  }>
}

export interface TopicDriftData {
  report_type: 'topic_drift'
  windows: Array<{
    label: string
    from_year: number
    to_year: number
    top_topics: Array<{ topic: string; papers: number }>
  }>
  emerging_topics: string[]
  fading_topics: string[]
}

export interface SignalImpactData {
  report_type: 'signal_impact'
  liked_count: number
  dismissed_count: number
  signals: Array<{
    signal: string
    liked_avg: number
    dismissed_avg: number
    delta: number
    impact: 'positive' | 'negative' | 'neutral'
  }>
}

export function getWeeklyBrief(): Promise<WeeklyBriefData> {
  return api.get<WeeklyBriefData>('/reports/weekly-brief')
}

export function getCollectionIntelligence(): Promise<CollectionIntelligenceData> {
  return api.get<CollectionIntelligenceData>('/reports/collection-intelligence')
}

export function getTopicDrift(): Promise<TopicDriftData> {
  return api.get<TopicDriftData>('/reports/topic-drift')
}

export function getSignalImpact(): Promise<SignalImpactData> {
  return api.get<SignalImpactData>('/reports/signal-impact')
}

// ── Bootstrap ──

export interface BootstrapData {
  library: {
    papers: number
    candidates: number
    authors: number
    followed_authors: number
    collections: number
    tags: number
  }
  feed: { unread: number }
  discovery: { active_lenses: number; pending_recommendations: number }
  alerts: { active: number }
}

export function getBootstrap(): Promise<BootstrapData> {
  return api.get<BootstrapData>('/bootstrap')
}
