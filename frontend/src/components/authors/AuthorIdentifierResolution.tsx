import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2, Search } from 'lucide-react'

import { api, type Author } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { useToast, errorToast } from '@/hooks/useToast'
import { invalidateQueries } from '@/lib/queryHelpers'

interface OpenAlexCandidate {
  openalex_id: string
  display_name: string
  score: number
  institution?: string
}

interface ScholarCandidate {
  scholar_id: string
  display_name: string
  score: number
  affiliation?: string
  source?: string
  scholar_url?: string
}

interface CandidatesResponse {
  openalex: OpenAlexCandidate[]
  scholar: ScholarCandidate[]
  scholar_manual_search_enabled?: boolean
  scholar_auto_scrape_enabled?: boolean
}

interface AuthorIdentifierResolutionProps {
  author: Author
}

/**
 * Identifier diagnostics for one author — OpenAlex & Scholar candidate
 * lookup, manual Google Scholar scrape, and confirm. Formerly lived inline
 * on the Authors page as a separate dialog; now it's a tab inside
 * ``AuthorDetailPanel`` per the Authors page rehaul (decision 3).
 */
export function AuthorIdentifierResolution({ author }: AuthorIdentifierResolutionProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [openalexId, setOpenalexId] = useState(author.openalex_id ?? '')
  const [scholarId, setScholarId] = useState(author.scholar_id ?? '')
  const [manualScholarCandidates, setManualScholarCandidates] = useState<ScholarCandidate[]>([])

  const candidateQuery = useQuery({
    queryKey: ['author-id-candidates', author.id],
    queryFn: () =>
      api.get<CandidatesResponse>(
        `/authors/${encodeURIComponent(author.id)}/id-candidates`,
      ),
  })

  const manualScholarSearchMutation = useMutation({
    mutationFn: () =>
      api.post<{ candidates: ScholarCandidate[] }>(
        `/authors/${encodeURIComponent(author.id)}/search-scholar`,
      ),
    onSuccess: (data) => {
      setManualScholarCandidates(data.candidates ?? [])
      if ((data.candidates ?? []).length === 0) {
        toast({
          title: 'No Scholar candidates found',
          description: 'Manual Google Scholar search did not return matches.',
        })
      }
    },
    onError: () =>
      errorToast(
        'Manual Scholar search failed',
        'Google Scholar scraping failed or is disabled in settings.',
      ),
  })

  const confirmMutation = useMutation({
    mutationFn: (payload: { openalex_id?: string; scholar_id?: string }) =>
      api.post(`/authors/${encodeURIComponent(author.id)}/confirm-identifiers`, payload),
    onSuccess: () => {
      void invalidateQueries(queryClient, ['authors'], ['author-detail', author.id])
      toast({ title: 'Identifiers updated', description: 'Author identifiers saved.' })
    },
    onError: () => errorToast('Error', 'Failed to save identifiers.'),
  })

  return (
    <div className="space-y-4">
      <section>
        <p className="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500">
          OpenAlex candidates
        </p>
        {candidateQuery.isLoading ? (
          <p className="text-xs text-slate-400">Loading candidates...</p>
        ) : (
          <div className="space-y-2">
            {(candidateQuery.data?.openalex ?? []).slice(0, 3).map((c) => (
              <div
                key={c.openalex_id}
                className="flex items-center justify-between gap-3 rounded border border-[var(--color-border)] px-2 py-1.5"
              >
                <div className="min-w-0">
                  <p className="truncate text-xs font-medium text-slate-700">{c.display_name}</p>
                  <p className="truncate text-[11px] text-slate-500">
                    {c.openalex_id} · score {c.score}
                  </p>
                </div>
                <Button size="sm" variant="outline" onClick={() => setOpenalexId(c.openalex_id)}>
                  Use
                </Button>
              </div>
            ))}
            {(candidateQuery.data?.openalex ?? []).length === 0 ? (
              <p className="text-xs text-slate-400">No OpenAlex candidates.</p>
            ) : null}
          </div>
        )}
      </section>

      <section>
        <div className="mb-2 flex items-center justify-between">
          <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
            Scholar candidates (API)
          </p>
          {candidateQuery.data?.scholar_manual_search_enabled ? (
            <Button
              size="sm"
              variant="outline"
              onClick={() => manualScholarSearchMutation.mutate()}
              disabled={manualScholarSearchMutation.isPending}
            >
              {manualScholarSearchMutation.isPending ? (
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
              ) : (
                <Search className="h-3.5 w-3.5" />
              )}
              Search Google Scholar
            </Button>
          ) : null}
        </div>
        <div className="space-y-2">
          {(candidateQuery.data?.scholar ?? []).slice(0, 3).map((c) => (
            <div
              key={c.scholar_id}
              className="flex items-center justify-between gap-3 rounded border border-[var(--color-border)] px-2 py-1.5"
            >
              <div className="min-w-0">
                <p className="truncate text-xs font-medium text-slate-700">{c.display_name}</p>
                <p className="truncate text-[11px] text-slate-500">
                  {c.scholar_id} · score {c.score}
                  {c.source ? ` · ${c.source}` : ''}
                </p>
              </div>
              <Button size="sm" variant="outline" onClick={() => setScholarId(c.scholar_id)}>
                Use
              </Button>
            </div>
          ))}
          {manualScholarCandidates.length > 0 ? (
            <>
              <p className="pt-1 text-xs font-medium text-amber-700">Manual scrape results</p>
              {manualScholarCandidates.slice(0, 3).map((c) => (
                <div
                  key={`manual-${c.scholar_id}`}
                  className="flex items-center justify-between gap-3 rounded border border-amber-200 bg-amber-50 px-2 py-1.5"
                >
                  <div className="min-w-0">
                    <p className="truncate text-xs font-medium text-slate-700">
                      {c.display_name || c.scholar_id}
                    </p>
                    <p className="truncate text-[11px] text-slate-500">
                      {c.scholar_id} · score {c.score}
                    </p>
                  </div>
                  <Button size="sm" variant="outline" onClick={() => setScholarId(c.scholar_id)}>
                    Use
                  </Button>
                </div>
              ))}
            </>
          ) : null}
          {(candidateQuery.data?.scholar ?? []).length === 0 &&
          manualScholarCandidates.length === 0 ? (
            <p className="text-xs text-slate-400">No Scholar candidates.</p>
          ) : null}
        </div>
        {candidateQuery.data?.scholar_manual_search_enabled ? (
          <p className="mt-1 text-[11px] text-slate-500">
            Manual search uses scraping and may fail due to rate limits.
          </p>
        ) : null}
      </section>

      <div className="grid gap-2 sm:grid-cols-2">
        <Input
          placeholder="OpenAlex ID (A...)"
          value={openalexId}
          onChange={(e) => setOpenalexId(e.target.value)}
        />
        <Input
          placeholder="Scholar ID"
          value={scholarId}
          onChange={(e) => setScholarId(e.target.value)}
        />
      </div>

      {author.id_resolution_reason ? (
        <p className="text-xs text-slate-500">Last reason: {author.id_resolution_reason}</p>
      ) : null}

      <div className="flex justify-end">
        <Button
          size="sm"
          onClick={() =>
            confirmMutation.mutate({
              openalex_id: openalexId.trim() || undefined,
              scholar_id: scholarId.trim() || undefined,
            })
          }
          disabled={confirmMutation.isPending}
        >
          {confirmMutation.isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : null}
          Save identifiers
        </Button>
      </div>
    </div>
  )
}
