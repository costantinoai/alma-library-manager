import { useEffect } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { Loader2, Sparkles } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { SubPanel } from '@/components/ui/sub-panel'
import { RefreshRunningBanner } from '@/components/shared'
import { OnlineSearchTab } from '@/components/OnlineSearchTab'
import { invalidateQueries } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'
import {
  getOnboardingStatus,
  listLenses,
  listLensRecommendations,
  refreshLens,
} from '@/api/client'
import { StepShell, StepNav, GoalMeter } from '../StepShell'
import type { StepContext } from '../types'

const MIN_LIBRARY = 10

export function StepDiscovery({ state, patch, next, back }: StepContext) {
  const qc = useQueryClient()

  const { data: status } = useQuery({
    queryKey: ['onboarding-status'],
    queryFn: getOnboardingStatus,
    refetchInterval: 4_000,
    staleTime: 2_000,
  })
  const libraryCount = status?.library_count ?? 0
  const enoughPapers = libraryCount >= MIN_LIBRARY

  // Resolve the lens even if the Lens step was skipped or a lens pre-existed:
  // fall back to a library lens, else the first one. Persist it so the rest of
  // the flow agrees.
  const { data: lenses } = useQuery({ queryKey: ['lenses'], queryFn: listLenses, staleTime: 10_000 })
  const lensId =
    state.lensId ??
    (lenses ?? []).find((l) => l.context_type === 'library_global')?.id ??
    (lenses ?? [])[0]?.id ??
    null
  useEffect(() => {
    if (!state.lensId && lensId) patch({ lensId })
  }, [state.lensId, lensId, patch])

  const recs = useQuery({
    queryKey: ['lens-recommendations', lensId],
    queryFn: () => listLensRecommendations(lensId as string, { limit: 50 }),
    enabled: Boolean(lensId) && state.discoveryRun,
    refetchInterval: (q) => ((q.state.data?.length ?? 0) > 0 ? false : 5_000),
    staleTime: 2_000,
  })
  const recCount = recs.data?.length ?? 0
  const recsReady = state.discoveryRun && recCount > 0

  const runMut = useMutation({
    mutationFn: () => refreshLens(lensId as string, 50),
    onSuccess: () => patch({ discoveryRun: true }),
    onError: () => errorToast('Discovery failed to start', 'Please try again in a moment.'),
  })

  const run = () => {
    if (!lensId) {
      errorToast('No lens yet', 'Go back and create your library lens first.')
      return
    }
    if (!enoughPapers) {
      errorToast(
        'Add a few more papers first',
        `Discovery needs at least ${MIN_LIBRARY} papers in your library — you have ${libraryCount}. Use the search below to add ones you like.`,
      )
      return
    }
    runMut.mutate()
  }

  return (
    <StepShell
      eyebrow="The first run"
      title="Let's discover something new."
      lead="Discovery reads your library lens and goes looking for papers you don't have yet — across citations, similar text, the graph, and live sources. It runs in the background and can take a minute or two."
      footer={
        <StepNav
          onBack={back}
          onSkip={recsReady ? undefined : next}
          skipLabel="Skip for now"
          onContinue={recsReady ? next : undefined}
          continueLabel="See my recommendations"
          primary={
            recsReady ? undefined : (
              <Button
                variant="accent"
                onClick={run}
                loading={runMut.isPending || (state.discoveryRun && !recsReady)}
                disabled={runMut.isPending || (state.discoveryRun && !recsReady)}
              >
                <Sparkles className="h-4 w-4" />
                {state.discoveryRun ? 'Discovering…' : 'Run discovery'}
              </Button>
            )
          }
        />
      }
    >
      <div className="space-y-5">
        {!state.discoveryRun ? (
          <SubPanel className="space-y-3">
            <div className="flex items-center justify-between gap-3">
              <p className="text-sm font-medium text-alma-800">Library readiness</p>
              <GoalMeter done={Math.min(libraryCount, MIN_LIBRARY)} target={MIN_LIBRARY} noun="papers" />
            </div>
            <p className="text-sm text-slate-600">
              {enoughPapers
                ? "Your library has enough to work with. Hit Run discovery whenever you're ready."
                : `Discovery needs at least ${MIN_LIBRARY} papers to learn from. Add a few you like below — search by title, author, or DOI — then run it.`}
            </p>
          </SubPanel>
        ) : null}

        {state.discoveryRun ? (
          <SubPanel variant="accent" className="space-y-3">
            <RefreshRunningBanner domain="discovery" />
            <div className="flex items-center gap-2 text-sm text-slate-600">
              {recsReady ? (
                <span className="font-medium text-alma-800">
                  {recCount} recommendations are ready — continue to take a look.
                </span>
              ) : (
                <>
                  <Loader2 className="h-4 w-4 animate-spin text-alma-folio" aria-hidden />
                  <span>Searching across sources… results will appear here shortly.</span>
                </>
              )}
            </div>
          </SubPanel>
        ) : null}

        {!recsReady ? (
          <div>
            <p className="mb-2 text-xs font-medium uppercase tracking-wide text-slate-500">
              {enoughPapers ? 'Add even more papers (optional)' : 'Add papers you like'}
            </p>
            <OnlineSearchTab
              resultPreviewLimit={enoughPapers ? 5 : null}
              onImportComplete={() => invalidateQueries(qc, ['onboarding-status'], ['bootstrap'])}
            />
          </div>
        ) : null}
      </div>
    </StepShell>
  )
}
