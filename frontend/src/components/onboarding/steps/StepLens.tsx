import { useEffect } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Check, Compass } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { SubPanel } from '@/components/ui/sub-panel'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { invalidateQueries } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'
import { createLens, listLenses, promoteOwnerPapers } from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

export function StepLens({ state, patch, next, back }: StepContext) {
  const qc = useQueryClient()
  const { data: lenses } = useQuery({ queryKey: ['lenses'], queryFn: listLenses, staleTime: 10_000 })

  // Best-effort: make sure the owner's papers are in the Library before the
  // lens is seeded (idempotent; the Identity step also does this).
  useEffect(() => {
    promoteOwnerPapers()
      .then(() => invalidateQueries(qc, ['bootstrap'], ['onboarding-status']))
      .catch(() => {})
  }, [qc])

  // Resolve the lens this onboarding will use: the one we created, else a
  // pre-existing library lens, else any pre-existing lens.
  const existing =
    (lenses ?? []).find((l) => l.id === state.lensId) ??
    (lenses ?? []).find((l) => l.context_type === 'library_global') ??
    (lenses ?? [])[0]
  const lensReady = Boolean(existing)

  // Persist the resolved lens id into onboarding state so the Branches /
  // Discovery / Triage steps don't think there's "no lens yet" when one
  // already existed before onboarding.
  useEffect(() => {
    if (!state.lensId && existing) patch({ lensId: existing.id })
  }, [state.lensId, existing, patch])

  const createMut = useMutation({
    mutationFn: () => createLens({ name: 'My Library', context_type: 'library_global' }),
    onSuccess: (lens) => {
      patch({ lensId: lens.id })
      invalidateQueries(qc, ['lenses'])
    },
    onError: () => errorToast('Could not create the lens', 'Please try again.'),
  })

  return (
    <StepShell
      eyebrow="Open up Discovery"
      title="Create your first lens."
      lead="A lens is how ALMa looks beyond what you already have. We'll start with one built from your whole library — your broadest view."
      footer={
        <StepNav
          onBack={back}
          onSkip={next}
          skipLabel="Skip for now"
          onContinue={next}
          continueLabel="Continue"
          continueDisabled={!lensReady}
        />
      }
    >
      <div className="space-y-5">
        <ConceptCallout
          eyebrow="What is a lens?"
          summary="A lens is a saved lookout, built from a set of papers, that finds new work similar to them."
          defaultOpen
        >
          <p>
            Think of a lens as a vantage point. It takes a seed set of papers and continuously looks for
            new work that resembles them. Lenses are independent — each one zooms into a different region
            of the literature.
          </p>
          <p>
            You might later make a lens from a single project's references, or from everything you've
            tagged "expertise", or from a topic. For now we'll make the broadest one:{' '}
            <span className="font-medium text-alma-800">your entire library</span>.
          </p>
        </ConceptCallout>

        {lensReady ? (
          <SubPanel variant="accent" className="flex items-start gap-3">
            <span className="mt-0.5 grid h-9 w-9 shrink-0 place-items-center rounded-sm bg-alma-folio text-alma-cream">
              <Check className="h-5 w-5" aria-hidden />
            </span>
            <div>
              <p className="font-brand text-base font-semibold text-alma-800">
                {existing?.name ?? 'My Library'}
              </p>
              <p className="text-sm text-slate-600">
                Your library lens is ready. Next we'll look at how it organises your interests.
              </p>
            </div>
          </SubPanel>
        ) : (
          <SubPanel className="flex flex-col items-start gap-3">
            <div className="flex items-start gap-3">
              <span className="mt-0.5 grid h-9 w-9 shrink-0 place-items-center rounded-sm bg-alma-folio-soft text-alma-folio">
                <Compass className="h-5 w-5" aria-hidden />
              </span>
              <div>
                <p className="font-brand text-base font-semibold text-alma-800">Library lens</p>
                <p className="text-sm text-slate-600">
                  Seeded from every paper in your library. The more you've saved, the richer it is.
                </p>
              </div>
            </div>
            <Button variant="accent" onClick={() => createMut.mutate()} loading={createMut.isPending}>
              Create my library lens
            </Button>
          </SubPanel>
        )}
      </div>
    </StepShell>
  )
}
