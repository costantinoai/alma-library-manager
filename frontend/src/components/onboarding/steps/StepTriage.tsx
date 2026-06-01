import { useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { PaperCard } from '@/components/shared'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { RevealItem, RevealList } from '@/components/ui/reveal'
import { invalidateAfterPaperMutation } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'
import {
  dislikeRecommendation,
  dismissRecommendation,
  likeRecommendation,
  listLensRecommendations,
  saveRecommendation,
} from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

type Reaction = 'like' | 'love' | 'dislike' | null

export function StepTriage({ state, next, back }: StepContext) {
  const qc = useQueryClient()
  const { data, isLoading } = useQuery({
    queryKey: ['lens-recommendations', state.lensId],
    queryFn: () => listLensRecommendations(state.lensId as string, { limit: 50 }),
    enabled: Boolean(state.lensId),
    staleTime: 5_000,
  })

  const [savedIds, setSavedIds] = useState<Set<string>>(new Set())
  const [reactions, setReactions] = useState<Record<string, Reaction>>({})
  const [dismissed, setDismissed] = useState<Set<string>>(new Set())
  // In-flight guard only — we never disable the buttons (a slow/locked write
  // would otherwise leave them greyed with no feedback).
  const inFlight = useRef<Set<string>>(new Set())

  const run = async (recId: string, fn: () => Promise<unknown>, after: () => void) => {
    if (inFlight.current.has(recId)) return
    inFlight.current.add(recId)
    try {
      await fn()
      after()
      invalidateAfterPaperMutation(qc, state.lensId ?? undefined)
    } catch {
      errorToast('Could not save that', 'The database was busy — give it a moment and try again.')
    } finally {
      inFlight.current.delete(recId)
    }
  }

  const recs = (data ?? []).filter((r) => r.paper && !dismissed.has(r.id))

  return (
    <StepShell
      eyebrow="Your first batch"
      title="Here's what ALMa found."
      lead="Fresh papers, none of them already in your library. React to as many as you can — this is the feedback loop that makes every future round better."
      footer={<StepNav onBack={back} onContinue={next} continueLabel="Continue" />}
    >
      <div className="space-y-4">
        <ConceptCallout
          eyebrow="Why react here?"
          summary="Saving and dismissing recommendations is the strongest signal you can give the engine."
        >
          <p>
            Each recommendation carries a score and a "why". Save the good ones, dismiss the misses, and
            use like / love / dislike to fine-tune. Your reactions feed straight back into your lens's
            branches — so the next refresh leans toward what you kept and away from what you didn't.
          </p>
        </ConceptCallout>

        {isLoading ? (
          <div className="flex items-center justify-center gap-2 py-12 text-sm text-slate-500">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading your recommendations…
          </div>
        ) : recs.length === 0 ? (
          <div className="rounded-sm border border-[var(--color-border)] bg-surface-2 px-4 py-10 text-center text-sm text-slate-500">
            No recommendations to show yet. You can always run Discovery again from the Discovery page.
          </div>
        ) : (
          <RevealList className="space-y-3">
            {recs.map((rec, i) => (
              <RevealItem key={rec.id} index={i}>
                <PaperCard
                  paper={rec.paper!}
                  score={rec.score}
                  rank={i + 1}
                  size="default"
                  suppressSummaries
                  compactActions
                  isSaved={savedIds.has(rec.id)}
                  reaction={reactions[rec.id] ?? null}
                  onAdd={() =>
                    run(rec.id, () => saveRecommendation(rec.id), () =>
                      setSavedIds((s) => new Set(s).add(rec.id)),
                    )
                  }
                  onLike={() =>
                    run(rec.id, () => likeRecommendation(rec.id, 4), () => {
                      setSavedIds((s) => new Set(s).add(rec.id))
                      setReactions((r) => ({ ...r, [rec.id]: 'like' }))
                    })
                  }
                  onLove={() =>
                    run(rec.id, () => likeRecommendation(rec.id, 5), () => {
                      setSavedIds((s) => new Set(s).add(rec.id))
                      setReactions((r) => ({ ...r, [rec.id]: 'love' }))
                    })
                  }
                  onDislike={() =>
                    run(rec.id, () => dislikeRecommendation(rec.id), () =>
                      setReactions((r) => ({ ...r, [rec.id]: 'dislike' })),
                    )
                  }
                  onDismiss={() =>
                    run(rec.id, () => dismissRecommendation(rec.id), () =>
                      setDismissed((s) => new Set(s).add(rec.id)),
                    )
                  }
                />
              </RevealItem>
            ))}
          </RevealList>
        )}
      </div>
    </StepShell>
  )
}
