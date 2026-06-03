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
  undoPaperFeedback,
  type LensRecommendation,
} from '@/api/client'
import { StepShell, StepNav } from '../StepShell'
import type { StepContext } from '../types'

type Reaction = 'like' | 'love' | 'dislike' | null
type Kind = 'add' | 'like' | 'love' | 'dislike' | 'dismiss'

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

  const setSaved = (recId: string, on: boolean) =>
    setSavedIds((s) => {
      const n = new Set(s)
      if (on) n.add(recId)
      else n.delete(recId)
      return n
    })

  const react = async (rec: LensRecommendation, kind: Kind) => {
    const recId = rec.id
    if (inFlight.current.has(recId)) return

    const paperId = rec.paper_id || rec.paper?.id
    if (!paperId) {
      errorToast('Could not save that', 'This recommendation is missing a paper id.')
      return
    }

    const wasSaved = savedIds.has(recId)
    const wasReaction = reactions[recId] ?? null

    let undoAspect: 'membership' | 'rating' | null = null
    if (kind === 'add') undoAspect = wasSaved && wasReaction === null ? 'membership' : null
    else if (kind === 'like') undoAspect = wasReaction === 'like' ? 'rating' : null
    else if (kind === 'love') undoAspect = wasReaction === 'love' ? 'rating' : null
    else if (kind === 'dislike') undoAspect = wasReaction === 'dislike' ? 'rating' : null

    if (kind === 'dismiss') {
      setDismissed((s) => new Set(s).add(recId))
    } else if (undoAspect === 'membership') {
      setSaved(recId, false)
      setReactions((r) => ({ ...r, [recId]: null }))
    } else if (undoAspect === 'rating') {
      setReactions((r) => ({ ...r, [recId]: null }))
    } else if (kind === 'add') {
      setSaved(recId, true)
      setReactions((r) => ({ ...r, [recId]: null }))
    } else if (kind === 'like' || kind === 'love') {
      setSaved(recId, true)
      setReactions((r) => ({ ...r, [recId]: kind }))
    } else if (kind === 'dislike') {
      setReactions((r) => ({ ...r, [recId]: 'dislike' }))
    }

    inFlight.current.add(recId)
    try {
      if (undoAspect) await undoPaperFeedback(paperId, undoAspect)
      else if (kind === 'add') await saveRecommendation(recId)
      else if (kind === 'like') await likeRecommendation(recId, 4)
      else if (kind === 'love') await likeRecommendation(recId, 5)
      else if (kind === 'dislike') await dislikeRecommendation(recId)
      else await dismissRecommendation(recId)

      void invalidateAfterPaperMutation(qc, state.lensId ?? undefined)
    } catch {
      if (kind === 'dismiss') {
        setDismissed((s) => {
          const n = new Set(s)
          n.delete(recId)
          return n
        })
      } else {
        setSaved(recId, wasSaved)
        setReactions((r) => ({ ...r, [recId]: wasReaction }))
      }
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
                  savedClickRemoves
                  onAdd={() => react(rec, 'add')}
                  onLike={() => react(rec, 'like')}
                  onLove={() => react(rec, 'love')}
                  onDislike={() => react(rec, 'dislike')}
                  onDismiss={() => react(rec, 'dismiss')}
                />
              </RevealItem>
            ))}
          </RevealList>
        )}
      </div>
    </StepShell>
  )
}
