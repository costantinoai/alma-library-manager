import { useRef, useState } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { PaperCard } from '@/components/shared'
import { ConceptCallout } from '@/components/ui/concept-callout'
import { RevealItem, RevealList } from '@/components/ui/reveal'
import { invalidateQueries } from '@/lib/queryHelpers'
import { errorToast } from '@/hooks/useToast'
import {
  listPapers,
  onboardingPaperFeedback,
  undoPaperFeedback,
  type Publication,
} from '@/api/client'
import { StepShell, StepNav, GoalMeter } from '../StepShell'
import type { StepContext } from '../types'

const TARGET = 10
type Reaction = 'like' | 'love' | 'dislike' | null
type Kind = 'add' | 'like' | 'love' | 'dislike' | 'dismiss'

export function StepReact({ next, back }: StepContext) {
  const qc = useQueryClient()
  const { data, isLoading } = useQuery({
    queryKey: ['onboarding-followed-papers'],
    queryFn: () =>
      listPapers({ scope: 'followed_corpus', order: 'citations', orderDir: 'desc', limit: 40 }),
    // Keep pulling while the followed-author backfills are still landing papers.
    refetchInterval: 12_000,
    staleTime: 8_000,
  })

  const [savedIds, setSavedIds] = useState<Set<string>>(new Set())
  const [reactions, setReactions] = useState<Record<string, Reaction>>({})
  const [dismissed, setDismissed] = useState<Set<string>>(new Set())
  // In-flight guard (NOT a visual disable — the optimistic state already shows
  // the result, and disabling buttons under a slow/locked write was the "grayed
  // out but nothing happened" bug). We just ignore a second click while a
  // write for the same paper is still settling.
  const inFlight = useRef<Set<string>>(new Set())

  const setSaved = (id: string, on: boolean) =>
    setSavedIds((s) => {
      const n = new Set(s)
      if (on) n.add(id)
      else n.delete(id)
      return n
    })

  // Optimistic + per-aspect toggle. Re-clicking the active action undoes only
  // that button's effect: re-clicking the active reaction clears the rating
  // (keeps the paper saved); re-clicking Save removes it from the library.
  // Errors roll back the optimistic state and surface a toast.
  const react = async (paperId: string, kind: Kind) => {
    if (inFlight.current.has(paperId)) return
    const wasSaved = savedIds.has(paperId)
    const wasReaction = reactions[paperId] ?? null

    // Which aspect (if any) this click toggles OFF.
    let undoAspect: 'membership' | 'rating' | null = null
    if (kind === 'add') undoAspect = wasSaved && wasReaction === null ? 'membership' : null
    else if (kind === 'like') undoAspect = wasReaction === 'like' ? 'rating' : null
    else if (kind === 'love') undoAspect = wasReaction === 'love' ? 'rating' : null
    else if (kind === 'dislike') undoAspect = wasReaction === 'dislike' ? 'rating' : null

    // Apply optimistically.
    if (kind === 'dismiss') {
      setDismissed((s) => new Set(s).add(paperId))
    } else if (undoAspect === 'membership') {
      setSaved(paperId, false)
      setReactions((r) => ({ ...r, [paperId]: null }))
    } else if (undoAspect === 'rating') {
      // Clear the reaction but keep the paper saved.
      setReactions((r) => ({ ...r, [paperId]: null }))
    } else if (kind === 'dislike') {
      setSaved(paperId, false)
      setReactions((r) => ({ ...r, [paperId]: 'dislike' }))
    } else {
      // add / like / love all save to the library (rating 3 / 4 / 5).
      setSaved(paperId, true)
      setReactions((r) => ({ ...r, [paperId]: kind === 'add' ? null : kind }))
    }

    inFlight.current.add(paperId)
    try {
      if (undoAspect) await undoPaperFeedback(paperId, undoAspect)
      else await onboardingPaperFeedback(paperId, kind)
    } catch {
      // Roll back to the pre-click state and tell the user.
      if (kind === 'dismiss') {
        setDismissed((s) => {
          const n = new Set(s)
          n.delete(paperId)
          return n
        })
      } else {
        setSaved(paperId, wasSaved)
        setReactions((r) => ({ ...r, [paperId]: wasReaction }))
      }
      errorToast('Could not save that', 'The database was busy — give it a moment and try again.')
    } finally {
      inFlight.current.delete(paperId)
    }
  }

  const goContinue = () => {
    invalidateQueries(qc, ['bootstrap'], ['onboarding-status'], ['library-papers'], ['papers'])
    next()
  }

  const papers = (data ?? []).filter((p) => !dismissed.has(p.id))
  const savedCount = savedIds.size

  return (
    <StepShell
      eyebrow="Teach the ranker"
      title="React to your authors' best work."
      lead="Here are the most-cited papers from the people you follow. Save the ones worth keeping, and react to the rest — every signal sharpens what ALMa shows you next."
      footer={
        <StepNav
          onBack={back}
          onSkip={savedCount >= TARGET ? undefined : next}
          skipLabel="Skip for now"
          onContinue={goContinue}
          continueLabel="Continue"
          hint={
            savedCount >= TARGET
              ? undefined
              : `Aim for at least ${TARGET} saved — the more you curate now, the better Discovery works later.`
          }
        />
      }
    >
      <div className="space-y-4">
        <ConceptCallout
          eyebrow="What do the buttons mean?"
          summary="Save keeps a paper; Like / Love are stronger praise; Dislike and Dismiss teach us what to avoid."
        >
          <p>
            <span className="font-medium text-alma-800">Save</span> adds a paper to your library.{' '}
            <span className="font-medium text-alma-800">Like</span> and{' '}
            <span className="font-medium text-alma-800">Love</span> save it too, with progressively
            stronger positive signal.
          </p>
          <p>
            <span className="font-medium text-alma-800">Dislike</span> is a quiet "not for me" — the paper
            stays but counts against similar work. <span className="font-medium text-alma-800">Dismiss</span>{' '}
            hides it. All of it is signal; none of it is permanent.
          </p>
        </ConceptCallout>

        <div className="flex justify-end">
          <GoalMeter done={savedCount} target={TARGET} noun="saved" />
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center gap-2 py-12 text-sm text-slate-500">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading your authors' papers…
          </div>
        ) : papers.length === 0 ? (
          <div className="flex items-center justify-center gap-2 rounded-sm border border-[var(--color-border)] bg-surface-2 px-4 py-10 text-center text-sm text-slate-500">
            <Loader2 className="h-4 w-4 animate-spin" />
            We're still gathering papers from the authors you follow. This updates automatically.
          </div>
        ) : (
          <RevealList className="space-y-3">
            {papers.map((p: Publication, i) => (
              <RevealItem key={p.id} index={i}>
                <PaperCard
                  paper={p}
                  size="default"
                  suppressSummaries
                  compactActions
                  isSaved={savedIds.has(p.id)}
                  savedClickRemoves
                  reaction={reactions[p.id] ?? null}
                  onAdd={() => react(p.id, 'add')}
                  onLike={() => react(p.id, 'like')}
                  onLove={() => react(p.id, 'love')}
                  onDislike={() => react(p.id, 'dislike')}
                  onDismiss={() => react(p.id, 'dismiss')}
                />
              </RevealItem>
            ))}
          </RevealList>
        )}
      </div>
    </StepShell>
  )
}
