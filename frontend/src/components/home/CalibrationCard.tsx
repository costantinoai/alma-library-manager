/**
 * CalibrationCard — one Signal Lab round on Home (task 54 M1, D20).
 *
 * One round per visit: tap the paper you'd read first (best), then the one
 * you'd skip (worst); "Can't tell" records an honest skip. Signal-only —
 * never touches Library membership, ratings, or reading state. Hidden when
 * the substrate isn't ready, dismissible for the day.
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { FlaskConical, X } from 'lucide-react'

import { answerSignalLabRound, getSignalLabRound } from '@/api/client'
import { Button } from '@/components/ui/button'
import { PageSection } from '@/components/ui/page-section'
import { PaperTile } from '@/components/shared/PaperTile'
import { PaperTileGrid } from '@/components/shared/PaperTileGrid'
import { StatusBadge } from '@/components/ui/status-badge'

const GAME_ID = 'triplet_best_worst'
const DISMISS_KEY = 'alma.signal-lab.dismissed-day'

function todayKey(): string {
  return new Date().toISOString().slice(0, 10)
}

export function CalibrationCard() {
  const [dismissed, setDismissed] = useState(
    () => localStorage.getItem(DISMISS_KEY) === todayKey(),
  )
  const [best, setBest] = useState<string | null>(null)
  const [done, setDone] = useState<'answered' | 'skipped' | null>(null)
  const shownAt = useRef<number>(Date.now())

  const roundQuery = useQuery({
    queryKey: ['signal-lab', 'round', GAME_ID],
    queryFn: () => getSignalLabRound(GAME_ID),
    staleTime: Infinity, // one round per visit — never refetch behind the user
    enabled: !dismissed,
  })

  useEffect(() => {
    if (roundQuery.data?.available) shownAt.current = Date.now()
  }, [roundQuery.data])

  const answerMutation = useMutation({
    mutationFn: (answer: { best?: string; worst?: string } | null) =>
      answerSignalLabRound(GAME_ID, {
        token: roundQuery.data?.token ?? '',
        answer,
        reaction_ms: Date.now() - shownAt.current,
      }),
    onSuccess: (_result, answer) => setDone(answer === null ? 'skipped' : 'answered'),
  })

  const papers = useMemo(() => roundQuery.data?.papers ?? [], [roundQuery.data])

  if (dismissed || roundQuery.isError) return null
  if (!roundQuery.data?.available) return null // hidden until the substrate is ready

  const pick = (paperId: string) => {
    if (done || answerMutation.isPending) return
    if (best === null) {
      setBest(paperId)
      return
    }
    if (paperId === best) {
      setBest(null) // tap again to un-pick
      return
    }
    answerMutation.mutate({ best, worst: paperId })
  }

  const dismissForToday = () => {
    localStorage.setItem(DISMISS_KEY, todayKey())
    setDismissed(true)
  }

  return (
    <PageSection
      id="home-calibration"
      title="Calibrate"
      icon={FlaskConical}
      description={
        done
          ? done === 'answered'
            ? 'Recorded. Your next model refit folds this in — see Settings → Intelligence → Signal Lab.'
            : 'Skipped — no verdict recorded.'
          : (roundQuery.data.question ?? 'Which would you read first — and which would you skip?') +
            (best === null ? ' Tap your read first.' : ' Now tap the one you’d skip.')
      }
      action={
        <div className="flex items-center gap-2">
          {!done && (
            <Button
              variant="ghost"
              size="sm"
              disabled={answerMutation.isPending}
              onClick={() => answerMutation.mutate(null)}
            >
              Can&apos;t tell
            </Button>
          )}
          <Button variant="ghost" size="sm" onClick={dismissForToday} aria-label="Dismiss for today">
            <X className="h-4 w-4" />
          </Button>
        </div>
      }
    >
      {!done && (
        <PaperTileGrid
          items={papers}
          getKey={(paper) => paper.id}
          collapsedRows={1}
          expandable={false}
          renderTile={(paper) => (
            <PaperTile
              title={paper.title}
              byline={[paper.authors, paper.year, paper.journal].filter(Boolean).join(' · ')}
              excerpt={paper.summary}
              onSelect={() => pick(paper.id)}
              eyebrow={
                best === paper.id ? (
                  <StatusBadge tone="accent" size="sm">
                    Your read
                  </StatusBadge>
                ) : undefined
              }
            />
          )}
        />
      )}
    </PageSection>
  )
}
