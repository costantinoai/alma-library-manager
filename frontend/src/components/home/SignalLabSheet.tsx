/**
 * Signal Lab — the daily calibration round on Home.
 *
 * Built from the page's own parts, deliberately: a `PageSection` like Inbox
 * and Reading list, holding a `PaperTileGrid` of three `PaperTile`s whose
 * `actions` strip carries the verdicts. Three earlier passes hand-rolled a
 * shell and each one read as a foreign object wedged between two real
 * sections. See `tasks/lessons.md` → "A new surface inherits the app's
 * vocabulary".
 *
 * Two games, on a segmented toggle. **Favourites** (best–worst) teaches what
 * should score high; **Odd one out** teaches where a region's boundary lies.
 * The day still picks the default, so the boundary question keeps getting
 * asked; the toggle only makes it overridable.
 *
 * The verdicts carry the app's own valence colours (`PaperActionBar`'s tone
 * map): **success** = your most favourite, **critical** = your least, and
 * **accent** for the odd one out, which is a categorical call and not a
 * good/bad one. Each button says what it records, so nothing has to be mapped
 * from position to meaning.
 *
 * Favourites records once BOTH verdicts are given; the pair is the datum. Odd
 * one out records on the single mark. Marking a paper lifts the verdict it
 * held elsewhere: no paper is both your most and your least favourite.
 *
 * Answers are signal-only: a round never touches Library membership, ratings,
 * reading state or coordinates (D11). The band says so, because that is the
 * reassurance that makes a fast instinctive answer safe to give.
 */
import { useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { BookOpen, FlaskConical, RotateCcw, Split, X } from 'lucide-react'

import {
  answerSignalLabRound,
  getApiErrorMessage,
  getPaperById,
  getSignalLabQueue,
  getSignalLabSummary,
  type SignalLabDirection,
} from '@/api/client'
import { PaperDetailPanel } from '@/components/discovery'
import { PaperTile, PaperTileGrid } from '@/components/shared'
import { Button } from '@/components/ui/button'
import { ErrorState } from '@/components/ui/ErrorState'
import { Meter } from '@/components/ui/meter'
import { Skeleton } from '@/components/ui/skeleton'
import { PageSection } from '@/components/ui/page-section'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { SurfaceProvider } from '@/components/ui/surface'
import { ToggleGroup, ToggleGroupItem } from '@/components/ui/toggle-group'
import { errorToast } from '@/hooks/useToast'
import { cn } from '@/lib/utils'

const DISMISS_KEY = 'alma.signal-lab.dismissed-day'
const DECK_SIZE = 12

function todayKey(): string {
  return new Date().toISOString().slice(0, 10)
}

/** The two games, as a segmented choice. They teach different things — one
 *  what you rate highly, the other where a region's boundary really lies — so
 *  which one you answer is a decision worth exposing rather than a hidden
 *  every-third-day rule. */
const GAMES = [
  {
    id: 'triplet_best_worst',
    label: 'Favourites',
    hint: 'Pick your most and least favourite — teaches what should score high',
  },
  {
    id: 'triplet_odd_one_out',
    label: 'Odd one out',
    hint: 'Pick the one that does not belong — teaches where region boundaries lie',
  },
] as const

/** Which game the day opens on. The rotation still sets the DEFAULT, so the
 *  boundary question keeps getting answered without being asked for; the
 *  toggle just makes it overridable. Stable for the day, so a reload cannot
 *  swap the rules under an unfinished deck. */
function gameForToday(): string {
  const day = Number(todayKey().split('-').join(''))
  return day % 3 === 0 ? 'triplet_odd_one_out' : 'triplet_best_worst'
}

function directionText(direction: SignalLabDirection): string {
  const change = Math.round(Math.abs(direction.value) * 100)
  return `${direction.label} ${direction.value >= 0 ? '+' : '−'}${change}%`
}

type Verdict = 'best' | 'worst' | 'odd'
type Marks = Record<Verdict, string | null>
const NO_MARKS: Marks = { best: null, worst: null, odd: null }

interface VerdictSpec {
  id: Verdict
  label: string
  icon: typeof BookOpen
  /** Why this hue: see the tone contract in `PaperActionBar`. */
  tone: 'success' | 'critical' | 'accent'
  /** The same valence, as a `StatusBadge` tone for the tile's eyebrow. */
  badge: StatusBadgeTone
  /** …and as a wash over the whole tile, so a given verdict is visible from
   *  across the row rather than only in its badge. */
  tile: string
  /** Accessible name, given the paper the button sits under. */
  name: (title: string) => string
}

// Copy is owned here, not taken from the API's `question`. The task has to be
// unequivocal in three words on a button, and "most / least favourite" is the
// plainest framing of the same best–worst judgement the backend records.
const BEST_WORST: VerdictSpec[] = [
  {
    id: 'best',
    label: 'Most favourite',
    icon: BookOpen,
    tone: 'success',
    badge: 'positive',
    tile: 'border-success-700/30 bg-success-700/[0.07]',
    name: (title) => `“${title}” is your most favourite of the three`,
  },
  {
    id: 'worst',
    label: 'Least favourite',
    icon: X,
    tone: 'critical',
    badge: 'negative',
    tile: 'border-critical-700/30 bg-critical-700/[0.07]',
    name: (title) => `“${title}” is your least favourite of the three`,
  },
]

const ODD_ONE_OUT: VerdictSpec[] = [
  {
    id: 'odd',
    label: "Doesn't belong",
    icon: Split,
    // Accent, not critical: naming the outlier is a categorical judgement, not
    // a verdict against the paper.
    tone: 'accent',
    badge: 'accent',
    tile: 'border-accent-edge bg-accent-soft/60',
    name: (title) => `“${title}” is the one that does not belong`,
  },
]

/** The instruction, in the app's own words rather than the API's. */
const TASK = {
  bestWorst: 'Pick your most favourite of these three, and your least favourite.',
  oddOneOut: 'Pick the one that does not belong with the other two.',
} as const

/** Tone classes, matching `PaperActionBar` so a verdict button here reads as
 *  the same object as an action button on a paper card. */
const TONE = {
  // `active` runs one step heavier than `PaperActionBar`'s: here the button
  // sits on a cell ALREADY tinted with its own hue, and the standard /15 wash
  // disappeared into it.
  success: {
    idle: 'hover:bg-success-700/10 hover:text-success-800',
    active: 'border-success-700/35 bg-success-700/25 font-semibold text-success-800',
    icon: 'text-success-600',
  },
  critical: {
    idle: 'hover:bg-critical-700/10 hover:text-critical-700',
    active: 'border-critical-700/35 bg-critical-700/25 font-semibold text-critical-700',
    icon: 'text-slate-500',
  },
  accent: {
    idle: 'hover:bg-accent-soft hover:text-alma-folio',
    active: 'border-accent-edge bg-alma-folio/20 font-semibold text-alma-folio',
    icon: 'text-alma-folio',
  },
} as const

function VerdictButton({
  spec,
  active,
  disabled,
  title,
  onClick,
}: {
  spec: VerdictSpec
  active: boolean
  disabled: boolean
  title: string
  onClick: () => void
}) {
  const tone = TONE[spec.tone]
  const Icon = spec.icon
  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      aria-pressed={active}
      aria-label={title}
      title={title}
      disabled={disabled}
      onClick={onClick}
      className={cn(
        'h-7 flex-1 gap-1.5 whitespace-nowrap rounded-sm border px-2 text-[11px] font-medium',
        'focus-visible:ring-offset-1 disabled:opacity-40',
        active
          ? tone.active
          : cn('border-control-edge bg-control-well text-alma-900', tone.idle),
      )}
    >
      <Icon className={cn('h-3.5 w-3.5 shrink-0', active ? 'text-current' : tone.icon)} />
      {spec.label}
    </Button>
  )
}

export function SignalLabSheet() {
  const queryClient = useQueryClient()
  const [dismissed, setDismissed] = useState(
    () => localStorage.getItem(DISMISS_KEY) === todayKey(),
  )
  const [gameId, setGameId] = useState<string>(() => gameForToday())
  // Progress is PER GAME. Each game has its own deck (its own query key and
  // its own signed tokens), so one shared cursor meant switching back to a
  // game replayed rounds you had already answered — and the backend rejects a
  // second answer for a spent nonce, so those rounds were unanswerable.
  const [progress, setProgress] = useState<Record<string, number>>({})
  const [marks, setMarks] = useState<Marks>(NO_MARKS)
  // Which paper's details are open. Only the id is held: the round payload is
  // a judging summary, and the popup wants the full corpus row.
  const [detailPaperId, setDetailPaperId] = useState<string | null>(null)
  const shownAt = useRef<number>(Date.now())

  const queueQuery = useQuery({
    queryKey: ['signal-lab', 'queue', gameId, DECK_SIZE],
    queryFn: () => getSignalLabQueue(gameId, DECK_SIZE),
    staleTime: Infinity,
    enabled: !dismissed,
  })
  const summaryQuery = useQuery({
    queryKey: ['signal-lab', 'summary'],
    queryFn: getSignalLabSummary,
    staleTime: 15_000,
    enabled: !dismissed,
  })

  const detailQuery = useQuery({
    queryKey: ['signal-lab', 'paper', detailPaperId],
    queryFn: () => getPaperById(detailPaperId as string),
    enabled: Boolean(detailPaperId),
    staleTime: 60_000,
  })

  const rounds = useMemo(() => queueQuery.data?.rounds ?? [], [queueQuery.data])
  const roundIndex = progress[gameId] ?? 0
  const round = rounds[roundIndex]
  const deckComplete = rounds.length > 0 && roundIndex >= rounds.length
  const isOddGame = queueQuery.data?.options?.includes('odd') ?? false
  const verdicts = isOddGame ? ODD_ONE_OUT : BEST_WORST
  const deckReady = queueQuery.data?.available === true && rounds.length >= 10

  // Switching games switches the query KEY, so the new deck starts with no
  // cached data. Without this latch the "no deck" guard below — which is also
  // the first-load guard — unmounted the whole band until the new deck landed,
  // and `/queue` can take tens of seconds: you pressed a toggle and the section
  // vanished. Once a deck has rendered, the band stays put and swaps its BODY
  // for a loading state instead.
  const [everReady, setEverReady] = useState(false)
  useEffect(() => {
    if (deckReady) setEverReady(true)
  }, [deckReady])

  useEffect(() => {
    if (round) shownAt.current = Date.now()
  }, [round])

  const answerMutation = useMutation({
    mutationFn: (answer: { best?: string; worst?: string; odd?: string } | null) =>
      answerSignalLabRound(gameId, {
        token: round?.token ?? '',
        answer,
        reaction_ms: Date.now() - shownAt.current,
      }),
    onSuccess: async () => {
      setMarks(NO_MARKS)
      setProgress((prev) => ({ ...prev, [gameId]: (prev[gameId] ?? 0) + 1 }))
      await queryClient.invalidateQueries({ queryKey: ['signal-lab', 'summary'] })
    },
    onError: (error) =>
      errorToast('Signal Lab answer was not recorded', getApiErrorMessage(error)),
  })

  if (dismissed) return null
  if (queueQuery.isError) {
    return (
      <PageSection
        id="home-signal-lab"
        title="Signal Lab"
        icon={FlaskConical}
        description="A daily round of three papers that tunes your ranking."
      >
        <ErrorState
          title="Today’s Signal Lab round did not load"
          message={getApiErrorMessage(queueQuery.error)}
          actionLabel="Try again"
          actionPending={queueQuery.isFetching}
          onAction={() => void queueQuery.refetch()}
        />
      </PageSection>
    )
  }
  // Silent until the FIRST deck exists — a band advertising a game the corpus
  // cannot serve yet is worse than no band. After that it stays on screen and
  // shows its own loading state.
  if (!deckReady && !everReady) return null

  const give = (verdict: Verdict, paperId: string) => {
    if (!round || answerMutation.isPending) return
    if (isOddGame) {
      setMarks({ ...NO_MARKS, odd: paperId })
      answerMutation.mutate({ odd: paperId })
      return
    }
    // One paper cannot be both the one you would read and the one you would
    // skip, so giving it one verdict lifts the other it may hold.
    const next: Marks = {
      ...NO_MARKS,
      best: verdict === 'best' ? paperId : marks.best === paperId ? null : marks.best,
      worst: verdict === 'worst' ? paperId : marks.worst === paperId ? null : marks.worst,
    }
    setMarks(next)
    if (next.best && next.worst) {
      answerMutation.mutate({ best: next.best, worst: next.worst })
    }
  }

  const switchGame = (next: string) => {
    if (next === gameId) return
    // Only the half-given verdicts are dropped; where you had got to in the
    // game you are leaving is kept, and where you had got to in the one you
    // are entering is restored.
    setMarks(NO_MARKS)
    setGameId(next)
  }

  const dismissForToday = () => {
    localStorage.setItem(DISMISS_KEY, todayKey())
    setDismissed(true)
  }

  const loadAnotherDeck = async () => {
    setMarks(NO_MARKS)
    setProgress((prev) => ({ ...prev, [gameId]: 0 }))
    await queueQuery.refetch()
  }

  // Read every GROUP of the summary defensively, not just the envelope. A
  // summary whose shape is a deploy behind still has `fit` undefined, and that
  // throw took all of Home down with it once. A band that cannot render its
  // own telemetry degrades to zeroes; it never white-screens its host page.
  const summary = summaryQuery.data
  const effects = summary?.effects
  const tally = summary?.rounds
  const fit = summary?.fit
  const coverage = summary?.coverage
  const upward = effects?.upward ?? []
  const downward = effects?.downward ?? []
  const authorsUp = effects?.authors_up ?? []
  const authorsDown = effects?.authors_down ?? []
  // Best–worst needs both verdicts before it can record. Say so only while the
  // round is half-answered: an instruction that is always on screen is read
  // once and then becomes furniture.
  const halfMarked = !isOddGame && Boolean(marks.best) !== Boolean(marks.worst)

  return (
    <PageSection
      id="home-signal-lab"
      collapsible
      title="Signal Lab"
      icon={FlaskConical}
      count={Math.max(0, rounds.length - roundIndex)}
      // The grey line is the standing reassurance, not the task: it is the same
      // sentence every round, and it is what makes a fast answer safe to give.
      // The task itself sits directly above the papers it applies to.
      description="Nothing in your library changes — answers only tune ranking."
      action={
        <div className="flex items-center gap-1">
          {halfMarked && (
            <span className="mr-1 text-xs font-medium text-alma-folio">
              Now pick the other one
            </span>
          )}
          {!deckComplete && (
            <Button
              variant="ghost"
              size="sm"
              disabled={answerMutation.isPending}
              onClick={() => answerMutation.mutate(null)}
            >
              Can&apos;t tell
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon"
            onClick={dismissForToday}
            aria-label="Put Signal Lab away for today"
            title="Put it away for today"
          >
            <X className="h-4 w-4" />
          </Button>
        </div>
      }
    >
      {deckComplete ? (
        <Button variant="outline" size="sm" onClick={() => void loadAnotherDeck()}>
          <RotateCcw className="h-4 w-4" />
          Load another deck
        </Button>
      ) : (
        <>
          {/* Which game, how far through, and the task — in that order, so the
              instruction is the last thing read before the papers it governs. */}
          <div className="mb-2 flex flex-wrap items-center gap-x-3 gap-y-2">
            <ToggleGroup
              type="single"
              variant="segment"
              size="sm"
              value={gameId}
              onValueChange={(next) => next && switchGame(next)}
              aria-label="Which question to answer"
              className="shrink-0"
            >
              {GAMES.map((game) => (
                <ToggleGroupItem key={game.id} value={game.id} title={game.hint}>
                  {game.label}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>
            <div className="flex min-w-[8rem] flex-1 items-center gap-2">
              <Meter
                value={(roundIndex / Math.max(1, rounds.length)) * 100}
                size="xs"
                className="flex-1"
                decorative
              />
              <span className="shrink-0 text-xs tabular-nums text-slate-500">
                {deckReady ? `${roundIndex} / ${rounds.length}` : '—'}
              </span>
            </div>
          </div>
          <p className="mb-3 text-sm font-medium text-alma-800">
            {deckReady ? (isOddGame ? TASK.oddOneOut : TASK.bestWorst) : 'Dealing a new deck…'}
          </p>

          {/* The same measured tile grid every other band on this page uses.
              `SurfaceProvider level={2}` lifts the tiles one rung above the
              section card so they read as the lighter objects ON it, rather
              than panels cut into it. The verdicts ride PaperTile's `actions`
              strip — the slot Home's Inbox already uses for triage, for the
              same reason: this surface owns the decision and has nowhere to
              hand the paper off to. No `href`: the paper is the subject of a
              question, not a link. */}
          <SurfaceProvider level={2}>
            {!deckReady ? (
              <div className="grid gap-3 sm:grid-cols-3">
                {[0, 1, 2].map((slot) => (
                  <Skeleton key={slot} className="h-44" />
                ))}
              </div>
            ) : (
            <PaperTileGrid
              items={round?.papers ?? []}
              getKey={(paper) => paper.id}
              renderTile={(paper) => {
                const held = verdicts.find((verdict) => marks[verdict.id] === paper.id)
                return (
                  <PaperTile
                    className={held?.tile}
                    // The tile opens the paper; the verdicts sit in `actions`,
                    // which PaperTile lifts above the stretched overlay so a
                    // verdict click never reads as "open details".
                    onSelect={() => setDetailPaperId(paper.id)}
                    title={paper.title}
                    byline={[paper.authors, paper.year, paper.journal]
                      .filter(Boolean)
                      .join(' · ')}
                    // The TLDR / abstract opening: judging "which of these do I
                    // like most" off a title alone is guesswork.
                    excerpt={paper.summary}
                    eyebrow={
                      held ? (
                        <StatusBadge tone={held.badge} size="sm">
                          {held.label}
                        </StatusBadge>
                      ) : undefined
                    }
                    actions={
                      <div className="flex items-center gap-1.5">
                        {verdicts.map((verdict) => (
                          <VerdictButton
                            key={verdict.id}
                            spec={verdict}
                            active={marks[verdict.id] === paper.id}
                            disabled={answerMutation.isPending}
                            title={verdict.name(paper.title)}
                            onClick={() => give(verdict.id, paper.id)}
                          />
                        ))}
                      </div>
                    }
                  />
                )
              }}
            />
            )}
          </SurfaceProvider>
        </>
      )}

      {/* Foot: what the answers have done so far. Evidence, not a second
          diagnostics product — the model, its eval and every knob live in
          Settings → Intelligence → Signal Lab. */}
      <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-edge-1 pt-2.5 text-[11px]">
        <dl className="flex flex-wrap items-center gap-x-4 gap-y-1">
          <Fact label="Rounds">
            {tally?.today ?? 0} today · {tally?.total ?? 0} recorded
          </Fact>
          {/* Valence colour belongs to a real reading; a placeholder painted
              green or red claims a direction that has not been measured. */}
          <Fact label="Up" tone={upward.length > 0 ? 'positive' : 'neutral'}>
            {upward.length > 0 ? upward.map(directionText).join(' · ') : 'not fitted'}
          </Fact>
          <Fact label="Down" tone={downward.length > 0 ? 'negative' : 'neutral'}>
            {downward.length > 0 ? downward.map(directionText).join(' · ') : 'not fitted'}
          </Fact>
          <Fact label="Authors up" tone={authorsUp.length > 0 ? 'positive' : 'neutral'}>
          {authorsUp.length > 0
            ? authorsUp.map((a) => a.label).join(' · ')
            : 'not fitted'}
        </Fact>
        <Fact label="Authors down" tone={authorsDown.length > 0 ? 'negative' : 'neutral'}>
          {authorsDown.length > 0
            ? authorsDown.map((a) => a.label).join(' · ')
            : 'not fitted'}
        </Fact>
        <Fact label="Boundaries">
            {effects?.boundary_overrides ?? 0} sharpened · {effects?.regions_moving ?? 0}{' '}
            moving
          </Fact>
          <Fact
            label="Fit"
            title={
              fit
                ? `${fit.fitted_queries} unique queries in the current fit; ${fit.pending_rounds} recorded rounds await refit`
                : undefined
            }
          >
            {fit?.fitted_observations ?? 0} obs · {fit?.utility_preferences ?? 0} prefs
          </Fact>
          <Fact
            label="Coverage"
            title={
              tally
                ? `${tally.unique_queries} unique question sets; ${tally.duplicate_queries} accidental repeats`
                : undefined
            }
          >
            {coverage?.regions_observed ?? 0}/{coverage?.regions_total ?? 0} regions ·{' '}
            {coverage?.edges_observed ?? 0}/{coverage?.edges_total ?? 0} edges
          </Fact>
        </dl>
      </div>

      {/* The paper behind a tile, in the app's one paper popup. */}
      <PaperDetailPanel
        paper={detailQuery.data ?? null}
        open={Boolean(detailPaperId)}
        onOpenChange={(next) => !next && setDetailPaperId(null)}
      />
    </PageSection>
  )
}

/** One `label value` pair in the foot ledger. */
function Fact({
  label,
  tone = 'neutral',
  title,
  children,
}: {
  label: string
  tone?: 'neutral' | 'positive' | 'negative'
  title?: string
  children: React.ReactNode
}) {
  return (
    <span className="flex items-baseline gap-1.5" title={title}>
      <dt className="text-slate-400">{label}</dt>
      <dd
        className={cn(
          tone === 'neutral' && 'text-slate-600',
          tone === 'positive' && 'text-success-800',
          tone === 'negative' && 'text-critical-700',
        )}
      >
        {children}
      </dd>
    </span>
  )
}
