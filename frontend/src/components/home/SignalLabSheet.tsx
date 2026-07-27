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
import { BookOpen, Check, FlaskConical, RotateCcw, Split, X } from 'lucide-react'

import {
  answerSignalLabRound,
  getApiErrorMessage,
  getPaperById,
  getSignalLabQueue,
  getSignalLabSummary,
  type SignalLabDirection,
} from '@/api/client'
import { PaperDetailPanel } from '@/components/discovery'
import { PaperTile, PaperTileGrid, SignalChip } from '@/components/shared'
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

/** The games, as a segmented choice. They teach different things — what you
 *  rate highly, where a region's boundary really lies, and which venue you
 *  would rather read at equal topic — so which one you answer is a decision
 *  worth exposing rather than a hidden every-third-day rule.
 *
 *  `tiles` is how many papers a round of that game shows. The skeleton and the
 *  grid read it, so a k=2 game does not flash three placeholders. */
const GAMES = [
  {
    id: 'triplet_best_worst',
    label: 'Favourites',
    tiles: 3,
    hint: 'Pick your most and least favourite — teaches what should score high',
    task: 'Pick your most favourite of these three, and your least favourite.',
  },
  {
    id: 'triplet_odd_one_out',
    label: 'Odd one out',
    tiles: 3,
    hint: 'Pick the one that does not belong — teaches where region boundaries lie',
    task: 'Pick the one that does not belong with the other two.',
  },
  {
    id: 'matched_pair_venue',
    label: 'Same field',
    tiles: 2,
    hint: 'Two papers on one topic — teaches which venue you would rather read',
    // Says WHY the two look alike, because that is the whole instrument: the
    // pair was drawn so topic is held constant and the venue is what is left
    // to choose on.
    task: 'These two are on the same topic. Which would you rather read?',
  },
] as const

/** Which game the day opens on. The rotation still sets the DEFAULT, so the
 *  questions nobody would go looking for keep getting answered; the toggle just
 *  makes it overridable. Stable for the day, so a reload cannot swap the rules
 *  under an unfinished deck.
 *
 *  Best–worst stays the most common — it trains the utility direction, which
 *  every other head is measured against. The other two get a regular share
 *  because they are the ONLY source of their evidence: boundary votes for
 *  regions, matched pairs for venues. */
function gameForToday(): string {
  const day = Number(todayKey().split('-').join(''))
  if (day % 3 === 0) return 'triplet_odd_one_out'
  if (day % 3 === 1) return 'matched_pair_venue'
  return 'triplet_best_worst'
}

function directionText(direction: SignalLabDirection): string {
  const change = Math.round(Math.abs(direction.value) * 100)
  return `${direction.label} ${direction.value >= 0 ? '+' : '−'}${change}%`
}

type Verdict = 'best' | 'worst' | 'odd' | 'picked'
type Marks = Record<Verdict, string | null>
const NO_MARKS: Marks = { best: null, worst: null, odd: null, picked: null }

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
//
// WHICH of these a round shows is decided by the API's `options` — the game's
// own answer vocabulary — not by a per-game branch here. That is the same
// contract the answer route validates against, so a new game reaches the UI by
// adding its verdict below and nothing else.
const VERDICTS: Record<Verdict, VerdictSpec> = {
  best: {
    id: 'best',
    label: 'Most favourite',
    icon: BookOpen,
    tone: 'success',
    badge: 'positive',
    tile: 'border-success-700/30 bg-success-700/[0.07]',
    name: (title) => `“${title}” is your most favourite of the three`,
  },
  worst: {
    id: 'worst',
    label: 'Least favourite',
    icon: X,
    tone: 'critical',
    badge: 'negative',
    tile: 'border-critical-700/30 bg-critical-700/[0.07]',
    name: (title) => `“${title}” is your least favourite of the three`,
  },
  odd: {
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
  picked: {
    id: 'picked',
    label: 'Rather read this',
    icon: BookOpen,
    // Success, like `best`: it is the same judgement — which of these would you
    // read — asked of two papers instead of three. The pair carries cleaner
    // evidence, not a different kind of opinion, so it must not wear a
    // different colour.
    tone: 'success',
    badge: 'positive',
    tile: 'border-success-700/30 bg-success-700/[0.07]',
    name: (title) => `you would rather read “${title}”`,
  },
}

/** Verdicts a game asks for, in display order, derived from its API vocabulary.
 *
 *  `cant_tell` is the skip sentinel and has its own control in the header, so
 *  it never becomes a tile button. An unknown option is dropped rather than
 *  crashing the band: a backend a deploy ahead must not white-screen Home. */
function verdictsFor(options: readonly string[] | undefined): VerdictSpec[] {
  return (options ?? [])
    .map((option) => VERDICTS[option as Verdict])
    .filter((spec): spec is VerdictSpec => Boolean(spec))
}

/**
 * Tone classes for the verdict controls.
 *
 * These are the app's CHIP language worn by a button, not the grey ink well
 * `PaperActionBar` uses. The difference is deliberate and local to this
 * surface: on a paper card the buttons are one of several things you may do,
 * so they recede until hovered; here answering IS the surface, the two
 * verdicts are the only interaction on the tile, and a row of identical grey
 * pills made a best–worst call read as a form to fill in. Wearing the wash at
 * rest (`hue-700 @ ~8%`, the chip formula one step lighter) means the row says
 * "green one, red one" from across the page — the fast instinctive answer the
 * round is asking for. Picking deepens the same wash and adds a check; no new
 * hue is introduced by the act of choosing.
 */
const TONE = {
  success: {
    idle: 'border-transparent bg-success-700/[0.12] text-success-800 hover:bg-success-700/[0.22]',
    active: 'border-success-700/40 bg-success-700/25 font-semibold text-success-800',
    icon: 'text-success-700',
  },
  critical: {
    idle: 'border-transparent bg-critical-700/[0.12] text-critical-700 hover:bg-critical-700/[0.22]',
    active: 'border-critical-700/40 bg-critical-700/25 font-semibold text-critical-700',
    icon: 'text-critical-700',
  },
  accent: {
    idle: 'border-transparent bg-alma-folio/[0.12] text-alma-folio hover:bg-alma-folio/[0.22]',
    active: 'border-accent-edge bg-alma-folio/25 font-semibold text-alma-folio',
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
        // Pill, not the letterpress `rounded-sm` corner every other button
        // wears: these read as chips (see the TONE note above). Chip METRICS
        // too — no stretch, no 8px height: a pair of buttons stretched across
        // the plate reads as a form's submit row, which is exactly the weight a
        // one-second instinctive judgement should not carry.
        'h-6 gap-1 whitespace-nowrap rounded-full border px-2.5 text-[11px] font-medium',
        'focus-visible:ring-offset-1 disabled:opacity-40',
        active ? tone.active : tone.idle,
      )}
    >
      <Icon className={cn('h-3.5 w-3.5 shrink-0', active ? 'text-current' : tone.icon)} />
      {spec.label}
      {/* The chosen chip carries a tick as well as a heavier wash: the wash
          alone is a hue difference, and hue is the one channel already spent
          on which verdict this is. */}
      {active && <Check className="h-3 w-3 shrink-0" aria-hidden />}
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
  const verdicts = useMemo(
    () => verdictsFor(queueQuery.data?.options),
    [queueQuery.data?.options],
  )
  const deckReady = queueQuery.data?.available === true && rounds.length >= 10
  const tileCount = GAMES.find((game) => game.id === gameId)?.tiles ?? 3

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
    mutationFn: (answer: Partial<Record<Verdict, string>> | null) =>
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
    // Marking a paper lifts whatever verdict it already held: one paper cannot
    // be both the one you would read and the one you would skip. Written over
    // the game's OWN verdict list rather than per game, so a one-verdict round
    // (odd one out, matched pair) is simply the case where the set is complete
    // after the first click.
    const next = { ...NO_MARKS }
    for (const spec of verdicts) {
      next[spec.id] =
        spec.id === verdict ? paperId : marks[spec.id] === paperId ? null : marks[spec.id]
    }
    setMarks(next)

    // The round records only once every verdict it asked for has an answer —
    // the complete set IS the datum.
    if (verdicts.every((spec) => next[spec.id])) {
      answerMutation.mutate(
        Object.fromEntries(verdicts.map((spec) => [spec.id, next[spec.id] as string])),
      )
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
  const hasEffects =
    upward.length + downward.length + authorsUp.length + authorsDown.length > 0
  // A multi-verdict round needs all of them before it can record. Say so only
  // while it is part-answered: an instruction that is always on screen is read
  // once and then becomes furniture.
  const marked = verdicts.filter((spec) => marks[spec.id]).length
  const halfMarked = marked > 0 && marked < verdicts.length

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
            {deckReady
              ? (GAMES.find((game) => game.id === gameId)?.task ??
                queueQuery.data?.question)
              : 'Dealing a new deck…'}
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
              <div
                className={cn(
                  'grid gap-3',
                  tileCount === 2 ? 'sm:grid-cols-2' : 'sm:grid-cols-3',
                )}
              >
                {Array.from({ length: tileCount }, (_, slot) => (
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
                      <div className="flex flex-wrap items-center gap-1.5">
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
          Settings → Intelligence → Signal Lab.

          It used to be one long line of grey `label value` pairs, which is the
          shape of a debug dump: eight facts of equal weight, none of them
          scannable, and the two that actually answer "did my answers do
          anything?" buried in the middle. Now it reads in two registers — the
          LEARNED directions as valence chips (the payoff), then the counters as
          quiet plumbing chips (the receipts) — through the same `SignalChip`
          registry every other pill in the app resolves through, so a green pill
          here means what a green pill means on a paper card. */}
      <div className="mt-4 space-y-2 border-t border-edge-1 pt-3">
        <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
          <span className="mr-0.5 text-[11px] font-medium uppercase tracking-wide text-slate-400">
            Learned
          </span>
          {/* Valence colour belongs to a real reading; a placeholder painted
              green or red claims a direction that has not been measured, so an
              unfitted model gets ONE quiet chip rather than four coloured
              "not fitted" ones. */}
          {hasEffects ? (
            <>
              {upward.map((direction) => (
                <SignalChip key={`up-${direction.label}`} kind="lab-up">
                  {directionText(direction)}
                </SignalChip>
              ))}
              {downward.map((direction) => (
                <SignalChip key={`down-${direction.label}`} kind="lab-down">
                  {directionText(direction)}
                </SignalChip>
              ))}
              {authorsUp.map((author) => (
                <SignalChip key={`author-up-${author.label}`} kind="lab-author-up">
                  {author.label}
                </SignalChip>
              ))}
              {authorsDown.map((author) => (
                <SignalChip key={`author-down-${author.label}`} kind="lab-author-down">
                  {author.label}
                </SignalChip>
              ))}
            </>
          ) : (
            <SignalChip kind="meta" title="The fit needs more rounds before it can state a direction">
              Not fitted yet — keep answering
            </SignalChip>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-x-2 gap-y-1.5">
          <span className="mr-0.5 text-[11px] font-medium uppercase tracking-wide text-slate-400">
            Ledger
          </span>
          <SignalChip kind="lab-rounds">
            <Stat value={tally?.today ?? 0} unit="today" />
            <Dot />
            <Stat value={tally?.total ?? 0} unit="recorded" />
          </SignalChip>
          <SignalChip kind="lab-boundary">
            <Stat value={effects?.boundary_overrides ?? 0} unit="sharpened" />
            <Dot />
            <Stat value={effects?.regions_moving ?? 0} unit="moving" />
          </SignalChip>
          <SignalChip
            kind="lab-fit"
            title={
              fit
                ? `${fit.fitted_queries} unique queries in the current fit; ${fit.pending_rounds} recorded rounds await refit`
                : 'Nothing fitted yet'
            }
          >
            <Stat value={fit?.fitted_observations ?? 0} unit="obs" />
            <Dot />
            <Stat value={fit?.utility_preferences ?? 0} unit="prefs" />
          </SignalChip>
          <SignalChip
            kind="lab-coverage"
            title={
              tally
                ? `${tally.unique_queries} unique question sets; ${tally.duplicate_queries} accidental repeats`
                : undefined
            }
          >
            <Stat
              value={`${coverage?.regions_observed ?? 0}/${coverage?.regions_total ?? 0}`}
              unit="regions"
            />
            <Dot />
            <Stat
              value={`${coverage?.edges_observed ?? 0}/${coverage?.edges_total ?? 0}`}
              unit="edges"
            />
          </SignalChip>
        </div>
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

/** A figure and the word for what it counts, inside a ledger chip.
 *
 *  Both live in ONE element: the number and its unit are a single phrase to
 *  read ("14 obs"), and splitting them across spans to dim the unit also splits
 *  the text node, which is how a screen reader — and `getByText` — stops seeing
 *  the phrase. Tabular figures keep a row of chips from twitching as the counts
 *  tick up. */
function Stat({ value, unit }: { value: number | string; unit: string }) {
  return (
    <span className="tabular-nums">
      {value} {unit}
    </span>
  )
}

/** The separator between two figures in one chip. */
function Dot() {
  return <span className="opacity-40" aria-hidden>·</span>
}
