/** One sentence under the Discovery weights: do these weights predict what you
 * keep? Reads the STORED evaluation (the backend never computes it on a GET)
 * and says plainly when it has not been measured, cannot be measured yet, is
 * being measured, or describes earlier weights.
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { getRankerOutcome, refreshRankerOutcome, type RankerOutcomeBar } from '@/api/client'
import { Button } from '@/components/ui/button'
import { formatPercent } from '@/lib/format'
import { formatRelativeTime } from '@/lib/utils'

export const RANKER_OUTCOME_KEY = ['ranker-outcome'] as const

const BAR_PHRASE = {
  negative: 'above one you rejected',
  random_corpus: 'above a random paper from your corpus',
} as const

function barText(name: keyof typeof BAR_PHRASE, bar: RankerOutcomeBar): string {
  const sizes = `${bar.n_pos} vs ${bar.n_neg} papers`
  if (bar.verdict === 'inconclusive') return `${BAR_PHRASE[name]}: inconclusive (${sizes})`
  return `${BAR_PHRASE[name]} ${formatPercent(bar.auc)} of the time (${sizes})`
}

export function RankerOutcomeLine() {
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: RANKER_OUTCOME_KEY,
    queryFn: getRankerOutcome,
    // Poll only while a measurement is running.
    refetchInterval: (q) => (q.state.data?.rebuilding ? 5000 : false),
  })
  const refresh = useMutation({
    mutationFn: () => refreshRankerOutcome(true),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: RANKER_OUTCOME_KEY }),
  })

  const data = query.data
  if (!data) return null

  let text: string
  if (data.state === 'ready') {
    const bars = (Object.keys(BAR_PHRASE) as (keyof typeof BAR_PHRASE)[])
      .filter((name) => data.bars?.[name])
      .map((name) => barText(name, data.bars![name]!))
    text = `Measured on your own history: the ranker places a paper you later kept ${bars.join(', and ')}.`
  } else if (data.state === 'not_ready') {
    text = `Not measurable yet: ${data.reason ?? 'too little history'}.`
  } else {
    text = 'Not measured yet: whether these weights predict what you keep.'
  }

  const status = data.rebuilding
    ? 'Measuring now…'
    : data.state !== 'not_built' && data.current === false
      ? 'Weights or library changed since — this describes the earlier state.'
      : data.computed_at
        ? `Measured ${formatRelativeTime(data.computed_at)}.`
        : null

  return (
    <p className="mb-3 text-xs text-slate-600" data-testid="ranker-outcome-line">
      {text} {status}{' '}
      {!data.rebuilding && (
        <Button
          type="button"
          variant="link"
          size="sm"
          className="h-auto p-0 text-xs"
          loading={refresh.isPending}
          onClick={() => refresh.mutate()}
        >
          {data.state === 'not_built' ? 'Measure' : 'Measure again'}
        </Button>
      )}
    </p>
  )
}
