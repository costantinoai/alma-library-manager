import type { HomeTrendPoint } from '@/api/client'
import { EyebrowLabel } from '@/components/ui/eyebrow-label'
import { SubPanel } from '@/components/ui/sub-panel'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { HOME_TREND_SERIES } from '@/lib/palette'
import { cn } from '@/lib/utils'

/** Tallest a column may draw, in px. Short enough to sit on a heading row. */
const MAX_BAR_HEIGHT = 26
/** A day with any inflow still gets a visible stub, so "1" never reads as "0". */
const MIN_NONZERO_HEIGHT = 3

/** `2026-07-26` → `Sun`. Parsed as local noon so no timezone shifts the day. */
function weekday(date: string): string {
  const parsed = new Date(`${date}T12:00:00`)
  if (Number.isNaN(parsed.getTime())) return ''
  return new Intl.DateTimeFormat(undefined, { weekday: 'short' }).format(parsed)
}

function dayLabel(point: HomeTrendPoint, isToday: boolean): string {
  const when = isToday ? 'Today' : weekday(point.date)
  const total = point.feed + point.discovery
  if (total === 0) return `${when}: nothing arrived`
  return `${when}: ${point.feed} from Feed, ${point.discovery} from Discovery`
}

export interface InflowStripProps {
  trend: HomeTrendPoint[]
  className?: string
}

/**
 * Seven days of inflow, as one small stacked column per day on its own plate.
 *
 * Answers the question the three headline tiles cannot: *is today normal?* A
 * zero-Feed morning means nothing on its own and everything next to six busy
 * days. Columns are scaled against the window's own maximum — this is a shape,
 * not a measurement — so the week's total is printed beside the label and every
 * column carries its exact figures on hover.
 *
 * Feed sits at the bottom of each column and Discovery on top, in the same two
 * hues those surfaces wear elsewhere (`HOME_TREND_SERIES`). Today's column is
 * marked by a folio wash rather than a third colour, because the colours are
 * already spent saying WHICH surface.
 *
 * **Empty days are a baseline tick, not a filled rail.** The first version drew
 * a full-height track behind every column, so a quiet week rendered as seven
 * solid bars — the opposite of the truth, and indistinguishable from a loading
 * skeleton. The recessed plate is what gives the marks presence instead.
 */
export function InflowStrip({ trend, className }: InflowStripProps) {
  if (trend.length === 0) return null

  const totals = trend.map((point) => point.feed + point.discovery)
  const peak = Math.max(...totals)
  const windowTotal = totals.reduce((sum, value) => sum + value, 0)
  const lastIndex = trend.length - 1

  const scale = (value: number) => {
    if (value <= 0) return 0
    return Math.max(MIN_NONZERO_HEIGHT, Math.round((value / peak) * MAX_BAR_HEIGHT))
  }

  return (
    <SubPanel padded={false} className={cn('shrink-0 space-y-1.5 px-3 py-2', className)}>
      <div className="flex items-baseline justify-between gap-6">
        <EyebrowLabel tone="muted">Last 7 days</EyebrowLabel>
        <span className="font-mono text-xs tabular-nums text-alma-700">{windowTotal}</span>
      </div>
      <div
        className="flex items-end gap-[3px] border-b border-control-edge pb-px"
        style={{ height: MAX_BAR_HEIGHT }}
        role="img"
        aria-label={
          windowTotal === 0
            ? 'Nothing arrived in the last 7 days.'
            : `Inflow over the last 7 days: ${windowTotal} papers in total, peaking at ${peak} in a day.`
        }
      >
        {trend.map((point, index) => {
          const isToday = index === lastIndex
          const empty = point.feed + point.discovery === 0
          return (
            <Tooltip key={point.date}>
              <TooltipTrigger asChild>
                <div
                  className={cn(
                    'flex h-full w-2.5 cursor-default flex-col justify-end gap-px',
                    isToday && 'rounded-t-[2px] bg-alma-folio/[0.08]',
                  )}
                >
                  {empty ? (
                    // A day with nothing still occupies its slot, or the week
                    // silently shortens. One tick on the baseline says "this
                    // day happened, and was empty".
                    <div className="h-px w-full bg-control-edge-strong" />
                  ) : (
                    <>
                      <div
                        className={cn('w-full rounded-t-[1px]', HOME_TREND_SERIES.discovery)}
                        style={{ height: scale(point.discovery) }}
                      />
                      <div
                        className={cn('w-full', HOME_TREND_SERIES.feed)}
                        style={{ height: scale(point.feed) }}
                      />
                    </>
                  )}
                </div>
              </TooltipTrigger>
              <TooltipContent side="bottom">{dayLabel(point, isToday)}</TooltipContent>
            </Tooltip>
          )
        })}
      </div>
    </SubPanel>
  )
}
