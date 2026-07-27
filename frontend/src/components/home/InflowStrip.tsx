import type { HomeTrendPoint } from '@/api/client'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { HOME_SECTION_THEMES, HOME_TREND_SERIES } from '@/lib/palette'
import { cn } from '@/lib/utils'

/** Tallest a column may draw, in px. Matches the strip's numeral height, so
 *  the chart reads as one more figure in the row rather than a widget. */
const MAX_BAR_HEIGHT = 28
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
 * Seven days of inflow, as the FINAL CELL of Home's scoreboard strip: the bars
 * sit exactly where the other cells put their numeral, over the same label
 * treatment, so the week's shape is read as one more figure rather than as a
 * chart bolted beside the heading (which is how it looked in its own plate).
 *
 * Answers the question the headline figures cannot: *is today normal?* A
 * zero-Feed morning means nothing on its own and everything next to six busy
 * days. Columns are scaled against the window's own maximum — this is a shape,
 * not a measurement — so the week's total is printed as the cell's number and
 * every column carries its exact figures on hover.
 *
 * Feed sits at the bottom of each column and Discovery on top, in the same two
 * hues those surfaces wear elsewhere (`HOME_TREND_SERIES`). Today's column is
 * marked by a folio wash rather than a third colour, because the colours are
 * already spent saying WHICH surface.
 *
 * **Empty days are a baseline tick, not a filled rail.** The first version drew
 * a full-height track behind every column, so a quiet week rendered as seven
 * solid bars — the opposite of the truth, and indistinguishable from a loading
 * skeleton. Presence comes from the printed total beside them instead.
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
    <div className={cn('min-w-0', className)}>
      <div
        className="flex items-end gap-[3px]"
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
        {/* The cell's number, baseline-aligned with the bars it describes. */}
        <span className="ml-2 self-end font-brand text-[1.75rem] font-normal leading-none tabular-nums text-alma-800">
          {windowTotal}
        </span>
      </div>
      <p className="mt-1.5 truncate text-xs text-slate-600">in the last 7 days</p>
      <p className="mt-0.5 text-[11px]">
        <span className={HOME_SECTION_THEMES.feed.title}>Feed</span>
        <span className="text-slate-400"> + </span>
        <span className={HOME_SECTION_THEMES.discovery.title}>Discovery</span>
      </p>
    </div>
  )
}
