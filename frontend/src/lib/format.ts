/**
 * Formatting helpers shared across pages.
 *
 * The repeated `(x * 100).toFixed(N)` and `toLocaleDateString('en-GB',
 * { month: 'short', year: 'numeric' })` patterns lived in 8+ components
 * with no single source — divergence in fraction digits / locale was
 * just waiting to happen.
 */

/**
 * Format a unit-interval ratio (0..1) as a percent string with the
 * given number of fraction digits. The trailing `%` is included.
 *
 * Example: `formatPercent(0.387, 1)` → `"38.7%"`.
 *
 * Pass `withSign=false` to drop the trailing `%` (useful inside table
 * cells that already render the unit separately).
 */
export function formatPercent(
  ratio: number | null | undefined,
  fractionDigits = 0,
  { withSign = true }: { withSign?: boolean } = {},
): string {
  const value = typeof ratio === 'number' && Number.isFinite(ratio) ? ratio : 0
  const text = (value * 100).toFixed(fractionDigits)
  return withSign ? `${text}%` : text
}

/**
 * Format an ISO date / `YYYY-MM-DD` string as `"Mar 2024"`. Returns
 * the empty string when the input is falsy or unparseable.
 */
export function formatYearMonth(value: string | null | undefined): string {
  if (!value) return ''
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return ''
  return parsed.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })
}

/**
 * Format a paper's publication date at the precision the source actually
 * provides — never fabricating a day (see lessons.md "Don't fabricate missing
 * timestamps"):
 *   "2026-05-26" → "26 May 2026"   (full date when the day is known)
 *   "2026-05"    → "May 2026"      (month precision)
 *   "2026"       → "2026"          (year precision)
 * Returns '' when the input is falsy or unparseable. Built from the raw
 * Y/M/D components (not Date string parsing) so a local timezone behind UTC
 * can't shift "the 26th" back to the 25th.
 */
export function formatPaperDate(value: string | null | undefined): string {
  if (!value) return ''
  const s = value.trim()
  if (/^\d{4}$/.test(s)) return s
  const ym = s.match(/^(\d{4})-(\d{2})$/)
  if (ym) {
    return new Date(Number(ym[1]), Number(ym[2]) - 1, 1)
      .toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })
  }
  const ymd = s.match(/^(\d{4})-(\d{2})-(\d{2})/)
  if (ymd) {
    return new Date(Number(ymd[1]), Number(ymd[2]) - 1, Number(ymd[3]))
      .toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })
  }
  // Unknown shape: parse but don't assume day precision.
  const parsed = new Date(s)
  if (Number.isNaN(parsed.getTime())) return ''
  return parsed.toLocaleDateString('en-GB', { month: 'short', year: 'numeric' })
}
