/**
 * Sort comparators shared across pages.
 *
 * The "weighted DESC" comparator was open-coded in three signal-rendering
 * components. Centralizing keeps behavior consistent and makes it
 * obvious where to add a tie-breaker if one is ever needed.
 */

/**
 * Comparator that orders entries with a numeric ``weighted`` field in
 * descending order. Use as ``items.sort(byWeightedDesc)`` /
 * ``items.sort(byWeightedDesc((s) => s.signal.weighted))``.
 *
 * Pass an accessor when ``weighted`` lives on a nested field.
 */
export function byWeightedDesc<T extends { weighted: number }>(): (a: T, b: T) => number
export function byWeightedDesc<T>(accessor: (item: T) => number): (a: T, b: T) => number
export function byWeightedDesc<T>(
  accessor?: (item: T) => number,
): (a: T, b: T) => number {
  const get = accessor ?? ((item: T) => (item as unknown as { weighted: number }).weighted)
  return (a, b) => get(b) - get(a)
}
