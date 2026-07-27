/**
 * Sidebar metrics — the ONE place the desk's left rail states how wide it is.
 *
 * Three things have to agree on that number: the rail itself, the main column
 * that clears it, and the Activity dock pinned to the bottom of the window.
 * They each spelled `260px` on their own, so collapsing the rail widened the
 * content but left the dock stranded 188px short of the left edge (user
 * report 2026-07-27). Anything that must sit flush beside the rail reads its
 * inset from here instead of hard-coding one.
 *
 * These are Tailwind CLASS strings, not numbers, because Tailwind only emits
 * utilities it can see literally in the source — an interpolated
 * `lg:left-[${width}px]` compiles to nothing.
 *
 * Below the `lg` breakpoint the rail is an overlay drawer, so nothing insets:
 * every map's mobile side is the bare, full-width default.
 */

/** The rail's own width. Always 260 as a mobile drawer; narrows when collapsed. */
export const SIDEBAR_RAIL_WIDTH = {
  expanded: 'w-[260px]',
  collapsed: 'w-[260px] lg:w-[72px]',
} as const

/** Left padding for the scrolling main column, so content clears the rail. */
export const SIDEBAR_CONTENT_INSET = {
  expanded: 'lg:pl-[260px]',
  collapsed: 'lg:pl-[72px]',
} as const

/** Left edge for a `fixed` element docked across the bottom (Activity). */
export const SIDEBAR_DOCK_INSET = {
  expanded: 'lg:left-[260px]',
  collapsed: 'lg:left-[72px]',
} as const

/** Pick the inset for the rail's current state. */
export function sidebarInset(
  map: Record<'expanded' | 'collapsed', string>,
  collapsed: boolean,
): string {
  return map[collapsed ? 'collapsed' : 'expanded']
}
