import type { ReactNode } from 'react'

import { PageThemeContext } from '@/components/ui/page-theme-context'
import type { PageTheme } from '@/lib/palette'

/**
 * PageThemeProvider — hands the current page's identity colour down the tree.
 *
 * Set ONCE, in `AppShell`, from the page currently routed. Every piece of
 * structural chrome below reads it: the `PageIntro` medallion, a banded
 * `PageSection`'s glyph and count pill, a `DisclosurePanel`'s medallion.
 *
 * It is a context and not a prop because the first attempt WAS a prop, and a
 * prop has to be remembered at every call site. Three icons on Discovery got
 * the page hue and the other hundred stayed folio — one page wearing two
 * icon-colour systems at once, which reads as a bug rather than a scheme
 * (user report 2026-07-27). A page cannot half-adopt a context.
 */
export function PageThemeProvider({
  theme,
  children,
}: {
  theme: PageTheme | null
  children: ReactNode
}) {
  return <PageThemeContext.Provider value={theme}>{children}</PageThemeContext.Provider>
}
