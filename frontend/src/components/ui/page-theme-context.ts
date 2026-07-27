import { createContext, useContext } from 'react'

import type { PageTheme } from '@/lib/palette'

export const PageThemeContext = createContext<PageTheme | null>(null)

/** Current page identity colour; folio fallback lives with each consumer. */
export function usePageTheme(): PageTheme | null {
  return useContext(PageThemeContext)
}
