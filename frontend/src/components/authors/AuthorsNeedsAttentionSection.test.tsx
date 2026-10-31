import { fireEvent, render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { describe, expect, it, vi } from 'vitest'

import { AuthorsNeedsAttentionSection } from './AuthorsNeedsAttentionSection'
import type { AuthorAttentionRouter } from './useAuthorAttentionRouter'
import type { AuthorNeedsAttentionRow } from '@/api/client'

const row = {
  author_id: 'a1',
  author_name: 'Ada Lovelace',
  reason_code: 'missing_history',
  status: 'resolved',
  suggested_action: { code: 'backfill_author', label: 'Backfill', hint: 'Queue history' },
} as unknown as AuthorNeedsAttentionRow

function show(router: Partial<AuthorAttentionRouter>) {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <AuthorsNeedsAttentionSection
        rows={[row]}
        isLoading={false}
        isError={false}
        router={{
          openForRow: vi.fn(),
          openAuthor: vi.fn(),
          isRefreshingFor: () => false,
          fixAll: vi.fn(),
          isFixingAll: false,
          countAutoFixable: () => 0,
          dialogs: null,
          ...router,
        }}
      />
    </QueryClientProvider>,
  )
}

describe('AuthorsNeedsAttentionSection', () => {
  it('opens the author from the name and runs no action', () => {
    const openForRow = vi.fn()
    const openAuthor = vi.fn()
    show({ openForRow, openAuthor })

    fireEvent.click(screen.getByRole('button', { name: 'Ada Lovelace' }))

    expect(openAuthor).toHaveBeenCalledWith(row)
    expect(openForRow).not.toHaveBeenCalled()
  })

  it('still runs the action from the action button', () => {
    const openForRow = vi.fn()
    show({ openForRow })

    fireEvent.click(screen.getByRole('button', { name: 'Backfill' }))

    expect(openForRow).toHaveBeenCalledWith(row)
  })
})
