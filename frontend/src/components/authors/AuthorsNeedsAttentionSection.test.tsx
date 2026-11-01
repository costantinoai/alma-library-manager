import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'

import { AuthorMergeDialog } from './AuthorMergeDialog'
import { AddIdentifierDialog, AuthorsNeedsAttentionSection } from './AuthorsNeedsAttentionSection'
import type { AuthorAttentionRouter } from './useAuthorAttentionRouter'
import { api, type Author, type AuthorNeedsAttentionRow } from '@/api/client'

const row = {
  author_id: 'a1',
  author_name: 'Ada Lovelace',
  reason_code: 'missing_history',
  status: 'resolved',
  suggested_action: { code: 'backfill_author', label: 'Backfill', hint: 'Queue history' },
} as unknown as AuthorNeedsAttentionRow

afterEach(() => vi.restoreAllMocks())

function router(overrides: Partial<AuthorAttentionRouter> = {}): AuthorAttentionRouter {
  return {
    openForRow: vi.fn(),
    openAuthor: vi.fn(),
    isRefreshingFor: () => false,
    fixAll: vi.fn(),
    isFixingAll: false,
    countAutoFixable: () => 0,
    dialogs: null,
    ...overrides,
  }
}

function show(overrides: Partial<AuthorAttentionRouter>) {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <AuthorsNeedsAttentionSection
        rows={[row]}
        isLoading={false}
        isError={false}
        router={router(overrides)}
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

describe('a row says why it is here', () => {
  it('shows the detail and names the kind of problem', () => {
    const conflict = {
      ...row,
      status: 'merge_conflict',
      reason: 'Merge kept one of two ORCID values',
      reason_detail: 'Primary: 1 · merged-in alt had 2.',
    } as AuthorNeedsAttentionRow
    render(
      <QueryClientProvider client={new QueryClient()}>
        <AuthorsNeedsAttentionSection
          rows={[conflict]}
          total={7}
          isLoading={false}
          isError={false}
          router={router()}
        />
      </QueryClientProvider>,
    )

    expect(screen.getByText('Conflict')).toBeTruthy()
    expect(screen.queryByText('Unresolved')).toBeNull()
    expect(screen.getByText('Primary: 1 · merged-in alt had 2.')).toBeTruthy()
    expect(screen.getByText(/first 1 of 7 rows/)).toBeTruthy()
  })
})

describe('AuthorMergeDialog', () => {
  it('survives two profiles with nothing to choose between', async () => {
    const twin = { name: 'Sam Twin', openalex_id: undefined, author_type: 'followed' }
    vi.spyOn(api, 'get').mockResolvedValue([
      { ...twin, id: 'a' },
      { ...twin, id: 'b' },
    ])
    render(
      <QueryClientProvider client={new QueryClient()}>
        <AuthorMergeDialog
          open
          onOpenChange={vi.fn()}
          primaryAuthor={{ ...twin, id: 'a' } as Author}
          allowedTargetIds={['b']}
          initialTargetId="b"
          offerNotSamePerson
        />
      </QueryClientProvider>,
    )

    expect(await screen.findByText(/No visible metadata discrepancies/)).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Not the same person' })).toBeTruthy()
  })
})

describe('AddIdentifierDialog', () => {
  const identityRow = { ...row, status: 'no_match', author_name: 'Ada Lovelace' } as AuthorNeedsAttentionRow

  it('cannot close while a save is running, so the result always lands', async () => {
    let finish: (value: unknown) => void = () => {}
    vi.spyOn(api, 'post').mockReturnValue(new Promise((resolve) => (finish = resolve)))
    const onClose = vi.fn()
    render(
      <QueryClientProvider client={new QueryClient()}>
        <AddIdentifierDialog row={identityRow} onClose={onClose} />
      </QueryClientProvider>,
    )

    fireEvent.change(screen.getByLabelText('ORCID iD'), { target: { value: '0000-0002-1825-0097' } })
    const save = screen.getByRole<HTMLButtonElement>('button', { name: 'Save' })
    fireEvent.click(save)
    await waitFor(() => expect(save.disabled).toBe(true))
    fireEvent.keyDown(screen.getByRole('dialog'), { key: 'Escape' })
    expect(onClose).not.toHaveBeenCalled()

    finish({ openalex_id: 'A1' })
    await waitFor(() => expect(onClose).toHaveBeenCalled())
  })

  it('stays open when the server refuses the identifier', async () => {
    vi.spyOn(api, 'post').mockRejectedValue(new Error('not an ORCID iD'))
    const onClose = vi.fn()
    render(
      <QueryClientProvider client={new QueryClient()}>
        <AddIdentifierDialog row={identityRow} onClose={onClose} />
      </QueryClientProvider>,
    )

    fireEvent.change(screen.getByLabelText('ORCID iD'), { target: { value: 'nope' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await waitFor(() => expect(api.post).toHaveBeenCalled())
    expect(onClose).not.toHaveBeenCalled()
    expect(screen.getByRole('dialog')).toBeTruthy()
  })
})
