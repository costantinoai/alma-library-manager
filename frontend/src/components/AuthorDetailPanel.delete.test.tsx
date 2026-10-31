import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { AuthorDetailPanel } from './AuthorDetailPanel'
import { softRemoveAuthor, type Author } from '@/api/client'

vi.mock('@/api/client', async (original) => {
  const actual = await original<typeof import('@/api/client')>()
  const pending = () => new Promise(() => undefined)
  const stubbed = Object.fromEntries(
    Object.entries(actual).map(([name, value]) => [name, typeof value === 'function' ? vi.fn(pending) : value]),
  )
  return { ...stubbed, softRemoveAuthor: vi.fn(async () => ({})) }
})

const author = { id: 'a1', name: 'Ada Lovelace', added_at: '2026-01-01', author_type: 'followed' } as Author

function show() {
  render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <AuthorDetailPanel author={author} open onOpenChange={() => undefined} />
    </QueryClientProvider>,
  )
}

describe('AuthorDetailPanel Delete', () => {
  beforeEach(() => vi.mocked(softRemoveAuthor).mockClear())

  it('asks first and removes nothing on the first click', () => {
    show()

    fireEvent.click(screen.getByRole('button', { name: 'Delete' }))

    expect(softRemoveAuthor).not.toHaveBeenCalled()
    expect(screen.getByText('Remove Ada Lovelace?')).toBeInTheDocument()
  })

  it('removes after the confirmation', async () => {
    show()

    fireEvent.click(screen.getByRole('button', { name: 'Delete' }))
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))

    await waitFor(() => expect(softRemoveAuthor).toHaveBeenCalledTimes(1))
  })
})
