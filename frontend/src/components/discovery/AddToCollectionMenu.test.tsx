import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { AddToCollectionMenu } from './AddToCollectionMenu'

const toast = vi.fn()

vi.stubGlobal(
  'ResizeObserver',
  class ResizeObserver {
    observe() {}
    unobserve() {}
    disconnect() {}
  },
)

vi.mock('@/api/client', () => ({
  listCollections: vi.fn().mockResolvedValue([
    {
      id: 'collection-1',
      name: 'Methods',
      description: null,
      color: '#123456',
      created_at: '2026-07-16T12:00:00',
      item_count: 2,
    },
  ]),
  createCollection: vi.fn(),
}))

vi.mock('@/hooks/useToast', () => ({
  useToast: () => ({ toast }),
}))

describe('AddToCollectionMenu', () => {
  beforeEach(() => toast.mockReset())

  it('surfaces a failed save and keeps the selection open for retry', async () => {
    const user = userEvent.setup()
    const onConfirm = vi.fn().mockRejectedValue(new Error('save failed'))
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    })
    render(
      <QueryClientProvider client={client}>
        <AddToCollectionMenu onConfirm={onConfirm} />
      </QueryClientProvider>,
    )

    await user.click(screen.getByRole('button', { name: 'Collections' }))
    await user.click(await screen.findByRole('checkbox', { name: /Methods/ }))
    await user.click(screen.getByRole('button', { name: 'Add (1)' }))

    await waitFor(() => {
      expect(toast).toHaveBeenCalledWith({
        title: 'Could not add to collections',
        description: 'Nothing changed. Try again.',
        variant: 'destructive',
      })
    })
    expect(screen.getByRole('button', { name: 'Add (1)' })).toBeInTheDocument()
  })
})
