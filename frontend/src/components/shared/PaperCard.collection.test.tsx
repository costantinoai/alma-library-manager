import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { PaperCard } from './PaperCard'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>()
  return {
    ...actual,
    listCollections: vi.fn().mockResolvedValue([]),
    createCollection: vi.fn(),
  }
})

describe('PaperCard collection action', () => {
  it('renders the collection chooser inside the same action row', () => {
    const client = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    })
    render(
      <QueryClientProvider client={client}>
        <PaperCard
          paper={{ id: 'paper-1', title: 'A paper' }}
          onAdd={() => undefined}
          onAddToCollections={() => undefined}
        />
      </QueryClientProvider>,
    )

    const actionRow = screen.getByTestId('paper-actions')
    expect(actionRow).toContainElement(screen.getByRole('button', { name: 'Save to library' }))
    expect(actionRow).toContainElement(screen.getByRole('button', { name: 'Collections' }))
  })
})
