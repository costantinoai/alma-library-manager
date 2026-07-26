import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { MapAuthorPopup } from './MapAuthorPopup'
import { MapPaperPopup } from './MapPaperPopup'

function withQueryClient(node: React.ReactNode) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      {node}
    </QueryClientProvider>,
  )
}

describe('map click popups', () => {
  it('shows paper context and routes every compact action through host callbacks', () => {
    const close = vi.fn()
    const like = vi.fn()
    const love = vi.fn()
    const dislike = vi.fn()
    const add = vi.fn()
    const queue = vi.fn()
    const collections = vi.fn()
    const goToPaper = vi.fn()

    withQueryClient(
      <MapPaperPopup
        paper={{
          id: 'paper-1',
          title: 'A paper on shared map semantics',
          authors: 'Ada Lovelace; Alan Turing',
          tldr: 'A compact explanation of why this result matters.',
          year: 2026,
          journal: 'Journal of Maps',
          citedByCount: 1234,
          score: 87.4,
          statusLabel: 'Suggestion',
          branchLabel: 'Methods',
          clusterLabel: 'Semantic systems',
          neighbours: [
            {
              id: 'paper-2',
              title: 'A nearby paper',
              relation: 'Nearby · same cluster',
            },
          ],
        }}
        onClose={close}
        onGoToPaper={goToPaper}
        onQueue={queue}
        onAdd={add}
        onLike={like}
        onLove={love}
        onDislike={dislike}
        onAddToCollections={collections}
      />,
    )

    expect(screen.getByRole('dialog', { name: /Actions for A paper/ })).toBeInTheDocument()
    expect(screen.getByText('Internal score')).toBeInTheDocument()
    expect(screen.getByText('87')).toBeInTheDocument()
    expect(screen.getByText('A compact explanation of why this result matters.')).toBeInTheDocument()
    expect(screen.getByText('1,234 citations')).toBeInTheDocument()
    expect(screen.getByText('Methods')).toBeInTheDocument()
    expect(screen.getByText('Semantic systems')).toBeInTheDocument()
    expect(screen.getByText('A nearby paper')).toBeInTheDocument()
    expect(screen.getByText('More context')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('button', { name: 'Save to library' }))
    fireEvent.click(screen.getByRole('button', { name: 'Add to reading list — decide later' }))
    fireEvent.click(screen.getByRole('button', { name: 'Like — save to library with a positive signal' }))
    fireEvent.click(screen.getByRole('button', { name: 'Love — save to library with a strong positive signal' }))
    fireEvent.click(screen.getByRole('button', { name: 'Negative signal — keeps the paper visible' }))
    fireEvent.click(screen.getByRole('button', { name: 'Close paper popup' }))
    fireEvent.click(screen.getByRole('button', { name: 'Go to paper' }))

    expect(add).toHaveBeenCalledTimes(1)
    expect(queue).toHaveBeenCalledTimes(1)
    expect(like).toHaveBeenCalledTimes(1)
    expect(love).toHaveBeenCalledTimes(1)
    expect(dislike).toHaveBeenCalledTimes(1)
    expect(close).toHaveBeenCalledTimes(1)
    expect(goToPaper).toHaveBeenCalledTimes(1)
    expect(screen.getByRole('button', { name: 'Collections' })).toBeInTheDocument()
  })

  it('shows author metrics and makes follow/profile explicit actions', () => {
    const follow = vi.fn()
    const details = vi.fn()

    render(
      <MapAuthorPopup
        author={{
          id: 'author-1',
          name: 'Grace Hopper',
          affiliation: 'US Navy',
          publicationCount: 42,
          hIndex: 17,
          score: 74,
          clusterLabel: 'Programming languages',
          suggestion: {
            source: 'OpenAlex related authors',
            reasons: ['Co-author of Ada Lovelace', 'Shares 3 topics'],
            score: 82,
          },
        }}
        isFollowed={false}
        onFollow={follow}
        onUnfollow={() => undefined}
        onOpenDetails={details}
        onClose={() => undefined}
      />,
    )

    expect(screen.getByText('42 papers')).toBeInTheDocument()
    expect(screen.getByText('h-index 17')).toBeInTheDocument()
    expect(screen.getByText('Internal score')).toBeInTheDocument()
    expect(screen.getByText('OpenAlex related authors')).toBeInTheDocument()
    expect(screen.getByText(/Co-author of Ada Lovelace/)).toBeInTheDocument()
    expect(screen.getByText('More context')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Follow' }))
    fireEvent.click(screen.getByRole('button', { name: /Open full profile/ }))

    expect(follow).toHaveBeenCalledTimes(1)
    expect(details).toHaveBeenCalledTimes(1)
  })

  it('routes a second click on an active reaction through the shared undo action', () => {
    const dislike = vi.fn()
    const undo = vi.fn()

    withQueryClient(
      <MapPaperPopup
        paper={{ id: 'paper-1', title: 'A disliked paper' }}
        onClose={() => undefined}
        onQueue={() => undefined}
        onAdd={() => undefined}
        onLike={() => undefined}
        onLove={() => undefined}
        onDislike={dislike}
        onUndo={undo}
        onAddToCollections={() => undefined}
        reaction="dislike"
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Negative signal — keeps the paper visible' }))

    expect(undo).toHaveBeenCalledWith('rating')
    expect(dislike).not.toHaveBeenCalled()
  })
})
