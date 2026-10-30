import { createElement, type ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import * as client from '@/api/client'
import { useWalkthrough } from './useWalkthrough'

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof client>()),
  getWalkthroughs: vi.fn(),
  importWalkthroughFlags: vi.fn(),
  recordWalkthroughOutcome: vi.fn(),
  replayWalkthrough: vi.fn(),
}))

function state(id: string, pending: boolean): client.WalkthroughState {
  return {
    id,
    pending,
    current_version: 1,
    outcome: pending ? null : 'done',
    recorded_version: pending ? null : 1,
    at: null,
    source: null,
  }
}

function serve(...states: client.WalkthroughState[]) {
  vi.mocked(client.getWalkthroughs).mockResolvedValue({ walkthroughs: states })
}

function render() {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = ({ children }: { children: ReactNode }) =>
    createElement(QueryClientProvider, { client: qc }, children)
  return renderHook(() => useWalkthrough('feed'), { wrapper })
}

const OVER_THE_OPEN_DELAY = 900
const settle = () => new Promise((resolve) => setTimeout(resolve, OVER_THE_OPEN_DELAY))

describe('useWalkthrough', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    localStorage.clear()
    vi.mocked(client.importWalkthroughFlags).mockResolvedValue({ imported: [], ignored: [] })
  })

  it('opens when the server reports it pending', async () => {
    serve(state('welcome', false), state('feed', true))
    const { result } = render()
    await waitFor(() => expect(result.current.open).toBe(true), { timeout: 2000 })
  })

  it('stays closed when the server has a verdict, whatever the browser holds', async () => {
    serve(state('welcome', false), state('feed', false))
    const { result } = render()
    await act(settle)
    expect(result.current.open).toBe(false)
  })

  it('shows nothing when the state request fails', async () => {
    vi.mocked(client.getWalkthroughs).mockRejectedValue(new Error('down'))
    const { result } = render()
    await act(settle)
    expect(result.current.open).toBe(false)
  })

  it('never opens over the first-run wizard', async () => {
    serve(state('welcome', true), state('feed', true))
    const { result } = render()
    await act(settle)
    expect(result.current.open).toBe(false)
  })

  it('imports old flags once, then deletes them', async () => {
    localStorage.setItem('alma.tour.feed.completed', 'done')
    localStorage.setItem('alma.onboarding.state', '{"step":3}')
    serve(state('welcome', false), state('feed', false))
    render()
    await waitFor(() => expect(client.getWalkthroughs).toHaveBeenCalled())
    expect(client.importWalkthroughFlags).toHaveBeenCalledTimes(1)
    expect(client.importWalkthroughFlags).toHaveBeenCalledWith(['feed'])
    expect(localStorage.getItem('alma.tour.feed.completed')).toBeNull()
    expect(localStorage.getItem('alma.onboarding.state')).toBeNull()
  })

  it('keeps old flags and shows nothing when the import fails', async () => {
    localStorage.setItem('alma.tour.feed.completed', 'done')
    vi.mocked(client.importWalkthroughFlags).mockRejectedValue(new Error('down'))
    serve(state('welcome', false), state('feed', true))
    const { result } = render()
    await act(settle)
    expect(result.current.open).toBe(false)
    expect(client.getWalkthroughs).not.toHaveBeenCalled()
    expect(localStorage.getItem('alma.tour.feed.completed')).toBe('done')
  })

  it('records the outcome and does not reopen when the save fails', async () => {
    serve(state('welcome', false), state('feed', true))
    vi.mocked(client.recordWalkthroughOutcome).mockRejectedValue(new Error('down'))
    const { result } = render()
    await waitFor(() => expect(result.current.open).toBe(true), { timeout: 2000 })
    act(() => result.current.close('skipped'))
    expect(client.recordWalkthroughOutcome).toHaveBeenCalledWith('feed', 'skipped')
    await act(settle)
    expect(result.current.open).toBe(false)
  })

  it('asks the server to replay before opening', async () => {
    serve(state('welcome', false), state('feed', false))
    vi.mocked(client.replayWalkthrough).mockResolvedValue(state('feed', true))
    const { result } = render()
    await waitFor(() => expect(client.getWalkthroughs).toHaveBeenCalled())
    await act(() => result.current.relaunch())
    expect(client.replayWalkthrough).toHaveBeenCalledWith('feed')
    expect(result.current.open).toBe(true)
  })
})
