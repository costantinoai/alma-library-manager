import { StrictMode, type ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { waitForJob } from '@/api/client'
import { useAsyncRefresh } from './useAsyncRefresh'

vi.mock('@/api/client', () => ({
  waitForJob: vi.fn(),
  getApiErrorMessage: (error: Error) => error.message,
}))

function setup() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const invalidate = vi.spyOn(client, 'invalidateQueries').mockResolvedValue()
  const wrapper = ({ children }: { children: ReactNode }) => (
    <StrictMode><QueryClientProvider client={client}>{children}</QueryClientProvider></StrictMode>
  )
  return { wrapper, invalidate }
}

beforeEach(() => vi.clearAllMocks())

describe('useAsyncRefresh', () => {
  it('posts once in StrictMode, retries once, and does not force reopen', async () => {
    const { wrapper, invalidate } = setup()
    const request = vi.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValue({ job_id: null })
    const { result, rerender } = renderHook(({ enabled }) => useAsyncRefresh({ queryKey: ['test'], enabled, request }), {
      wrapper, initialProps: { enabled: true },
    })
    await waitFor(() => expect(result.current.buildError).toBe('offline'))
    expect(request).toHaveBeenCalledTimes(1)
    expect(request).toHaveBeenLastCalledWith(false)
    act(() => { result.current.refresh(); result.current.refresh() })
    await waitFor(() => expect(invalidate).toHaveBeenCalledTimes(1))
    expect(request).toHaveBeenCalledTimes(2)
    expect(request).toHaveBeenLastCalledWith(true)
    rerender({ enabled: false })
    rerender({ enabled: true })
    await waitFor(() => expect(request).toHaveBeenCalledTimes(3))
    expect(request).toHaveBeenLastCalledWith(false)
  })

  it('aborts polling on close and ignores late completion', async () => {
    const { wrapper, invalidate } = setup()
    let finish!: () => void
    vi.mocked(waitForJob).mockImplementation(() => new Promise(resolve => { finish = () => resolve({} as never) }))
    const request = vi.fn().mockResolvedValue({ job_id: 'job' })
    const { result, rerender } = renderHook(({ enabled }) => useAsyncRefresh({ queryKey: ['test'], enabled, request }), {
      wrapper, initialProps: { enabled: false },
    })
    expect(request).not.toHaveBeenCalled()
    rerender({ enabled: true })
    await waitFor(() => expect(waitForJob).toHaveBeenCalledTimes(1))
    const signal = vi.mocked(waitForJob).mock.calls[0][1]!.signal!
    rerender({ enabled: false })
    expect(signal.aborted).toBe(true)
    await act(async () => finish())
    expect(invalidate).not.toHaveBeenCalled()
    expect(result.current.building).toBe(false)
  })

  it('ignores an obsolete POST and uses changed identity without forcing', async () => {
    const { wrapper, invalidate } = setup()
    let finish!: (value: { job_id: string }) => void
    const request = vi.fn().mockImplementationOnce(() => new Promise(resolve => { finish = resolve }))
      .mockResolvedValue({ job_id: null })
    const { rerender } = renderHook(({ id }) => useAsyncRefresh({ queryKey: ['test', id], request }), {
      wrapper, initialProps: { id: 'a' },
    })
    await waitFor(() => expect(request).toHaveBeenCalledTimes(1))
    rerender({ id: 'b' })
    await waitFor(() => expect(invalidate).toHaveBeenCalledWith({ queryKey: ['test', 'b'] }))
    await act(async () => finish({ job_id: 'obsolete' }))
    expect(waitForJob).not.toHaveBeenCalled()
    expect(invalidate).toHaveBeenCalledTimes(1)
    expect(request).toHaveBeenLastCalledWith(false)
  })
})
