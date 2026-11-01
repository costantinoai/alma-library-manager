import { createElement, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { renderHook, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import { updateLens, type Lens } from '@/api/client'
import { useBranchControls } from '@/hooks/useBranchControls'

vi.mock('@/api/client', async (original) => ({
  ...(await original<typeof import('@/api/client')>()),
  updateLens: vi.fn(async (id: string) => ({ id })),
}))

const wrapper = ({ children }: { children: ReactNode }) =>
  createElement(QueryClientProvider, { client: new QueryClient() }, children)

describe('useBranchControls', () => {
  it('leaves a temperature nobody set unset when a pin is saved', async () => {
    const lens = { id: 'l', branch_controls: { temperature: null, resolution: null, pinned: [] } } as unknown as Lens
    const { result } = renderHook(() => useBranchControls(lens), { wrapper })

    result.current.setBranchState('b1', 'pinned')

    await waitFor(() => expect(updateLens).toHaveBeenCalled())
    const sent = vi.mocked(updateLens).mock.calls[0][1] as { branch_controls: Record<string, unknown> }
    expect(sent.branch_controls.temperature).toBeNull()
    expect(sent.branch_controls.resolution).toBeNull()
    expect(sent.branch_controls.pinned).toEqual(['b1'])
  })
})
