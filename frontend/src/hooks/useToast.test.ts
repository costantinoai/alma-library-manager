import { beforeEach, describe, expect, it, vi } from 'vitest'
import { toast as sonnerToast } from 'sonner'

import { errorToast, toast } from './useToast'

vi.mock('sonner', () => ({
  toast: { success: vi.fn(() => 1), error: vi.fn(() => 2), dismiss: vi.fn() },
}))

const durationOf = (fn: unknown) =>
  (vi.mocked(fn as typeof sonnerToast.success).mock.lastCall?.[1] as { duration: number }).duration

describe('toast duration', () => {
  beforeEach(() => vi.clearAllMocks())

  it('keeps a plain toast up long enough to read', () => {
    toast({ title: 'Saved', description: 'Added to your Library.' })
    expect(durationOf(sonnerToast.success)).toBe(6_000)
  })

  it('keeps an error longer', () => {
    errorToast('Action failed', 'The server said no.')
    expect(durationOf(sonnerToast.error)).toBe(10_000)
  })

  it('keeps a toast with an action longer, because Undo must stay reachable', () => {
    toast({ title: 'Dismissed', action: { label: 'Undo', onClick: () => undefined } })
    expect(durationOf(sonnerToast.success)).toBe(10_000)
  })

  it('recomputes the duration when an update adds an action', () => {
    toast({ title: 'Dismissed' }).update({ action: { label: 'Undo', onClick: () => undefined } })
    expect(durationOf(sonnerToast.success)).toBe(10_000)
  })
})
