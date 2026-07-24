import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { StatusBadge } from './status-badge'
import { monitorHealthTone, severityTone } from './status-badge-tones'

describe('StatusBadge', () => {
  it('renders its children', () => {
    render(<StatusBadge>Ready</StatusBadge>)
    expect(screen.getByText('Ready')).toBeInTheDocument()
  })

  it('applies the neutral tone classes by default', () => {
    render(<StatusBadge>Idle</StatusBadge>)
    // Neutral is the QUIET chip, but it must never borrow the warm paper
    // ladder — a cream-on-cream chip disappears into its own card. It's a
    // translucent COOL INK wash instead (2026-07-25 colour contract).
    expect(screen.getByText('Idle')).toHaveClass('bg-alma-800/[0.06]')
    expect(screen.getByText('Idle')).not.toHaveClass('bg-surface-2')
  })

  it('switches visual treatment with the tone prop', () => {
    render(<StatusBadge tone="positive">Healthy</StatusBadge>)
    // Positive routes through the success semantic token.
    expect(screen.getByText('Healthy')).toHaveClass('text-success-800')
  })

  it('merges a caller className', () => {
    render(<StatusBadge className="ml-2">X</StatusBadge>)
    expect(screen.getByText('X')).toHaveClass('ml-2')
  })

  it('renders the category glyph when an icon is supplied', () => {
    // Colour carries valence, the icon carries category — both must survive.
    const Dot = ({ className }: { className?: string }) => (
      <svg data-testid="chip-glyph" className={className} />
    )
    render(
      <StatusBadge tone="accent" icon={Dot}>
        Cited together
      </StatusBadge>,
    )
    expect(screen.getByTestId('chip-glyph')).toBeInTheDocument()
  })
})

describe('tone mapping helpers', () => {
  it('monitorHealthTone maps health → tone', () => {
    expect(monitorHealthTone('ready')).toBe('positive')
    expect(monitorHealthTone('disabled')).toBe('neutral')
    expect(monitorHealthTone('degraded')).toBe('warning')
    expect(monitorHealthTone(undefined)).toBe('warning')
  })

  it('severityTone returns a valid StatusBadge tone', () => {
    const tone = severityTone('critical')
    expect(['neutral', 'positive', 'negative', 'warning', 'info', 'accent']).toContain(tone)
  })
})
