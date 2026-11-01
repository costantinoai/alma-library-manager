import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { LostLanesNotice } from './LostLanesNotice'

describe('LostLanesNotice', () => {
  it('names each lost lane in plain words', () => {
    render(
      <LostLanesNotice
        lanes={[
          { lane: 'lexical', label: 'Lexical (local corpus + frontier)', reason: 'deadline' },
          { lane: 'graph', label: 'Graph (local references + PPR)', reason: 'deadline' },
        ]}
      />,
    )

    expect(screen.getByText('This deck is incomplete')).toBeInTheDocument()
    expect(screen.getByText(/built without Lexical, Graph,/)).toBeInTheDocument()
  })

  it('stays silent when every lane contributed', () => {
    const { container } = render(<LostLanesNotice lanes={[]} />)
    expect(container).toBeEmptyDOMElement()
  })
})
