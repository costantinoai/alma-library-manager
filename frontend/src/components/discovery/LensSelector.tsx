import type { Lens } from '@/api/client'
import { Button } from '@/components/ui/button'
import { EmptyState } from '@/components/ui/empty-state'

interface LensSelectorProps {
  lenses: Lens[]
  selectedLensId: string | null
  onSelectLens: (lensId: string) => void
}

export function LensSelector({ lenses, selectedLensId, onSelectLens }: LensSelectorProps) {
  if (lenses.length === 0) {
    return <EmptyState title="No lenses yet" description="Create one to start context-aware discovery." />
  }

  return (
    <div className="flex flex-wrap gap-2">
      {lenses.map((lens) => {
        const active = selectedLensId === lens.id
        return (
          <Button
            key={lens.id}
            variant={active ? 'default' : 'outline'}
            size="sm"
            type="button"
            onClick={() => onSelectLens(lens.id)}
          >
            {lens.name}
          </Button>
        )
      })}
    </div>
  )
}
