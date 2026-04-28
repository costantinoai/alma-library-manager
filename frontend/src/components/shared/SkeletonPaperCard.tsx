import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/utils'

interface SkeletonPaperCardProps {
  compact?: boolean
  className?: string
}

export function SkeletonPaperCard({ compact = false, className }: SkeletonPaperCardProps) {
  const padding = compact ? 'p-3' : 'p-4'
  return (
    <Card className={className}>
      <div className={padding}>
        <div className="flex items-start gap-3">
          <Skeleton className="h-7 w-7 shrink-0 rounded-full" />
          <div className="min-w-0 flex-1 space-y-2">
            <Skeleton className="h-4 w-3/4" />
            <Skeleton className="h-3 w-1/2" />
            <div className="mt-2 flex gap-1.5">
              <Skeleton className="h-5 w-14 rounded-full" />
              <Skeleton className="h-5 w-10 rounded-full" />
              <Skeleton className="h-5 w-20 rounded-full" />
            </div>
          </div>
        </div>
      </div>
    </Card>
  )
}

interface SkeletonListProps {
  count?: number
  compact?: boolean
  className?: string
}

export function SkeletonList({ count = 4, compact = false, className }: SkeletonListProps) {
  return (
    <div className={cn('space-y-3', className)}>
      {Array.from({ length: count }, (_, i) => (
        <SkeletonPaperCard key={i} compact={compact} />
      ))}
    </div>
  )
}
