import {
  Eye,
  EyeOff,
  Heart,
  Loader2,
  Sparkles,
  X,
} from 'lucide-react'

import { Card } from '@/components/ui/card'
import type { DiscoveryStats } from './constants'

interface DiscoveryStatsBarProps {
  stats: DiscoveryStats | undefined
  isLoading: boolean
}

export function DiscoveryStatsBar({ stats, isLoading }: DiscoveryStatsBarProps) {
  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-2">
        <Loader2 className="h-4 w-4 animate-spin text-slate-400" />
      </div>
    )
  }

  if (!stats) return null

  // "Untouched" = recommendations the user has not yet acted on. The
  // backend field is `actioned` (renamed 2026-04-23 — previously `seen`,
  // which wrongly implied an impression/view counter; we only record
  // explicit save/like/love/dismiss actions here).
  const untouched = stats.total - stats.actioned

  return (
    <Card className="flex flex-wrap items-center gap-4 px-5 py-3">
      <div className="flex items-center gap-2">
        <Sparkles className="h-4 w-4 text-purple-500" />
        <span className="text-sm text-slate-500">Total</span>
        <span className="text-sm font-bold text-alma-800">{stats.total}</span>
      </div>
      <div className="h-4 w-px bg-slate-200" />
      <div className="flex items-center gap-2">
        <Heart className="h-4 w-4 text-pink-500" />
        <span className="text-sm text-slate-500">Liked</span>
        <span className="text-sm font-bold text-alma-800">{stats.liked}</span>
      </div>
      <div className="h-4 w-px bg-slate-200" />
      <div className="flex items-center gap-2">
        <X className="h-4 w-4 text-slate-400" />
        <span className="text-sm text-slate-500">Dismissed</span>
        <span className="text-sm font-bold text-alma-800">{stats.dismissed}</span>
      </div>
      <div className="h-4 w-px bg-slate-200" />
      <div className="flex items-center gap-2">
        {untouched > 0 ? (
          <EyeOff className="h-4 w-4 text-alma-500" />
        ) : (
          <Eye className="h-4 w-4 text-green-500" />
        )}
        <span className="text-sm text-slate-500">Untouched</span>
        <span className="text-sm font-bold text-alma-800">{untouched > 0 ? untouched : 0}</span>
      </div>
    </Card>
  )
}
