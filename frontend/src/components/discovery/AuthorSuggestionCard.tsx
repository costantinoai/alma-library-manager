import { useState } from 'react'
import {
  ChevronDown,
  ChevronUp,
  Info,
} from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { truncate } from '@/lib/utils'

import { SOURCE_TYPE_CONFIG, type AuthorSuggestion } from './constants'

interface AuthorSuggestionCardProps {
  suggestion: AuthorSuggestion
}

export function AuthorSuggestionCard({ suggestion }: AuthorSuggestionCardProps) {
  const [expanded, setExpanded] = useState(false)

  return (
    <Card className="border-slate-200">
      <CardContent className="p-4">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 flex-1">
            <p className="text-sm font-semibold text-alma-800">{suggestion.name}</p>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-xs text-slate-500">
              <span>{suggestion.paper_count} recommended papers</span>
              <span>•</span>
              <span>{Math.round(suggestion.avg_score)}% avg match</span>
            </div>
          </div>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 gap-1 px-2 text-xs text-alma-600 hover:bg-alma-50 hover:text-alma-700"
            onClick={() => setExpanded((prev) => !prev)}
          >
            <Info className="h-3 w-3" />
            Why
            {expanded ? <ChevronUp className="h-3 w-3" /> : <ChevronDown className="h-3 w-3" />}
          </Button>
        </div>

        {expanded && (
          <div className="mt-3 rounded-sm border border-[var(--color-border)] bg-parchment-50 p-3 text-xs text-slate-600">
            <p>
              Suggested because this author appears across{' '}
              <span className="font-semibold text-slate-800">{suggestion.paper_count}</span> high-scoring recommendations.
            </p>
            <p className="mt-1">
              Signals:{' '}
              <span className="font-medium text-slate-800">
                {suggestion.source_types.map((v) => SOURCE_TYPE_CONFIG[v]?.label ?? v).join(', ')}
              </span>
            </p>
            {suggestion.sample_titles.length > 0 && (
              <p className="mt-1">
                Examples:{' '}
                <span className="text-slate-700">
                  {suggestion.sample_titles.map((title) => `"${truncate(title, 80)}"`).join(', ')}
                </span>
              </p>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  )
}
