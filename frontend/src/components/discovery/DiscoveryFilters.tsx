import {
  ArrowUpDown,
  Search,
  Sparkles,
} from 'lucide-react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'

import { STATUS_FILTERS, type SortField, type StatusFilter } from './constants'

interface DiscoveryFiltersProps {
  statusFilter: StatusFilter
  sortField: SortField
  searchText: string
  semanticEnabled: boolean
  aiAvailable: boolean
  onStatusFilterChange: (value: StatusFilter) => void
  onSortFieldChange: (value: SortField) => void
  onSearchTextChange: (value: string) => void
  onSemanticToggle: () => void
}

export function DiscoveryFilters({
  statusFilter,
  sortField,
  searchText,
  semanticEnabled,
  aiAvailable,
  onStatusFilterChange,
  onSortFieldChange,
  onSearchTextChange,
  onSemanticToggle,
}: DiscoveryFiltersProps) {
  return (
    <div className="space-y-3 rounded-sm border border-[var(--color-border)] bg-alma-paper p-4 shadow-paper-sm shadow-sm">
      <div className="flex flex-wrap items-center gap-3">
        {/* Status filter */}
        <div className="flex items-center gap-2">
          <span className="text-xs font-medium text-slate-500">Status:</span>
          {STATUS_FILTERS.map((f) => (
            <button
              key={f.value}
              type="button"
              onClick={() => onStatusFilterChange(f.value)}
              className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                statusFilter === f.value
                  ? 'bg-slate-800 text-white'
                  : 'bg-parchment-100 text-slate-600 hover:bg-slate-200'
              }`}
            >
              {f.label}
            </button>
          ))}
        </div>

        <div className="h-5 w-px bg-slate-200" />

        {/* Sort */}
        <div className="flex items-center gap-1.5">
          <ArrowUpDown className="h-3.5 w-3.5 text-slate-400" />
          <Select value={sortField} onValueChange={(value) => onSortFieldChange(value as SortField)}>
            <SelectTrigger className="h-8 w-28 text-xs">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="score">Score</SelectItem>
              <SelectItem value="date">Date</SelectItem>
              <SelectItem value="title">Title</SelectItem>
            </SelectContent>
          </Select>
        </div>

        {/* Search */}
        <div className="ml-auto flex flex-1 items-center gap-2 sm:max-w-md">
          <div className="relative flex-1">
            <Search className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400" />
            <Input
              placeholder="Search title or author..."
              value={searchText}
              onChange={(e) => onSearchTextChange(e.target.value)}
              className="h-8 pl-8 text-xs"
            />
          </div>
          <Button
            variant={semanticEnabled ? 'gold' : 'outline'}
            size="sm"
            onClick={onSemanticToggle}
            disabled={!aiAvailable}
            title={aiAvailable ? 'Toggle semantic search (AI-powered)' : 'Enable an AI provider in Settings to use semantic search'}
            className="shrink-0"
          >
            <Sparkles className="h-3.5 w-3.5" />
            Semantic
          </Button>
        </div>
      </div>
    </div>
  )
}
