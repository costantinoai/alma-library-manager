import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'

/**
 * Optional local-collection target shared by the import tabs and the online
 * search tab. When the user types a name, the saved/imported papers are grouped
 * into that local collection (created if it doesn't exist). Forwarded to the
 * backend as `collection_name`. This is the single definition — do not
 * hand-roll another label+input+hint block for the same purpose.
 */
export function CollectionNameField({
  id,
  value,
  onChange,
  label = 'Add to new collection (optional)',
  hint = 'Papers are grouped into this local collection (created if new).',
  compact = false,
}: {
  id: string
  value: string
  onChange: (value: string) => void
  label?: string
  hint?: string
  /** Denser styling for tight surfaces (e.g. the online-search tab header). */
  compact?: boolean
}) {
  return (
    <div className={compact ? 'space-y-1' : undefined}>
      <Label
        htmlFor={id}
        className={
          compact
            ? 'text-[11px] font-medium text-slate-600'
            : 'mb-1 block text-sm font-medium text-slate-700'
        }
      >
        {label}
      </Label>
      <Input
        id={id}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder="e.g. Systematic review 2026"
        className={compact ? 'h-9 text-sm' : undefined}
      />
      {!compact && <p className="mt-1 text-xs text-slate-400">{hint}</p>}
    </div>
  )
}
