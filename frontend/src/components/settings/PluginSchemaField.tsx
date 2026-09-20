import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import type { PluginSchemaProperty } from '@/api/client'
import type { ConfigValue } from '@/components/settings/pluginSchema'
import { docsUrl } from '@/lib/docs'
import { cn } from '@/lib/utils'

/**
 * One configuration field, rendered from the plugin's own JSON schema — the
 * backend's Pydantic model is the single source of truth for what a plugin
 * can be asked, so nothing here is written per plugin.
 */
export function SchemaField({
  name,
  schema,
  value,
  onChange,
}: {
  name: string
  schema: PluginSchemaProperty
  value: ConfigValue
  onChange: (value: ConfigValue) => void
}) {
  const id = `plugin-field-${name}`
  if (schema.type === 'boolean') {
    return (
      <div className="flex items-start justify-between gap-4">
        <div>
          <Label htmlFor={id}>{schema.title ?? name}</Label>
          <FieldHelp schema={schema} className="mt-1" />
        </div>
        <Switch id={id} checked={Boolean(value)} onCheckedChange={onChange} />
      </div>
    )
  }

  const numeric = schema.type === 'number' || schema.type === 'integer'
  return (
    <div className="space-y-1.5">
      <Label htmlFor={id}>{schema.title ?? name}</Label>
      <Input
        id={id}
        type={schema['x-alma-secret'] ? 'password' : numeric ? 'number' : 'text'}
        value={String(value ?? '')}
        min={schema.minimum}
        max={schema.maximum}
        step={schema.type === 'integer' ? 1 : schema['x-alma-step']}
        onChange={(event) => {
          if (!numeric) {
            onChange(event.target.value)
            return
          }
          const parsed = schema.type === 'integer'
            ? Number.parseInt(event.target.value, 10)
            : Number.parseFloat(event.target.value)
          onChange(Number.isFinite(parsed) ? parsed : 0)
        }}
      />
      <FieldHelp schema={schema} />
    </div>
  )
}

/** A field's description and its help links (`x-alma-links`), under the input. */
function FieldHelp({ schema, className }: { schema: PluginSchemaProperty; className?: string }) {
  const links = schema['x-alma-links'] ?? []
  if (!schema.description && links.length === 0) return null
  return (
    <div className={cn('space-y-1 text-xs text-slate-500', className)}>
      {schema.description && <p>{schema.description}</p>}
      {links.length > 0 && (
        <p className="flex flex-wrap gap-x-3 gap-y-1">
          {links.map((link) => (
            <a
              key={link.url}
              href={docsUrl(link.url)}
              target="_blank"
              rel="noreferrer"
              className="text-accent hover:underline"
            >
              {link.label} ↗
            </a>
          ))}
        </p>
      )}
    </div>
  )
}
