/**
 * Settings primitives — the small palette every settings card is built from.
 *
 * Why these live here rather than in `components/ui/`:
 *   `ui/` is the shadcn/Radix layer (Card, Button, Input, RadioGroupItem,
 *   StatusBadge, Checkbox, Switch, ...). This file composes those lower-level
 *   parts into the repeating settings patterns (the icon-+-title-+-description
 *   card shell, the "action with a spinner" button, the "title / description
 *   / toggle" row, the "radio card" option, the uniform labeled-number-input,
 *   the stat tile, the key-value row, the dependency chip, and the disclosure
 *   section). Pulling these out removes ~100 lines of copy-pasted JSX per card
 *   and keeps settings consistent with the frontend-design rules in
 *   `tasks/lessons.md` (Status goes through StatusBadge, form surfaces come
 *   from `components/ui/`, only slate in the neutral scale, etc.).
 */

import * as React from 'react'
import { ChevronDown, type LucideIcon } from 'lucide-react'

import { Button, type ButtonProps } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Checkbox } from '@/components/ui/checkbox'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Input } from '@/components/ui/input'
import { RadioGroupItem } from '@/components/ui/radio-group'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { MetricTile, type MetricTileTone } from '@/components/shared/MetricTile'
import { cn } from '@/lib/utils'

// ---------------------------------------------------------------------------
// SettingsCard — the canonical card shell every settings card uses.
// ---------------------------------------------------------------------------

export interface SettingsCardProps {
  /** Lucide icon rendered at the top-left of the header. */
  icon?: LucideIcon
  /** Title shown inside `CardTitle`. */
  title: React.ReactNode
  /** Optional description rendered as `CardDescription`. */
  description?: React.ReactNode
  /**
   * Optional action slot on the right side of the header. Used by cards that
   * mount an "Open explorer" button, a header-level stats strip, or a
   * secondary link next to the title.
   */
  action?: React.ReactNode
  /** Extra classes for the outer `<Card>`. */
  className?: string
  headerClassName?: string
  contentClassName?: string
  /** When true, the body gets `space-y-6` instead of the default `space-y-4`. */
  roomy?: boolean
  children: React.ReactNode
}

export function SettingsCard({
  icon: Icon,
  title,
  description,
  action,
  className,
  headerClassName,
  contentClassName,
  roomy,
  children,
}: SettingsCardProps) {
  return (
    <Card className={className}>
      <CardHeader className={cn('gap-2', headerClassName)}>
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="flex min-w-0 items-start gap-2">
            {Icon ? <Icon className="mt-0.5 h-5 w-5 shrink-0 text-slate-500" /> : null}
            <div className="min-w-0 space-y-1">
              <CardTitle className="text-base">{title}</CardTitle>
              {description ? <CardDescription>{description}</CardDescription> : null}
            </div>
          </div>
          {action ? (
            <div className="flex shrink-0 flex-wrap items-center gap-2">{action}</div>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className={cn(roomy ? 'space-y-6' : 'space-y-4', contentClassName)}>
        {children}
      </CardContent>
    </Card>
  )
}

// ---------------------------------------------------------------------------
// AsyncButton — Button + pending state without the copy-pasted spinner branch.
// ---------------------------------------------------------------------------

export interface AsyncButtonProps extends Omit<ButtonProps, 'loading'> {
  /** When true, renders the Loader2 spinner instead of `icon` and disables the button. */
  pending?: boolean
  /**
   * Icon shown to the left of the label at rest. Swapped out for the
   * spinner while `pending` is true.
   */
  icon?: React.ReactNode
}

/**
 * Thin wrapper around `Button` that captures the repeated pattern:
 *
 *   {isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Icon />}
 *   Label
 *
 * The underlying `Button` already renders the spinner when `loading` is
 * truthy; we only need to hide the rest-state icon so we don't end up with
 * two icons side-by-side mid-request.
 */
export function AsyncButton({
  pending,
  icon,
  disabled,
  children,
  ...props
}: AsyncButtonProps) {
  return (
    <Button loading={pending} disabled={disabled ?? pending} {...props}>
      {!pending && icon}
      {children}
    </Button>
  )
}

// ---------------------------------------------------------------------------
// ToggleRow — title + description on the left, control on the right.
// ---------------------------------------------------------------------------

export interface ToggleRowProps {
  title: React.ReactNode
  description?: React.ReactNode
  /**
   * `warning` tints the row amber for toggles that opt into
   * riskier-than-default behaviour (auto scraping, auto-apply branch
   * suggestions, etc.). Default keeps a neutral slate border.
   */
  tone?: 'default' | 'warning'
  /**
   * Custom control slot — pass `<FormControl><Switch .../></FormControl>`
   * or a wired-up Radix primitive when state is managed by react-hook-form.
   * When omitted, falls back to a `Checkbox` bound to `checked` /
   * `onCheckedChange`.
   */
  control?: React.ReactNode
  checked?: boolean
  onCheckedChange?: (value: boolean) => void
  disabled?: boolean
  className?: string
}

export function ToggleRow({
  title,
  description,
  tone = 'default',
  control,
  checked,
  onCheckedChange,
  disabled,
  className,
}: ToggleRowProps) {
  const isWarning = tone === 'warning'
  return (
    <label
      className={cn(
        'flex items-center justify-between gap-3 rounded-lg border px-3 py-2',
        isWarning ? 'border-amber-200 bg-amber-50' : 'border-slate-200',
        disabled && 'opacity-70',
        className,
      )}
    >
      <div className="min-w-0 space-y-0.5">
        <p
          className={cn(
            'text-sm font-medium',
            isWarning ? 'text-amber-800' : 'text-slate-700',
          )}
        >
          {title}
        </p>
        {description ? (
          <p className={cn('text-xs', isWarning ? 'text-amber-700' : 'text-slate-500')}>
            {description}
          </p>
        ) : null}
      </div>
      {control ?? (
        <Checkbox
          checked={!!checked}
          disabled={disabled}
          onCheckedChange={(value) => onCheckedChange?.(value === true)}
        />
      )}
    </label>
  )
}

// ---------------------------------------------------------------------------
// OptionCard — radio-style card for backend / provider / mode pickers.
// ---------------------------------------------------------------------------

export interface OptionCardProps {
  /** Value forwarded to the inner `RadioGroupItem`. Must be unique within the group. */
  value: string
  /**
   * Visual selection state. Driven by the parent `<RadioGroup value={...}>`;
   * pass `value === selected` when you want the alma ring rendered on top of
   * the Radix checked state (keeps the card visible as selected even before
   * the radio's focus-visible ring appears).
   */
  selected?: boolean
  icon?: React.ReactNode
  title: React.ReactNode
  description?: React.ReactNode
  /** Extra content (meta badges, model tier info) shown inline after the title. */
  meta?: React.ReactNode
  disabled?: boolean
  /** Optional additional body content rendered under the description. */
  children?: React.ReactNode
  className?: string
}

/**
 * Must be rendered inside a `<RadioGroup>`. Clicking anywhere in the label
 * selects the radio; the card gets an alma-tinted ring when `selected` is true.
 */
export function OptionCard({
  value,
  selected,
  icon,
  title,
  description,
  meta,
  disabled,
  children,
  className,
}: OptionCardProps) {
  return (
    <label
      className={cn(
        'flex items-start gap-3 rounded-lg border p-3 transition-colors',
        disabled
          ? 'cursor-not-allowed border-slate-100 bg-parchment-50 opacity-70'
          : selected
            ? 'cursor-pointer border-alma-300 bg-alma-50'
            : 'cursor-pointer border-slate-200 hover:bg-parchment-50',
        className,
      )}
    >
      <RadioGroupItem value={value} disabled={disabled} className="mt-0.5" />
      {icon ? <span className="mt-0.5 text-slate-500">{icon}</span> : null}
      <span className="min-w-0 flex-1 space-y-1">
        <span className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium text-slate-800">{title}</span>
          {meta}
        </span>
        {description ? (
          <span className="block text-xs text-slate-500">{description}</span>
        ) : null}
        {children}
      </span>
    </label>
  )
}

// ---------------------------------------------------------------------------
// SettingsNumberField — labeled number input row.
// ---------------------------------------------------------------------------

export interface SettingsNumberFieldProps {
  label: React.ReactNode
  description?: React.ReactNode
  value: number
  onChange: (value: number) => void
  onBlur?: () => void
  min?: number
  max?: number
  step?: number | string
  disabled?: boolean
  inputClassName?: string
  className?: string
}

/**
 * Used for discovery weights, retrieval limits, schedule intervals, branch
 * tuning, and feed monitor defaults. `onChange` always delivers a parsed
 * number; empty/invalid inputs are coerced to 0 so the form stays controlled.
 */
export function SettingsNumberField({
  label,
  description,
  value,
  onChange,
  onBlur,
  min,
  max,
  step = 1,
  disabled,
  inputClassName,
  className,
}: SettingsNumberFieldProps) {
  return (
    <label className={cn('block space-y-1', className)}>
      <div className="flex items-center justify-between gap-3 text-sm">
        <span className="font-medium text-slate-800">{label}</span>
        <Input
          type="number"
          className={cn('h-9 w-28 text-right', inputClassName)}
          value={Number.isFinite(value) ? value : 0}
          min={min}
          max={max}
          step={step}
          disabled={disabled}
          onChange={(event) => {
            const parsed = Number(event.target.value)
            onChange(Number.isFinite(parsed) ? parsed : 0)
          }}
          onBlur={onBlur}
        />
      </div>
      {description ? <p className="text-xs text-slate-500">{description}</p> : null}
    </label>
  )
}

// ---------------------------------------------------------------------------
// StatTile — Settings-flavour wrapper around the canonical MetricTile.
//
// Settings cards (OpenAlex usage, AI capability, Operational status,
// Library management) use a slightly different tone palette than the
// Insights MetricTile (`positive` / `negative` / `info` / `accent` are
// finer-grained than MetricTile's `success` / `critical`). This shim
// preserves the Settings tone names while delegating the surface, the
// chrome-elev background, and the truncate guards to MetricTile so the
// v3 visual stays consistent everywhere.
//
// Existing call sites pass `label`, `value`, `tone`, optional `caption`
// (mapped to MetricTile's `hint`) — no migration needed.
// ---------------------------------------------------------------------------

export type StatTileTone =
  | 'neutral'
  | 'positive'
  | 'negative'
  | 'warning'
  | 'info'
  | 'accent'

// Settings tones map directly onto MetricTile's tone palette — every
// Settings tone has a canonical equivalent now that v3 MetricTile
// covers info + accent.
const STAT_TILE_TONE_TO_METRIC: Record<StatTileTone, MetricTileTone> = {
  neutral: 'neutral',
  positive: 'success',
  negative: 'critical',
  warning: 'warning',
  info: 'info',
  accent: 'accent',
}

export interface StatTileProps {
  label: React.ReactNode
  value: React.ReactNode
  /** Colour applied to the big value; the shell stays neutral. */
  tone?: StatTileTone
  /** Optional caption rendered under the label for secondary context. */
  caption?: React.ReactNode
  className?: string
}

export function StatTile({
  label,
  value,
  tone = 'neutral',
  caption,
  className,
}: StatTileProps) {
  // MetricTile expects string|number for value + label; we render as
  // strings via String() so React nodes (rare in Settings — usually
  // just numbers + currency strings) still render. The `caption`
  // becomes MetricTile's `hint`.
  const stringValue =
    typeof value === 'string' || typeof value === 'number'
      ? value
      : String(value ?? '—')
  const stringLabel = typeof label === 'string' ? label : String(label ?? '')
  const stringHint =
    typeof caption === 'string' || typeof caption === 'number'
      ? String(caption)
      : caption
        ? String(caption)
        : undefined
  return (
    <MetricTile
      label={stringLabel}
      value={stringValue}
      tone={STAT_TILE_TONE_TO_METRIC[tone]}
      hint={stringHint}
      className={className}
    />
  )
}

// ---------------------------------------------------------------------------
// KeyValueRow — inline label / value pair.
// ---------------------------------------------------------------------------

export interface KeyValueRowProps {
  label: React.ReactNode
  value: React.ReactNode
  className?: string
}

export function KeyValueRow({ label, value, className }: KeyValueRowProps) {
  return (
    <div className={cn('flex items-start justify-between gap-3', className)}>
      <span className="shrink-0 text-[10px] font-semibold uppercase tracking-wide text-slate-400">
        {label}
      </span>
      <span className="min-w-0 flex-1 text-right text-slate-700">{value}</span>
    </div>
  )
}

// ---------------------------------------------------------------------------
// PackageChip — mono-font pill for dependency packages and resolution codes.
// ---------------------------------------------------------------------------

export interface PackageChipProps {
  tone?: StatusBadgeTone
  icon?: React.ReactNode
  label: React.ReactNode
  suffix?: React.ReactNode
  title?: string
  className?: string
}

export function PackageChip({
  tone = 'neutral',
  icon,
  label,
  suffix,
  title,
  className,
}: PackageChipProps) {
  return (
    <StatusBadge tone={tone} size="default" title={title} className={cn('gap-1.5', className)}>
      {icon}
      <span className="font-mono">{label}</span>
      {suffix ? <span className="text-[10px] opacity-80">{suffix}</span> : null}
    </StatusBadge>
  )
}

// ---------------------------------------------------------------------------
// SettingsSection — Collapsible sub-section inside a big settings card.
// ---------------------------------------------------------------------------

export interface SettingsSectionProps {
  title: React.ReactNode
  /** Optional hint rendered below the title while the section is open. */
  description?: React.ReactNode
  /** Header-right slot — small status pill (e.g. "Sum 1.00") or chip. */
  trailing?: React.ReactNode
  /** Starts open by default so a first-time reader sees all controls. */
  defaultOpen?: boolean
  className?: string
  children: React.ReactNode
}

/**
 * Disclosure section used inside `AIConfigCard` and `DiscoveryWeightsCard`
 * to stop those cards from scrolling endlessly. Built on the already-in-repo
 * Radix `Collapsible`; keeps all sections independently toggleable.
 */
export function SettingsSection({
  title,
  description,
  trailing,
  defaultOpen = true,
  className,
  children,
}: SettingsSectionProps) {
  return (
    <Collapsible
      defaultOpen={defaultOpen}
      // Tier 3 (parchment-50) — sections are RECESSED inside the cream
      // SettingsCard so the nesting reads as visible depth instead of
      // a flat cream-on-cream sandwich. Inset shadow reinforces the
      // "stamped into the page" feel.
      className={cn(
        'rounded-sm border border-[var(--color-border)] bg-parchment-50/85 shadow-paper-inset',
        className,
      )}
    >
      <CollapsibleTrigger
        className={cn(
          'group flex w-full items-center justify-between gap-3 rounded-t-sm px-4 py-3 text-left',
          'hover:bg-parchment-100/70 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-alma-500',
          'data-[state=closed]:rounded-b-sm',
        )}
      >
        <div className="flex min-w-0 items-center gap-2">
          <ChevronDown className="h-4 w-4 text-slate-500 transition-transform group-data-[state=closed]:-rotate-90" />
          <h4 className="text-sm font-semibold text-slate-800">{title}</h4>
        </div>
        {trailing ? <div className="flex shrink-0 items-center gap-2">{trailing}</div> : null}
      </CollapsibleTrigger>
      <CollapsibleContent className="border-t border-parchment-300/50 px-4 py-4">
        {description ? (
          <p className="mb-3 text-xs text-slate-500">{description}</p>
        ) : null}
        {children}
      </CollapsibleContent>
    </Collapsible>
  )
}

// ---------------------------------------------------------------------------
// SettingsSections — flex container for a stack of `SettingsSection`s.
// ---------------------------------------------------------------------------

export function SettingsSections({
  children,
  className,
}: {
  children: React.ReactNode
  className?: string
}) {
  return <div className={cn('space-y-3', className)}>{children}</div>
}
