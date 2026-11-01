import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ExternalLink, Loader2, Puzzle, Trash2 } from 'lucide-react'

import {
  getAddon,
  getApiErrorMessage,
  listAddons,
  removeAddon,
  setAddonEnabled,
  type AddonInfo,
} from '@/api/client'
import { SettingsCard } from '@/components/settings/primitives'
import {
  AlertDialog,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { StatusBadge } from '@/components/ui/status-badge'
import { Switch } from '@/components/ui/switch'
import { errorToast, useToast } from '@/hooks/useToast'
import { docsUrl } from '@/lib/docs'
import { formatBytes } from '@/lib/format'
import { invalidateQueries } from '@/lib/queryHelpers'

/** Queries that change shape when an add-on goes on or off. */
const SWITCH_ROOTS = [
  ['addons'],
  ['bootstrap'],
  ['plugins'],
  ['home'],
  ['home-brief'],
  ['health'],
  ['ai-status'],
]

/**
 * Optional features, one row each. Disable keeps everything; Remove is a
 * separate, explicit step behind a typed confirmation.
 */
export function AddonsSection() {
  const addonsQuery = useQuery({ queryKey: ['addons'], queryFn: listAddons })

  if (addonsQuery.isLoading) {
    return <p className="text-sm text-slate-500">Loading add-ons…</p>
  }
  if (addonsQuery.isError) {
    return (
      <p className="text-sm text-critical-600">
        Add-ons could not be loaded: {getApiErrorMessage(addonsQuery.error)}
      </p>
    )
  }
  const addons = addonsQuery.data ?? []

  return (
    <SettingsCard
      icon={Puzzle}
      title="Add-ons"
      description={
        'Optional features. Feed, Discovery, Authors and Library work without any of them. ' +
        'Switching one off stops it and keeps its data. Removing it deletes that data for good.'
      }
    >
      {addons.length === 0 ? (
        <p className="text-sm text-slate-500">This build has no add-ons.</p>
      ) : (
        <ul className="divide-y divide-[var(--color-border)]">
          {addons.map((addon) => (
            <AddonRow key={addon.id} addon={addon} />
          ))}
        </ul>
      )}
    </SettingsCard>
  )
}

function AddonRow({ addon }: { addon: AddonInfo }) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [removing, setRemoving] = useState(false)
  const [confirmingOff, setConfirmingOff] = useState(false)

  const switchMutation = useMutation({
    mutationFn: (enabled: boolean) => setAddonEnabled(addon.id, enabled),
    onSuccess: async (result) => {
      await invalidateQueries(queryClient, ...SWITCH_ROOTS)
      if (result.still_finishing?.length) {
        toast({
          title: `${addon.name} is off`,
          description: 'A job that had already started is finishing.',
        })
      }
    },
    onError: (error) => errorToast(`Could not switch ${addon.name}`, getApiErrorMessage(error)),
  })

  return (
    <li className="flex items-start gap-3 py-3 first:pt-0 last:pb-0">
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-alma-900">{addon.name}</p>
        <p className="mt-0.5 text-xs text-slate-500">{addon.description}</p>
        {addon.blocked && (
          <p className="mt-1 text-xs text-critical-600">Held off: {addon.blocked}.</p>
        )}
        {!addon.enabled && addon.has_data && (
          <p className="mt-1 text-xs text-slate-500">Off, with its data kept.</p>
        )}
        <div className="mt-1 flex items-center gap-3">
          <a
            href={docsUrl(addon.docs_path)}
            target="_blank"
            rel="noreferrer"
            className="inline-flex items-center gap-1 text-xs text-alma-folio hover:underline"
          >
            Documentation
            <ExternalLink className="h-3 w-3" aria-hidden />
          </a>
          {!addon.enabled && (
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="h-auto px-1 py-0.5 text-xs"
              onClick={() => setRemoving(true)}
            >
              <Trash2 className="h-3 w-3" aria-hidden />
              Remove
            </Button>
          )}
        </div>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <StatusBadge tone={addon.enabled ? 'positive' : 'neutral'} size="sm">
          {addon.enabled ? 'On' : 'Off'}
        </StatusBadge>
        <Switch
          checked={addon.enabled}
          disabled={switchMutation.isPending}
          onCheckedChange={(enabled) => {
            if (!enabled && addon.disable_warning) setConfirmingOff(true)
            else switchMutation.mutate(enabled)
          }}
          aria-label={`${addon.enabled ? 'Switch off' : 'Switch on'} ${addon.name}`}
        />
      </div>
      {confirmingOff && (
        <AlertDialog open onOpenChange={(open) => !open && setConfirmingOff(false)}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Switch {addon.name} off?</AlertDialogTitle>
              <AlertDialogDescription>{addon.disable_warning}</AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <Button
                onClick={() => {
                  setConfirmingOff(false)
                  switchMutation.mutate(false)
                }}
              >
                Switch off
              </Button>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      )}
      {removing && <RemoveDialog addon={addon} onClose={() => setRemoving(false)} />}
    </li>
  )
}

/** Shows what would go, and asks for the id to be typed before it can go. */
function RemoveDialog({ addon, onClose }: { addon: AddonInfo; onClose: () => void }) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const [typed, setTyped] = useState('')

  const detailQuery = useQuery({
    queryKey: ['addons', addon.id],
    queryFn: () => getAddon(addon.id),
  })
  const removeMutation = useMutation({
    mutationFn: () => removeAddon(addon.id),
    onSuccess: async () => {
      await invalidateQueries(queryClient, ...SWITCH_ROOTS)
      toast({ title: `Removing ${addon.name}`, description: 'A backup is taken first.' })
      onClose()
    },
    onError: (error) => errorToast(`Could not remove ${addon.name}`, getApiErrorMessage(error)),
  })
  const footprint = detailQuery.data?.footprint

  return (
    <AlertDialog open onOpenChange={(open) => !open && onClose()}>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>Remove {addon.name}?</AlertDialogTitle>
          <AlertDialogDescription>
            This deletes what it stored, its settings and its credentials. A database backup is
            taken first. Your papers are not touched. Switching it on again starts empty.
          </AlertDialogDescription>
        </AlertDialogHeader>
        {footprint ? (
          <ul className="space-y-1 text-sm text-slate-600">
            {footprint.tables.map((table) => (
              <li key={table.name} className="flex justify-between gap-3">
                <span className="font-mono text-xs">{table.name}</span>
                <span>{table.rows.toLocaleString()} {table.rows === 1 ? 'row' : 'rows'}</span>
              </li>
            ))}
            {footprint.core_rows.map((row) => (
              <li key={row.label} className="flex justify-between gap-3">
                <span>{row.label}</span>
                <span>{row.rows.toLocaleString()} {row.rows === 1 ? 'row' : 'rows'}</span>
              </li>
            ))}
            {footprint.files > 0 && (
              <li className="flex justify-between gap-3">
                <span>Files</span>
                <span>
                  {footprint.files.toLocaleString()} ({formatBytes(footprint.bytes)})
                </span>
              </li>
            )}
            {footprint.settings.length > 0 && (
              <li className="flex justify-between gap-3">
                <span>Settings</span>
                <span>{footprint.settings.length}</span>
              </li>
            )}
            {footprint.secrets.length > 0 && (
              <li className="flex justify-between gap-3">
                <span>Credentials</span>
                <span>{footprint.secrets.length}</span>
              </li>
            )}
          </ul>
        ) : (
          <Loader2 className="h-4 w-4 animate-spin text-slate-500" aria-label="Loading" />
        )}
        <label className="block space-y-1 text-sm text-slate-600">
          <span>
            Type <span className="font-mono text-xs">{addon.id}</span> to confirm
          </span>
          <Input value={typed} onChange={(event) => setTyped(event.target.value)} autoFocus />
        </label>
        <AlertDialogFooter>
          <AlertDialogCancel disabled={removeMutation.isPending}>Cancel</AlertDialogCancel>
          <Button
            variant="destructive"
            disabled={typed !== addon.id || removeMutation.isPending}
            onClick={() => removeMutation.mutate()}
          >
            {removeMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
            Remove
          </Button>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  )
}
