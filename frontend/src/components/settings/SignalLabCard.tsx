/**
 * SignalLabCard — Settings → Intelligence: the lab's one destructive control.
 *
 * D20: the Signal Lab's only durable state is its round history; the fitted
 * model is derived from it wholesale. Purge deletes both, atomically, and
 * cannot be undone. Library / ratings / the always-on feedback history are
 * untouched — the confirm copy says exactly that (truthful UI).
 */
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { FlaskConical, Trash2 } from 'lucide-react'

import { getSignalLabEval, getSignalLabModel, purgeSignalLab } from '@/api/client'
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from '@/components/ui/alert-dialog'
import { Button } from '@/components/ui/button'
import { SettingsCard } from '@/components/settings/primitives'

function fmtAcc(value: number | null | undefined): string {
  return value == null ? '—' : `${Math.round(value * 100)}%`
}

export function SignalLabCard() {
  const queryClient = useQueryClient()
  const [lastPurged, setLastPurged] = useState<number | null>(null)

  const modelQuery = useQuery({
    queryKey: ['signal-lab', 'model'],
    queryFn: getSignalLabModel,
    staleTime: 30_000,
  })

  const evalQuery = useQuery({
    queryKey: ['signal-lab', 'eval'],
    queryFn: getSignalLabEval,
    staleTime: 60_000,
    enabled: (modelQuery.data?.counts?.answered ?? 0) > 0,
  })

  const purgeMutation = useMutation({
    mutationFn: purgeSignalLab,
    onSuccess: (result) => {
      setLastPurged(result.rounds_deleted)
      queryClient.invalidateQueries({ queryKey: ['signal-lab'] })
    },
  })

  const rounds = modelQuery.data?.counts?.rounds ?? 0
  const ready = modelQuery.data?.ready ?? false

  return (
    <SettingsCard
      icon={FlaskConical}
      title="Signal Lab"
      description="Calibration minigames write only their own round history; ranking reads a model derived from it. Purging deletes that history and its model — your Library, ratings, and feedback history are untouched."
      action={
        <AlertDialog>
          <AlertDialogTrigger asChild>
            <Button
              variant="outline"
              size="sm"
              disabled={purgeMutation.isPending || (ready && rounds === 0)}
            >
              <Trash2 className="h-4 w-4" />
              Purge Signal Lab
            </Button>
          </AlertDialogTrigger>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Purge the Signal Lab?</AlertDialogTitle>
              <AlertDialogDescription>
                Deletes every round you have answered and everything derived
                from them. Your Library, ratings, and feedback history are
                untouched. This cannot be undone.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <AlertDialogAction onClick={() => purgeMutation.mutate()}>
                Purge
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      }
    >
      <p className="text-xs text-slate-500">
        {ready
          ? `${rounds} round${rounds === 1 ? '' : 's'} recorded.`
          : 'No calibration model yet — it appears after the first rounds are answered.'}
        {lastPurged !== null ? ` Purged ${lastPurged} round${lastPurged === 1 ? '' : 's'}.` : ''}
        {purgeMutation.isError ? ' Purge failed — see backend logs.' : ''}
      </p>
      {/* Stage-1 promotion evidence (task 54 §6): held-out accuracy per nested
          head + what a hypothetical promotion would do to the live top-20.
          Heads are promoted by raising weights.lab_* in discovery settings —
          one at a time, only when both numbers argue for it. */}
      {evalQuery.data?.ready && evalQuery.data.holdout && (
        <p className="text-xs text-slate-500">
          Held-out accuracy — prior {fmtAcc(evalQuery.data.holdout.prior_accuracy)} · +regions{' '}
          {fmtAcc(evalQuery.data.holdout.offsets_accuracy)} · +utility{' '}
          {fmtAcc(evalQuery.data.holdout.utility_accuracy)} ({evalQuery.data.holdout.pairs} pairs).
          {evalQuery.data.churn?.top_n
            ? ` If promoted at ${evalQuery.data.churn.hypothetical_points} pts: ${evalQuery.data.churn.entered_top}/${evalQuery.data.churn.top_n} papers would enter the top ${evalQuery.data.churn.top_n}.`
            : ''}
        </p>
      )}
    </SettingsCard>
  )
}
