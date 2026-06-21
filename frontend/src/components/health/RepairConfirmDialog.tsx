/**
 * RepairConfirmDialog — the explicit review gate for a DESTRUCTIVE maintenance
 * operation (author/preprint merge, orphan GC). These never run from a single
 * click and never run automatically (no Auto switch on their card). This dialog
 * fetches the CURRENT apply-plan — its pending/selected counts plus the one-time
 * `confirmation_token` + `plan_fingerprint` the backend will demand — and only
 * on an explicit "Confirm & run" does it send those back inside the run request.
 *
 * Why fetch fresh: the token is derived from a fingerprint of the exact selected
 * work. If the underlying data changes between preview and confirm, the
 * fingerprint no longer matches and `run_task_now` rejects the launch — so the
 * user can never apply a stale plan, and the UI never hard-codes or bypasses the
 * backend confirmation.
 */
import { useQuery } from '@tanstack/react-query'
import { AlertTriangle } from 'lucide-react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { AsyncButton } from '@/components/settings/primitives'
import { EtaHint } from '@/components/shared/EtaHint'
import {
  estimateMaintenanceOperation,
  type MaintenanceOperation,
  type MaintenanceRunRequest,
} from '@/api/client'

interface RepairConfirmDialogProps {
  op: MaintenanceOperation
  open: boolean
  onOpenChange: (open: boolean) => void
  /** The base run request from the card's visible controls (no token yet). */
  request: MaintenanceRunRequest
  /** Apply: receives the request augmented with the live token + fingerprint. */
  onConfirm: (request: MaintenanceRunRequest) => void
  running: boolean
}

export function RepairConfirmDialog({
  op,
  open,
  onOpenChange,
  request,
  onConfirm,
  running,
}: RepairConfirmDialogProps) {
  // The live apply-plan (dry_run:false) so the backend mints a confirmation
  // token + fingerprint for THIS exact scope / limit / batch.
  const planQuery = useQuery({
    queryKey: [
      'health',
      'confirm-plan',
      op.key,
      request.scope,
      request.max_items,
      request.request_batch_size,
    ],
    queryFn: () =>
      estimateMaintenanceOperation(op.key, {
        scope: request.scope ?? undefined,
        max_items: request.max_items,
        request_batch_size: request.request_batch_size ?? undefined,
        dry_run: false,
      }),
    enabled: open,
    staleTime: 0,
  })
  const plan = planQuery.data
  const token = plan?.confirmation_token ?? null
  const fingerprint = plan?.plan_fingerprint ?? null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="bg-surface-1">
        <DialogHeader>
          <div className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 shrink-0 text-warning-600" aria-hidden />
            <DialogTitle className="text-alma-900">Review: {op.label}</DialogTitle>
          </div>
          <DialogDescription className="text-slate-600">{op.description}</DialogDescription>
        </DialogHeader>

        <div className="space-y-2 rounded-sm border border-warning-100 bg-warning-50 p-3 text-sm text-warning-800">
          <p className="font-medium">
            This is a destructive operation — it merges or removes rows and is not
            reversed automatically.
          </p>
          {planQuery.isLoading ? (
            <p>Computing what will change…</p>
          ) : plan ? (
            <p>
              It will apply to <strong>{plan.selected_items}</strong> of{' '}
              <strong>{plan.candidates_pending}</strong> {op.unit.replace(/_/g, ' ')}{' '}
              candidate{plan.candidates_pending === 1 ? '' : 's'}.
            </p>
          ) : (
            <p>Couldn't compute the plan. Close this dialog and try again.</p>
          )}
          {plan?.eta ? <EtaHint eta={plan.eta} /> : null}
        </div>

        <DialogFooter>
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <AsyncButton
            variant="outline"
            pending={running}
            // No fresh token (still loading, or nothing to apply) → can't confirm.
            disabled={!token || !fingerprint || (plan?.selected_items ?? 0) <= 0}
            className="border-critical-200 text-critical-700 hover:bg-critical-50"
            onClick={() =>
              onConfirm({
                ...request,
                dry_run: false,
                confirmation_token: token,
                plan_fingerprint: fingerprint,
              })
            }
          >
            Confirm &amp; run
          </AsyncButton>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
