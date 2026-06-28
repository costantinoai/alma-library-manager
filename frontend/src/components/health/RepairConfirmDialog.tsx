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
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertTriangle, Check, X } from 'lucide-react'

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/status-badge'
import { AsyncButton } from '@/components/settings/primitives'
import { EtaHint } from '@/components/shared/EtaHint'
import {
  estimateMaintenanceOperation,
  listMergeCandidates,
  mergeOneCandidate,
  rejectMergeCandidate,
  type MergeCandidate,
  type MaintenanceOperation,
  type MaintenanceRunRequest,
} from '@/api/client'

/** Source/confidence badge so the user knows how much to trust each pair:
 *  ORCID is authoritative; a name match is a heuristic to review (the weaker the
 *  confidence, the quieter the badge). */
function sourceBadge(c: MergeCandidate): { tone: StatusBadgeTone; label: string } {
  if (c.source === 'orcid') return { tone: 'info', label: 'ORCID' }
  const tone: BadgeTone = c.confidence === 'high' ? 'info' : c.confidence === 'medium' ? 'warning' : 'neutral'
  return { tone, label: `Name · ${c.confidence ?? 'low'}` }
}

const MERGE_OP_KEY = 'dedup_orcid_merge'

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

  // The merge op is "apply what the scan already found", so the review shows the
  // EXACT pairs (who → who, source/confidence, papers reassigned) — the user's
  // "who would be merged" — from the persisted queue (no network re-scan). Each
  // row can be merged or REJECTED individually; a rejection is permanent (the
  // pair is never resurfaced). Only this op has a candidate list.
  const queryClient = useQueryClient()
  const isMergeOp = op.key === MERGE_OP_KEY
  const candidatesQuery = useQuery({
    queryKey: ['merge-candidates'],
    queryFn: () => listMergeCandidates(),
    enabled: open && isMergeOp,
    staleTime: 0,
  })
  const candidates = candidatesQuery.data?.candidates ?? []

  // After a per-row action the count + queue + apply-plan token must refresh.
  const refreshAfterAction = () =>
    Promise.all([
      queryClient.invalidateQueries({ queryKey: ['merge-candidates'] }),
      queryClient.invalidateQueries({ queryKey: ['health'] }),
    ])
  const rejectMutation = useMutation({
    mutationFn: (id: string) => rejectMergeCandidate(id),
    onSuccess: refreshAfterAction,
  })
  const mergeMutation = useMutation({
    mutationFn: (id: string) => mergeOneCandidate(id),
    onSuccess: refreshAfterAction,
  })
  const rowBusy = (id: string) =>
    (rejectMutation.isPending && rejectMutation.variables === id) ||
    (mergeMutation.isPending && mergeMutation.variables === id)

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

        {/* Merge review: the exact pairs (primary ← duplicate · source/confidence ·
            papers reassigned). Merge or REJECT each individually; reject is
            permanent. "Merge all" in the footer applies whatever remains. */}
        {isMergeOp ? (
          <div className="space-y-1.5">
            <p className="text-xs font-medium uppercase tracking-wide text-slate-500">
              {candidates.length > 0 ? `Pending merges (${candidates.length})` : 'Pending merges'}
            </p>
            {candidatesQuery.isLoading ? (
              <p className="text-sm text-slate-500">Loading the merge list…</p>
            ) : candidates.length === 0 ? (
              <p className="text-sm text-slate-500">No pending merges — run a scan first.</p>
            ) : (
              <ul className="max-h-72 space-y-0.5 overflow-y-auto rounded-sm border border-[var(--color-border)] bg-surface-2 p-2">
                {candidates.map((c) => {
                  const badge = sourceBadge(c)
                  const busy = rowBusy(c.id)
                  return (
                    <li
                      key={c.id}
                      className="flex items-center justify-between gap-2 rounded-sm px-2 py-1.5 text-sm"
                    >
                      <span className="flex min-w-0 items-center gap-2">
                        <StatusBadge tone={badge.tone} size="sm" className="shrink-0">
                          {badge.label}
                        </StatusBadge>
                        <span className="min-w-0 truncate">
                          <span className="font-medium text-alma-800">{c.primary_name}</span>
                          <span className="px-1 text-slate-400" aria-label="merges in">←</span>
                          <span className="text-slate-600">{c.alt_name}</span>
                        </span>
                      </span>
                      <span className="flex shrink-0 items-center gap-1">
                        <span className="px-1 text-xs text-slate-500">
                          {c.papers_estimate} paper{c.papers_estimate === 1 ? '' : 's'}
                        </span>
                        <Button
                          variant="ghost"
                          size="icon-sm"
                          aria-label={`Merge ${c.alt_name} into ${c.primary_name}`}
                          title="Merge this pair"
                          disabled={busy}
                          className="text-success-700 hover:bg-success-700/10"
                          onClick={() => mergeMutation.mutate(c.id)}
                        >
                          <Check className="h-4 w-4" aria-hidden />
                        </Button>
                        <Button
                          variant="ghost"
                          size="icon-sm"
                          aria-label={`Reject — ${c.primary_name} and ${c.alt_name} are not the same person`}
                          title="Not the same person — reject permanently"
                          disabled={busy}
                          className="text-critical-700 hover:bg-critical-700/10"
                          onClick={() => rejectMutation.mutate(c.id)}
                        >
                          <X className="h-4 w-4" aria-hidden />
                        </Button>
                      </span>
                    </li>
                  )
                })}
              </ul>
            )}
            <p className="text-xs text-slate-400">
              ✓ merge · ✕ reject (permanent — a rejected pair is never suggested again).
            </p>
          </div>
        ) : null}

        <DialogFooter>
          <Button variant="ghost" onClick={() => onOpenChange(false)}>
            {isMergeOp ? 'Done' : 'Cancel'}
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
            {isMergeOp ? 'Merge all remaining' : 'Confirm & run'}
          </AsyncButton>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
