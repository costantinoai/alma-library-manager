import { useState } from 'react'
import { Archive, Inbox, Link2, RotateCcw } from 'lucide-react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import {
  applyCaptureMessageAction,
  getApiErrorMessage,
  getCaptureAttentionMessages,
  type CaptureAttentionMessage,
  type CaptureMessageActionResult,
} from '@/api/client'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { EmptyState } from '@/components/ui/empty-state'
import { ErrorState } from '@/components/ui/ErrorState'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { StatusBadge } from '@/components/ui/status-badge'
import { Surface } from '@/components/ui/surface'
import { errorToast, toast } from '@/hooks/useToast'
import { invalidateAfterCaptureMutation } from '@/lib/queryHelpers'
import { CAPTURE_CHANNEL_LABEL } from '@/lib/palette'
import { formatRelativeShort } from '@/lib/utils'

interface CaptureReviewDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

type CaptureMutation =
  | { messageId: string; action: 'archive' }
  | { messageId: string; action: 'retry'; replacementLink: string }

function resultToast(result: CaptureMessageActionResult): string {
  if (result.action === 'archive') return 'Capture archived'
  if (result.outcome === 'resolved') return 'Paper captured and added to your Inbox'
  if (result.outcome === 'duplicate') return 'Paper is already in your Library'
  return 'That link still could not be identified'
}

function CaptureRecord({
  message,
  mutate,
  pending,
}: {
  message: CaptureAttentionMessage
  mutate: (input: CaptureMutation) => void
  pending: boolean
}) {
  const [replacementLink, setReplacementLink] = useState(message.retry_input ?? '')
  const channel = CAPTURE_CHANNEL_LABEL[message.channel] ?? message.channel
  const formId = `capture-retry-${message.id}`

  return (
    <Surface className="space-y-4 rounded-sm p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge tone={message.outcome === 'error' ? 'negative' : 'warning'}>
              {message.outcome === 'error' ? 'Capture failed' : 'Not identified'}
            </StatusBadge>
            <span className="text-xs text-slate-500">
              {channel} · {formatRelativeShort(message.received_at)}
            </span>
          </div>
          <p className="whitespace-pre-wrap break-words text-sm text-alma-900">
            {message.raw_text || 'No message text was recorded.'}
          </p>
        </div>
        <Button
          type="button"
          size="sm"
          variant="ghost"
          disabled={pending}
          onClick={() => mutate({ messageId: message.id, action: 'archive' })}
        >
          <Archive />
          Archive
        </Button>
      </div>

      {message.error && (
        <p className="rounded-sm border border-warning-700/20 bg-warning-700/[0.06] px-3 py-2 text-xs text-warning-900">
          {message.error}
        </p>
      )}

      <form
        className="space-y-2"
        onSubmit={(event) => {
          event.preventDefault()
          mutate({
            messageId: message.id,
            action: 'retry',
            replacementLink: replacementLink.trim(),
          })
        }}
      >
        <Label htmlFor={formId}>Try another paper link</Label>
        <div className="flex flex-col gap-2 sm:flex-row">
          <div className="relative min-w-0 flex-1">
            <Link2 className="pointer-events-none absolute left-3 top-3 h-4 w-4 text-slate-400" />
            <Input
              id={formId}
              type="url"
              inputMode="url"
              required
              value={replacementLink}
              onChange={(event) => setReplacementLink(event.target.value)}
              placeholder="https://doi.org/… or publisher link"
              className="pl-9"
            />
          </div>
          <Button type="submit" size="sm" loading={pending} disabled={!replacementLink.trim()}>
            <RotateCcw />
            Try capture
          </Button>
        </div>
      </form>
    </Surface>
  )
}

export function CaptureReviewDialog({ open, onOpenChange }: CaptureReviewDialogProps) {
  const queryClient = useQueryClient()
  const messagesQuery = useQuery({
    queryKey: ['capture-attention'],
    queryFn: getCaptureAttentionMessages,
    enabled: open,
  })
  const actionMutation = useMutation({
    mutationFn: (input: CaptureMutation) =>
      input.action === 'archive'
        ? applyCaptureMessageAction(input.messageId, { action: 'archive' })
        : applyCaptureMessageAction(input.messageId, {
            action: 'retry',
            replacement_link: input.replacementLink,
          }),
    onSuccess: (result) => {
      toast({ title: resultToast(result) })
    },
    onError: (error) => {
      errorToast('Capture action failed', getApiErrorMessage(error))
    },
    onSettled: () => invalidateAfterCaptureMutation(queryClient),
  })

  const messages = messagesQuery.data ?? []

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex max-h-[85dvh] max-w-2xl flex-col overflow-hidden p-0">
        <DialogHeader className="border-b border-edge-1 px-6 pb-4 pt-6 pr-12">
          <DialogTitle>Capture review</DialogTitle>
          <DialogDescription>
            Messages ALMa received but could not turn into a paper. Keep the record and
            archive it, or provide a better link and try again.
          </DialogDescription>
        </DialogHeader>
        <div className="min-h-0 flex-1 space-y-3 overflow-y-auto px-6 pb-6">
          {messagesQuery.isLoading && (
            <p className="py-8 text-center text-sm text-slate-500">Loading captures…</p>
          )}
          {messagesQuery.isError && (
            <ErrorState
              message="Couldn't load capture records."
              actionLabel="Retry"
              actionPending={messagesQuery.isFetching}
              onAction={() => void messagesQuery.refetch()}
            />
          )}
          {!messagesQuery.isLoading && !messagesQuery.isError && messages.length === 0 && (
            <EmptyState
              icon={Inbox}
              title="No captures need attention"
              description="Archived and successfully retried messages no longer count here."
            />
          )}
          {messages.map((message) => (
            <CaptureRecord
              key={message.id}
              message={message}
              mutate={(input) => actionMutation.mutate(input)}
              pending={
                actionMutation.isPending && actionMutation.variables?.messageId === message.id
              }
            />
          ))}
        </div>
      </DialogContent>
    </Dialog>
  )
}
