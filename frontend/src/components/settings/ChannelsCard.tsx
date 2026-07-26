import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useMutation } from '@tanstack/react-query'
import { Inbox, MessageSquare, Zap } from 'lucide-react'

import { sweepInboxNow, testPluginConnection, type Settings } from '@/api/client'
import { AsyncButton, SettingsCard } from '@/components/settings/primitives'
import {
  Form,
  FormControl,
  FormDescription,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from '@/components/ui/form'
import { Input } from '@/components/ui/input'
import { useToast, errorToast } from '@/hooks/useToast'

const channelsSchema = z.object({
  slack_token: z.string(),
  // Accept any non-empty string: a channel name (`general`, `#general`),
  // a user display name (`Andrea Costantino`), or a Slack ID (`C…`/`U…`).
  // Resolution happens server-side in SlackNotifier._resolve_target.
  slack_channel: z.string(),
  // INBOUND capture channel — the one ALMa POLLS for papers you send yourself.
  // Deliberately separate from `slack_channel` (where alerts are POSTED):
  // polling the channel ALMa writes to would re-ingest its own notifications.
  // Empty disables Inbox capture. See docs/concepts/inbox.md.
  slack_inbox_channel: z.string(),
  check_interval_hours: z
    .number()
    .int()
    .min(1, 'Minimum is 1 hour.')
    .max(168, 'Maximum is 168 hours (one week).'),
})

type ChannelsForm = z.infer<typeof channelsSchema>

interface ChannelsCardProps {
  formData: Settings
  onFormDataChange: (updater: (prev: Settings) => Settings) => void
}

export function ChannelsCard({ formData, onFormDataChange }: ChannelsCardProps) {
  const { toast } = useToast()

  const form = useForm<ChannelsForm>({
    resolver: zodResolver(channelsSchema),
    defaultValues: {
      slack_token: formData.slack_token ?? '',
      slack_channel: formData.slack_channel ?? '',
      slack_inbox_channel: formData.slack_inbox_channel ?? '',
      check_interval_hours: formData.check_interval_hours ?? 24,
    },
    mode: 'onBlur',
  })

  useEffect(() => {
    form.reset({
      slack_token: formData.slack_token ?? '',
      slack_channel: formData.slack_channel ?? '',
      slack_inbox_channel: formData.slack_inbox_channel ?? '',
      check_interval_hours: formData.check_interval_hours ?? 24,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    formData.slack_token,
    formData.slack_channel,
    formData.slack_inbox_channel,
    formData.check_interval_hours,
  ])

  useEffect(() => {
    const sub = form.watch((values) => {
      onFormDataChange((prev) => ({
        ...prev,
        slack_token: values.slack_token ?? '',
        slack_channel: values.slack_channel ?? '',
        slack_inbox_channel: values.slack_inbox_channel ?? '',
        check_interval_hours: values.check_interval_hours ?? 24,
      }))
    })
    return () => sub.unsubscribe()
  }, [form, onFormDataChange])

  const testSlackMutation = useMutation({
    mutationFn: () => testPluginConnection('slack'),
    onSuccess: (result) => {
      if (result.ok) {
        toast({
          title: 'Slack test sent',
          description: result.target
            ? `Delivered to ${result.target}.`
            : result.message,
        })
      } else {
        errorToast(
          'Slack test failed',
          result.error || result.message || 'Check your token and channel.',
        )
      }
    },
    onError: (err) => {
      errorToast(
        'Slack test failed',
        err instanceof Error ? err.message : 'Check your token and channel.',
      )
    },
  })

  // Manual capture check. The sweep runs on a timer, but waiting minutes to
  // find out whether your setup works is a bad first experience — and after a
  // config change you want an answer now. Idempotent, so pressing it repeatedly
  // is harmless.
  const sweepInboxMutation = useMutation({
    mutationFn: () => sweepInboxNow(),
    onSuccess: (result) => {
      const captured = result?.captured ?? 0
      toast({
        title: captured
          ? `Captured ${captured} paper${captured === 1 ? '' : 's'}`
          : 'Nothing new to capture',
        description: captured
          ? 'They are waiting in your Inbox on Home.'
          : 'Your capture channel had no unread paper links.',
      })
    },
    onError: (err) => {
      errorToast(
        'Capture check failed',
        err instanceof Error
          ? err.message
          : 'Check the capture channel name and the bot’s scopes.',
      )
    },
  })

  return (
    <SettingsCard
      icon={MessageSquare}
      title="Channels"
      description="Slack for outgoing alerts, and the channel ALMa reads to capture papers you send yourself."
    >
      <Form {...form}>
        <form className="space-y-4" onSubmit={(e) => e.preventDefault()}>
          <FormField
            control={form.control}
            name="slack_token"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Slack Bot Token</FormLabel>
                <FormControl>
                  <Input type="password" placeholder="Paste Slack bot token" {...field} />
                </FormControl>
                <FormDescription>
                  Your Slack Bot OAuth token. Requires chat:write permission.
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="slack_channel"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Default Slack Channel</FormLabel>
                <FormControl>
                  <Input placeholder="#publications" {...field} />
                </FormControl>
                <FormDescription>
                  Channel where publication notifications will be posted.
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="slack_inbox_channel"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Capture channel (Inbox)</FormLabel>
                <FormControl>
                  <Input placeholder="alma-inbox" {...field} />
                </FormControl>
                <FormDescription>
                  A private channel ALMa <strong>reads</strong>: send a paper
                  link here from your phone and it lands in your Inbox on Home.
                  Invite the bot, and add the <code>groups:history</code>,{' '}
                  <code>groups:read</code> and <code>reactions:write</code>{' '}
                  scopes. Keep it separate from the channel above — polling the
                  one ALMa posts to would re-read its own notifications. Leave
                  empty to turn capture off.
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <FormField
            control={form.control}
            name="check_interval_hours"
            render={({ field }) => (
              <FormItem>
                <FormLabel>Check Interval (hours)</FormLabel>
                <FormControl>
                  <Input
                    type="number"
                    min={1}
                    max={168}
                    value={field.value}
                    onChange={(e) => field.onChange(parseInt(e.target.value, 10) || 0)}
                    onBlur={field.onBlur}
                    name={field.name}
                  />
                </FormControl>
                <FormDescription>
                  How often to check for new publications (1–168 hours).
                </FormDescription>
                <FormMessage />
              </FormItem>
            )}
          />

          <div className="flex flex-wrap gap-2">
            <AsyncButton
              type="button"
              variant="outline"
              icon={<Zap className="h-4 w-4" />}
              pending={testSlackMutation.isPending}
              onClick={() => testSlackMutation.mutate()}
            >
              Test Slack Connection
            </AsyncButton>

            <AsyncButton
              type="button"
              variant="outline"
              icon={<Inbox className="h-4 w-4" />}
              pending={sweepInboxMutation.isPending}
              onClick={() => sweepInboxMutation.mutate()}
            >
              Check capture channel now
            </AsyncButton>
          </div>
        </form>
      </Form>
    </SettingsCard>
  )
}
