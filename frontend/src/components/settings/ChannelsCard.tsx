import { useEffect } from 'react'
import { useForm } from 'react-hook-form'
import { zodResolver } from '@hookform/resolvers/zod'
import { z } from 'zod'
import { useMutation } from '@tanstack/react-query'
import { MessageSquare, Zap } from 'lucide-react'

import { testPluginConnection, type Settings } from '@/api/client'
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
      check_interval_hours: formData.check_interval_hours ?? 24,
    },
    mode: 'onBlur',
  })

  useEffect(() => {
    form.reset({
      slack_token: formData.slack_token ?? '',
      slack_channel: formData.slack_channel ?? '',
      check_interval_hours: formData.check_interval_hours ?? 24,
    })
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [formData.slack_token, formData.slack_channel, formData.check_interval_hours])

  useEffect(() => {
    const sub = form.watch((values) => {
      onFormDataChange((prev) => ({
        ...prev,
        slack_token: values.slack_token ?? '',
        slack_channel: values.slack_channel ?? '',
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

  return (
    <SettingsCard
      icon={MessageSquare}
      title="Channels"
      description="Configure Slack notification channel."
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

          <AsyncButton
            type="button"
            variant="outline"
            icon={<Zap className="h-4 w-4" />}
            pending={testSlackMutation.isPending}
            onClick={() => testSlackMutation.mutate()}
          >
            Test Slack Connection
          </AsyncButton>
        </form>
      </Form>
    </SettingsCard>
  )
}
