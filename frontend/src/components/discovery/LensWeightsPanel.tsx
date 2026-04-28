import { useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { AlertCircle, HelpCircle } from 'lucide-react'

import { api, type Lens, type AIStatus } from '@/api/client'
import { Button } from '@/components/ui/button'
import { Slider } from '@/components/ui/slider'

interface LensWeightsPanelProps {
  lens: Lens | null
  onSave: (weights: Record<string, number>) => void
}

const CHANNELS = ['lexical', 'vector', 'graph', 'external'] as const

const CHANNEL_DESCRIPTIONS: Record<string, string> = {
  lexical: 'SQL/FTS text search over titles, abstracts, topics, and authors',
  vector: 'Embedding similarity against your top-rated papers',
  graph: 'Citation chain, co-author, and followed-author graph expansion',
  external: 'OpenAlex related/citing/topic works for out-of-library discovery',
}

export function LensWeightsPanel({ lens, onSave }: LensWeightsPanelProps) {
  const [weights, setWeights] = useState<Record<string, number>>({
    lexical: 0.25,
    vector: 0.25,
    graph: 0.25,
    external: 0.25,
  })

  const { data: aiStatus } = useQuery({
    queryKey: ['ai-status'],
    queryFn: () => api.get<AIStatus>('/ai/status'),
    staleTime: 30_000,
  })

  const vectorEnabled = aiStatus?.capability_tiers?.tier1_embeddings?.ready ?? false
  const activeModel = aiStatus?.capability_tiers?.tier1_embeddings?.active_model ?? aiStatus?.embeddings?.model

  useEffect(() => {
    if (!lens?.weights) return
    const incoming = lens.weights as Record<string, number>
    setWeights({
      lexical: Number(incoming.lexical ?? 0.25),
      vector: Number(incoming.vector ?? 0.25),
      graph: Number(incoming.graph ?? 0.25),
      external: Number(incoming.external ?? 0.25),
    })
  }, [lens?.id, lens?.weights])

  if (!lens) return null

  const update = (channel: string, value: number) => {
    setWeights((prev) => ({ ...prev, [channel]: value }))
  }

  const normalizeAndSave = () => {
    const total = CHANNELS.reduce((acc, key) => acc + Number(weights[key] ?? 0), 0)
    if (total <= 0) return
    const normalized: Record<string, number> = {}
    for (const key of CHANNELS) {
      normalized[key] = Number((weights[key] / total).toFixed(4))
    }
    onSave(normalized)
  }

  return (
    <div className="space-y-4 rounded-sm border border-[var(--color-border)] bg-alma-chrome-elev p-4">
      <div className="font-brand text-sm font-semibold text-alma-800">
        Lens Weights
      </div>
      <div className="space-y-4">
        {CHANNELS.map((channel) => {
          const isVectorChannel = channel === 'vector'
          const disabled = isVectorChannel && !vectorEnabled
          const value = weights[channel]
          return (
            <div
              key={channel}
              className={disabled ? 'opacity-50' : undefined}
              title={
                disabled
                  ? 'Fetch downloaded vectors or configure an embedding provider in Settings'
                  : undefined
              }
            >
              {/* Two-row layout — label + value above, slider below.
                  Lets the slider have full row width on every screen
                  size and keeps the numeric readout from clipping
                  inside a fixed-px third column. */}
              <div className="flex items-baseline justify-between gap-2">
                <span className="inline-flex items-center gap-1 text-sm capitalize text-slate-700">
                  {channel}
                  <span
                    title={CHANNEL_DESCRIPTIONS[channel]}
                    className="cursor-help"
                  >
                    <HelpCircle className="h-3 w-3 text-slate-300 hover:text-slate-500" />
                  </span>
                </span>
                <span className="font-brand text-sm font-semibold tabular-nums text-alma-800">
                  {value.toFixed(2)}
                </span>
              </div>
              <Slider
                className="mt-2"
                min={0}
                max={1}
                step={0.01}
                value={[value]}
                onValueChange={([next]) => update(channel, Number(next))}
                disabled={disabled}
                aria-label={`${channel} weight`}
              />
              {disabled && (
                <p className="mt-1 flex items-center gap-1 text-xs text-amber-600">
                  <AlertCircle className="h-3 w-3" />
                  Fetch vectors or configure embeddings in Settings
                  {activeModel ? ` (${activeModel})` : ''}
                </p>
              )}
            </div>
          )
        })}
      </div>
      <Button type="button" size="sm" onClick={normalizeAndSave}>
        Save Weights
      </Button>
    </div>
  )
}
