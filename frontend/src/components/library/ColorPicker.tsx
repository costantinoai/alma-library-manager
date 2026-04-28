import { PRESET_COLORS } from './types'

interface ColorPickerProps {
  value: string
  onChange: (color: string) => void
}

export function ColorPicker({ value, onChange }: ColorPickerProps) {
  return (
    <div className="flex flex-wrap gap-2">
      {PRESET_COLORS.map((color) => (
        <button
          key={color}
          type="button"
          onClick={() => onChange(color)}
          className={`h-7 w-7 rounded-full border-2 transition-all ${
            value === color
              ? 'border-slate-900 scale-110 shadow-md'
              : 'border-transparent hover:border-[var(--color-border)] hover:scale-105'
          }`}
          style={{ backgroundColor: color }}
          title={color}
        />
      ))}
    </div>
  )
}
