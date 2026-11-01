import {
  CircleCheck,
  Info,
  LoaderCircle,
  OctagonX,
  TriangleAlert,
} from "lucide-react"
import { Toaster as Sonner } from "sonner"

type ToasterProps = React.ComponentProps<typeof Sonner>

const Toaster = ({ ...props }: ToasterProps) => {
  return (
    <Sonner
      theme="light"
      className="toaster group"
      icons={{
        success: <CircleCheck className="h-4 w-4" />,
        info: <Info className="h-4 w-4" />,
        warning: <TriangleAlert className="h-4 w-4" />,
        error: <OctagonX className="h-4 w-4" />,
        loading: <LoaderCircle className="h-4 w-4 animate-spin" />,
      }}
      // Sonner injects its stylesheet unlayered, and an unlayered rule beats every
      // Tailwind v4 utility (they live in `@layer utilities`), whatever the
      // selector. So a toast is themed through the variables Sonner reads, and
      // the rest carries `!`. A plain utility here silently renders Sonner's
      // default white.
      style={
        {
          '--normal-bg': 'var(--color-surface-1)',
          '--normal-text': 'var(--color-alma-900)',
          '--normal-border': 'var(--color-border)',
        } as React.CSSProperties
      }
      toastOptions={{
        classNames: {
          // The left edge says the outcome: green, red, or a neutral hairline
          // for anything else. Sits on shadow-paper-lg so it floats clearly
          // above the page without going hard-shadow.
          toast:
            "border-l-4! border-l-control-edge-strong! data-[type=success]:border-l-success-600! data-[type=error]:border-l-critical-600! shadow-paper-lg! rounded-card!",
          // slate-600, not slate-500: 500 misses AA on this paper.
          description: "text-slate-600!",
          actionButton: "bg-alma-800! text-alma-cream! rounded-control!",
          cancelButton: "bg-control-quiet! text-alma-700! rounded-control!",
        },
      }}
      {...props}
    />
  )
}

export { Toaster }
