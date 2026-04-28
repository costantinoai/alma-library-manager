import {
  CircleCheck,
  Info,
  LoaderCircle,
  OctagonX,
  TriangleAlert,
} from "lucide-react"
import { useTheme } from "next-themes"
import { Toaster as Sonner } from "sonner"

type ToasterProps = React.ComponentProps<typeof Sonner>

const Toaster = ({ ...props }: ToasterProps) => {
  const { theme = "system" } = useTheme()

  return (
    <Sonner
      theme={theme as ToasterProps["theme"]}
      className="toaster group"
      icons={{
        success: <CircleCheck className="h-4 w-4" />,
        info: <Info className="h-4 w-4" />,
        warning: <TriangleAlert className="h-4 w-4" />,
        error: <OctagonX className="h-4 w-4" />,
        loading: <LoaderCircle className="h-4 w-4 animate-spin" />,
      }}
      toastOptions={{
        classNames: {
          // Paper-warm toast surface with a thin gold left-edge ribbon
          // (the v2 trim accent). Sits on shadow-paper-lg so it floats
          // clearly above the page without going hard-shadow.
          toast:
            "group toast group-[.toaster]:bg-alma-chrome group-[.toaster]:text-alma-900 group-[.toaster]:border group-[.toaster]:border-[var(--color-border)] group-[.toaster]:border-l-4 group-[.toaster]:border-l-gold-400 group-[.toaster]:shadow-paper-lg group-[.toaster]:rounded",
          description: "group-[.toast]:text-slate-500",
          actionButton:
            "group-[.toast]:bg-alma-800 group-[.toast]:text-alma-cream group-[.toast]:rounded-sm",
          cancelButton:
            "group-[.toast]:bg-parchment-100 group-[.toast]:text-alma-700 group-[.toast]:rounded-sm",
        },
      }}
      {...props}
    />
  )
}

export { Toaster }
