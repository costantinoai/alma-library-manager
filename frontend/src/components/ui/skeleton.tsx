import { cn } from "@/lib/utils"

function Skeleton({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      // Parchment-warm pulse instead of slate — keeps the loading state
      // anchored in the v2 paper-warm palette so the page doesn't go
      // cool-grey while waiting for data.
      className={cn("animate-pulse rounded-md bg-parchment-100", className)}
      {...props}
    />
  )
}

export { Skeleton }
