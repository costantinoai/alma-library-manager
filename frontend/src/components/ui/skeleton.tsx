import { cn } from "@/lib/utils"

function Skeleton({
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      // Control ink wash, not a paper step: a skeleton has to be visible on
      // every host (desk, card, panel, popover) and a fixed cream fill
      // vanishes on the surfaces closest to it.
      className={cn("animate-pulse rounded-md bg-control-quiet", className)}
      {...props}
    />
  )
}

export { Skeleton }
