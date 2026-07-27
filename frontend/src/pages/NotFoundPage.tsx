import { Button } from '@/components/ui/button'
import { NotFoundState } from '@/components/ui/NotFoundState'
import { navigateTo } from '@/lib/hashRoute'

export default function NotFoundPage() {
  return (
    <NotFoundState
      scope="page"
      message="The address may be old, incomplete, or mistyped. Your Library and research data have not changed."
      action={
        <Button type="button" onClick={() => navigateTo('home')}>
          Return to Home
        </Button>
      }
    />
  )
}
