/**
 * Home's desk must actually work when driven, not just render.
 *
 * Guards three things a green unit suite cannot see:
 *   1. the Inbox is a measured tile GRID (tiles share a row), not a stack;
 *   2. its ✕ really triages — one POST to the canonical action route, the tile
 *      leaves, and a toast says which verb ran. The user reported "pressing X
 *      does nothing" on 2026-07-26 and the mutation had no `onError` at all, so
 *      any rejected write was indistinguishable from a dead button;
 *   3. the connections rail and inflow strip render on the real payload.
 *
 * Not part of `vitest run`: needs the dev server. Run with
 *   npm run e2e:home            (scripts/start-dev.sh must be up)
 *
 * The Inbox assertions need at least one paper in `status='inbox'`. With an
 * empty Inbox the spec SAYS it skipped them rather than passing silently — a
 * quiet skip here would report "triage works" on the strength of never having
 * tried it.
 */
import { chromium } from 'playwright'

const BASE = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'

const failures = []
const check = (ok, message) => {
  console.log(`${ok ? '  ok  ' : '  FAIL'} ${message}`)
  if (!ok) failures.push(message)
}

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1500, height: 1200 } })
const consoleErrors = []
page.on('console', (m) => m.type() === 'error' && consoleErrors.push(m.text().slice(0, 200)))
page.on('pageerror', (e) => consoleErrors.push(String(e).slice(0, 200)))
const actionPosts = []
page.on('response', (r) => {
  if (r.request().method() === 'POST' && r.url().includes('/action')) {
    actionPosts.push(r.status())
  }
})

await page.goto(BASE, { waitUntil: 'networkidle' })
// The onboarding tour lays a full-page scrim that swallows every click — an
// undismissed tour makes ALL of this look broken. Dismiss it for real.
for (let i = 0; i < 6; i++) {
  const skip = page.getByRole('button', { name: /skip tour/i }).first()
  if (!(await skip.count())) break
  await skip.click()
  await page.waitForTimeout(400)
}
await page.waitForTimeout(1500)

check(
  (await page.getByText('Connections', { exact: true }).count()) > 0,
  'connections rail renders',
)
check(
  (await page.getByRole('img', { name: /last 7 days|Nothing arrived in the last 7 days/ }).count()) > 0,
  'inflow strip renders on its plate',
)

const inbox = page.locator('section[aria-labelledby="home-inbox"]')
if (!(await inbox.count())) {
  console.log('  SKIP  Inbox is empty — triage assertions did NOT run')
} else {
  // The count pill must not be folded into the heading's accessible name.
  check(
    (await page.getByRole('heading', { name: 'Inbox', exact: true }).count()) === 1,
    'Inbox heading is still named exactly "Inbox"',
  )

  const bars = inbox.locator('[data-testid="paper-actions"]')
  const before = await bars.count()
  check(before > 0, `Inbox renders ${before} tiles carrying triage controls`)

  if (before > 1) {
    const [a, b] = [await bars.nth(0).boundingBox(), await bars.nth(1).boundingBox()]
    check(Math.abs(a.y - b.y) < 4, 'tiles share a row — a measured grid, not a stack')
  }

  const x = bars.first().getByRole('button', { name: /Clear from Inbox/i })
  check((await x.count()) === 1, 'the ✕ is present and labelled as a clear, not a hide')
  await x.click()
  await page.waitForTimeout(2500)

  check(actionPosts.length === 1 && actionPosts[0] === 200, `✕ posted once, 200 (${actionPosts})`)
  check((await bars.count()) === before - 1, 'the triaged tile left the section')
  const toast = await page
    .locator('[data-sonner-toast]')
    .first()
    .innerText()
    .catch(() => '')
  check(/kept in your corpus/i.test(toast), `✕ confirmed what it did ("${toast.trim()}")`)
}

check(consoleErrors.length === 0, `no console errors (${consoleErrors.join(' | ') || 'none'})`)

await browser.close()
if (failures.length) {
  console.error(`\n${failures.length} failure(s)`)
  process.exit(1)
}
console.log('\nHome desk OK')
