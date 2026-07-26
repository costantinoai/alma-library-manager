/**
 * Region selection must visibly answer the drag — on BOTH map hosts.
 *
 * This guards the exact failure the user reported on 2026-07-26: the lasso was
 * wired correctly on the Map page, but its only feedback was a Card below a
 * 560px plate, so dragging a box appeared to do nothing. Authors had no lasso
 * at all. Neither gap was visible to TypeScript, vitest, or 993 backend tests —
 * it took driving the real app.
 *
 * Not part of `vitest run`: needs the dev server. Run with
 *   npm run e2e:maps            (scripts/start-dev.sh must be up)
 *
 * Two assertions per host, and the second is the one that matters:
 *   1. the lasso produces a region at all;
 *   2. the region's feedback is REACHABLE — an on-plate card, plus a drilldown
 *      within a viewport of the plate rather than stranded below the fold.
 */
import { chromium } from 'playwright'

const BASE = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
/** Max px between plate bottom and drilldown before it counts as stranded. */
const REACHABLE_GAP = 900

const failures = []
const check = (ok, message) => {
  console.log(`${ok ? '  ok  ' : '  FAIL'} ${message}`)
  if (!ok) failures.push(message)
}

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1600, height: 1100 } })
const consoleErrors = []
page.on('console', (m) => m.type() === 'error' && consoleErrors.push(m.text().slice(0, 200)))
page.on('pageerror', (e) => consoleErrors.push(String(e).slice(0, 200)))

/**
 * The onboarding tour lays a `fixed inset-0 z-[55]` scrim over the page. It must
 * be dismissed with a REAL click: `force: true` dispatches at the coordinates
 * and the scrim swallows the event, which silently produces a no-op drag and
 * looks exactly like a broken lasso.
 */
async function dismissTour() {
  for (let i = 0; i < 6; i++) {
    const skip = page.getByRole('button', { name: /skip tour/i }).first()
    if (!(await skip.count())) break
    await skip.click()
    await page.waitForTimeout(400)
  }
}

async function lassoTheMiddle() {
  const canvas = page.locator('canvas').first()
  await canvas.waitFor({ timeout: 30_000 })
  // ALMa scrolls in an inner container, so window.scrollY stays 0 while the
  // plate sits above the viewport. Scroll it in before trusting its box.
  await canvas.scrollIntoViewIfNeeded()
  await page.waitForTimeout(500)
  const box = await canvas.boundingBox()
  const [x1, y1] = [box.x + box.width * 0.25, box.y + box.height * 0.25]
  const [x2, y2] = [box.x + box.width * 0.75, box.y + box.height * 0.75]
  await page.mouse.move(x1, y1)
  await page.mouse.down()
  await page.mouse.move((x1 + x2) / 2, (y1 + y2) / 2, { steps: 8 })
  await page.mouse.move(x2, y2, { steps: 8 })
  await page.mouse.up()
  await page.waitForTimeout(3000)
  return canvas
}

for (const host of [
  { name: 'Map', hash: '#/map', drilldown: /^Region — \d+ papers$/ },
  { name: 'Authors', hash: '#/authors', drilldown: /^Area — \d+ authors$/ },
]) {
  console.log(`\n=== ${host.name} ===`)
  await page.goto(`${BASE}/${host.hash}`, { waitUntil: 'domcontentloaded' })
  await page.waitForTimeout(6000)
  await dismissTour()

  const toggle = page.getByRole('button', { name: /select region/i }).first()
  check((await toggle.count()) > 0, `${host.name}: "Select region" control exists`)
  if (!(await toggle.count())) continue

  const canvas = page.locator('canvas').first()
  await canvas.scrollIntoViewIfNeeded()
  await toggle.click()
  await page.waitForTimeout(300)
  check(
    (await canvas.evaluate((el) => el.style.cursor)) === 'crosshair',
    `${host.name}: toggling puts the plate in lasso mode`,
  )

  await lassoTheMiddle()

  const dismiss = page.getByRole('button', { name: /cancel selection/i }).first()
  check((await dismiss.count()) > 0, `${host.name}: on-plate region card appears`)

  const drill = page.getByText(host.drilldown).first()
  const found = (await drill.count()) > 0
  check(found, `${host.name}: dense drilldown appears`)
  if (found) {
    const gap = await page.evaluate((pattern) => {
      const c = document.querySelector('canvas')
      const el = [...document.querySelectorAll('p')].find((n) =>
        new RegExp(pattern).test(n.textContent?.trim() ?? ''),
      )
      if (!c || !el) return null
      return Math.round(el.getBoundingClientRect().top - c.getBoundingClientRect().bottom)
    }, host.drilldown.source)
    check(
      gap != null && gap < REACHABLE_GAP,
      `${host.name}: drilldown is reachable, not stranded below the fold (gap ${gap}px)`,
    )
  }
}

console.log('\n=== console errors ===')
console.log(consoleErrors.length ? [...new Set(consoleErrors)].join('\n') : '  none')
check(consoleErrors.length === 0, 'no console errors while selecting regions')

await browser.close()
if (failures.length) {
  console.error(`\n${failures.length} check(s) failed`)
  process.exit(1)
}
console.log('\nall region-selection checks passed')
