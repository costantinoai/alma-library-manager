/**
 * Signal Lab must survive being PLAYED against the real backend.
 *
 * Every defect this guards against was invisible to the unit suite, because
 * the unit suite mocks one game with one deck and never switches:
 *
 *   1. switching games unmounted the whole band until the new deck arrived
 *      (`/queue` can take tens of seconds), so a toggle press looked broken;
 *   2. progress was one shared cursor, so switching back replayed rounds you
 *      had already answered — and the backend rejects a second answer for a
 *      spent token, making those rounds unanswerable;
 *   3. an answer that the server refuses must surface, not fail silently.
 *
 * So this drives the real thing: answer in game A, switch to B, answer, switch
 * back, and assert the cursor stayed where it was and every POST was accepted.
 *
 * Not part of `vitest run`: needs the dev server AND a corpus with a built
 * `graph:super_regions` view. Run with
 *   npm run e2e:signal-lab       (scripts/start-dev.sh must be up)
 *
 * With no deck available the spec SAYS it skipped rather than passing quietly —
 * a silent skip here would report "the games work" having never played one.
 */
import { chromium } from 'playwright'

const BASE = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
// `/queue` designs a whole deck server-side and has been seen to take ~35s.
const DECK_TIMEOUT_MS = Number(process.env.ALMA_DECK_TIMEOUT_MS ?? 120_000)

const failures = []
const check = (ok, message) => {
  console.log(`${ok ? '  ok  ' : '  FAIL'} ${message}`)
  if (!ok) failures.push(message)
}

const browser = await chromium.launch()
const page = await browser.newPage({ viewport: { width: 1400, height: 1200 } })

const consoleErrors = []
page.on('pageerror', (e) => consoleErrors.push(String(e).slice(0, 200)))
page.on('console', (m) => m.type() === 'error' && consoleErrors.push(m.text().slice(0, 200)))

/** Every answer POST the run made, with its status. */
const answers = []
page.on('response', (r) => {
  if (r.request().method() === 'POST' && r.url().includes('/signal-lab/')) {
    answers.push({ url: r.url(), status: r.status() })
  }
})

const section = () =>
  page.locator('section').filter({ has: page.getByRole('heading', { name: 'Signal Lab' }) }).first()

/** The "N / M" progress readout, as a number pair. */
async function progress() {
  const text = await section().getByText(/^\d+ \/ \d+$/).first().textContent()
  const [done, total] = text.split('/').map((part) => Number(part.trim()))
  return { done, total }
}

/** Wait for a dealt deck — the readout stops showing the loading dash. */
async function waitForDeck() {
  await section()
    .getByText(/^\d+ \/ \d+$/)
    .first()
    .waitFor({ state: 'visible', timeout: DECK_TIMEOUT_MS })
}

await page.goto(`${BASE}/#home`, { waitUntil: 'domcontentloaded' })
await page.waitForTimeout(3000)
const skipTour = page.getByRole('button', { name: /Skip tour/i })
if (await skipTour.count()) await skipTour.first().click()

if ((await section().count()) === 0) {
  // The band hides itself until the corpus can serve a deck. Say so loudly.
  console.log('SKIPPED — Signal Lab is not being served (no deck / no super-regions view).')
  await browser.close()
  process.exit(0)
}

await waitForDeck()
const started = await progress()
check(started.total >= 10, `Favourites deck dealt (${started.total} rounds)`)

// ── Play one Favourites round ──────────────────────────────────────────────
await section().getByRole('button', { name: /is your most favourite/ }).first().click()
check(
  await section().getByText('Now pick the other one').isVisible(),
  'one verdict given → the band asks for the other half of the pair',
)
await section().getByRole('button', { name: /is your least favourite/ }).nth(2).click()

await page
  .waitForFunction(
    (before) =>
      [...document.querySelectorAll('span')].some((el) => {
        const m = /^(\d+) \/ (\d+)$/.exec(el.textContent?.trim() ?? '')
        return m && Number(m[1]) > before
      }),
    started.done,
    { timeout: 30_000 },
  )
  .catch(() => {})

const afterFavourites = await progress()
check(
  afterFavourites.done === started.done + 1,
  `Favourites recorded and advanced (${started.done} → ${afterFavourites.done})`,
)

// ── Switch to Odd one out ──────────────────────────────────────────────────
await section().getByRole('radio', { name: 'Odd one out' }).click()
check(
  (await section().count()) === 1,
  'the band stays mounted while the second deck is dealt (it used to vanish)',
)
await waitForDeck()
const oddStart = await progress()
check(oddStart.total >= 10, `Odd-one-out deck dealt (${oddStart.total} rounds)`)
check(
  (await section().getByRole('button', { name: /does not belong/ }).count()) === 3,
  'odd-one-out shows one verdict per paper',
)

await section().getByRole('button', { name: /does not belong/ }).first().click()
await page.waitForTimeout(2500)
const oddAfter = await progress()
check(oddAfter.done === oddStart.done + 1, 'odd-one-out records on the single mark')

// ── Switch back: the Favourites cursor must be where we left it ────────────
await section().getByRole('radio', { name: 'Favourites' }).click()
await waitForDeck()
const backAgain = await progress()
check(
  backAgain.done === afterFavourites.done,
  `switching back resumes Favourites at ${afterFavourites.done}, not 0 (was ${backAgain.done})`,
)

// A round we can actually answer: the token must still be unspent.
await section().getByRole('button', { name: /is your most favourite/ }).first().click()
await section().getByRole('button', { name: /is your least favourite/ }).nth(2).click()
await page.waitForTimeout(3000)

// ── Every answer the run posted must have been accepted ────────────────────
check(answers.length >= 3, `${answers.length} answers posted to the live API`)
const rejected = answers.filter((a) => a.status >= 400)
check(rejected.length === 0, `no answer was rejected (${rejected.map((r) => r.status).join(', ') || 'none'})`)
check(consoleErrors.length === 0, `no console errors (${consoleErrors[0] ?? 'none'})`)

await browser.close()
console.log(failures.length ? `\n${failures.length} FAILED` : '\nall checks passed')
process.exit(failures.length ? 1 : 0)
