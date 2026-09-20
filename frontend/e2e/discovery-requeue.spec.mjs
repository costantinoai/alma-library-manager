/** Real Discovery page: "Add to reading list" works, and keeps working after
 * the paper leaves the reading list. Requires a running dev stack.
 *
 * The bug this pins (2026-09-19): a Discovery card read "Queued" from the
 * recommendation's `user_action='read'` stamp as well as from the paper. The
 * stamp is history — it is never cleared — so once a queued paper left the
 * reading list (Library → Reading list → remove, or marked done), every lens
 * still showed "Queued", and clicking the button called UNDO instead of Queue:
 * the paper could never be put back on the reading list from Discovery.
 *
 * A second path to the same symptom: each recommendation embeds the live
 * paper and Discovery caches the list for 60 s, but Library's reading-status
 * mutations never invalidated it — so removing the paper in Library and coming
 * straight back showed the cached "Queued" and the click ran undo again.
 *
 * WRITES: queues one paper three times and clears its reading status twice;
 * restores the reading status it found in `finally`. The recommendation keeps
 * its `user_action='read'` stamp (history by design) — run against a
 * dev/isolated profile, never prod.
 *
 * Steps and what each proves:
 *   1. a first-time Queue POSTs `/papers/{id}/action` with action 'read' and
 *      the paper's reading_status becomes 'reading' (the card flips to Queued);
 *   2. IN-APP: remove it on Library → Reading list, return to Discovery inside
 *      the cache window — the card offers Queue again and re-queueing works;
 *   3. RELOAD: it leaves the reading list elsewhere (API), a full reload shows
 *      Queue again, not the history stamp — and re-queueing works.
 */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const api = process.env.ALMA_API ?? 'http://127.0.0.1:8001/api/v1'
const shots = process.env.ALMA_SHOT_DIR ?? '/tmp'

const ADD = 'Add to reading list — decide later'
const REMOVE = 'Remove from reading list'

const readingStatus = async (paperId) => {
  const response = await fetch(`${api}/papers/${encodeURIComponent(paperId)}/details`)
  assert.equal(response.status, 200, `details GET returned ${response.status}`)
  return (await response.json()).reading_status ?? null
}
const setReadingStatus = async (paperId, value) => {
  const response = await fetch(`${api}/library/papers/${encodeURIComponent(paperId)}/reading-status`, {
    method: 'PATCH',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ reading_status: value }),
  })
  assert.equal(response.status, 200, `reading-status PATCH returned ${response.status}`)
}

const lenses = await (await fetch(`${api}/lenses`)).json()
assert.ok(lenses.length > 0, 'no Discovery lens to test against')
const lensId = lenses[0].id

const browser = await chromium.launch({
  executablePath: process.env.ALMA_BROWSER_EXECUTABLE || undefined,
})
const errors = []
const mutations = []
let paperId = null
let found = null
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } })
  // A fresh browser has never seen the Discovery tour; its full-screen scrim
  // would swallow every click. Mark it seen, as a returning user has.
  await page.addInitScript(() => {
    localStorage.setItem('alma.tour.discovery.completed', 'done')
    localStorage.setItem('alma.tour.library.completed', 'done')
  })
  page.on('pageerror', (error) => errors.push(String(error)))
  page.on('request', (request) => {
    if (request.url().includes('/api/v1/') && request.method() !== 'GET') {
      mutations.push(`${request.method()} ${new URL(request.url()).pathname}`)
    }
  })
  const openDiscovery = async () => {
    await page.goto(`${base}/#/discovery?lens=${lensId}`, { waitUntil: 'domcontentloaded' })
    await page.locator('[id^="rec-card-"]').first().waitFor({ timeout: 30_000 })
  }
  const queueOnce = async (card) => {
    // Centre the card: at the viewport's bottom edge its action bar sits under
    // the fixed Activity dock, where a click lands on the dock instead.
    await card.evaluate((element) => element.scrollIntoView({ block: 'center' }))
    const [response] = await Promise.all([
      page.waitForResponse(
        (r) => r.request().method() === 'POST' && r.url().includes(`/papers/${paperId}/`),
      ),
      card.getByTitle(ADD).click(),
    ])
    const path = new URL(response.url()).pathname
    assert.ok(path.endsWith(`/papers/${paperId}/action`), `Queue click hit ${path}, not the action route`)
    assert.equal(response.request().postDataJSON().action, 'read')
    assert.equal(response.status(), 200, `action POST returned ${response.status()}`)
  }

  await openDiscovery()
  // The first card that currently offers Queue (not saved, not queued).
  const cards = page.locator('[id^="rec-card-"]')
  const count = await cards.count()
  for (let i = 0; i < count && paperId === null; i += 1) {
    if ((await cards.nth(i).getByTitle(ADD).count()) > 0) {
      paperId = (await cards.nth(i).getAttribute('id')).slice('rec-card-'.length)
    }
  }
  assert.ok(paperId, 'no Discovery card offers "Add to reading list"')
  found = await readingStatus(paperId)
  assert.ok(!found, `picked paper already has reading_status=${found}`)
  const card = page.locator(`#rec-card-${paperId}`)

  // 1. First-time queue.
  await queueOnce(card)
  await card.getByTitle(REMOVE).waitFor({ timeout: 10_000 })
  assert.equal(await readingStatus(paperId), 'reading')
  await card.screenshot({ path: `${shots}/discovery-requeue-1-queued.png` })

  const offersQueueAgain = async (label) => {
    const again = page.locator(`#rec-card-${paperId}`)
    await again.waitFor({ timeout: 30_000 })
    await again.evaluate((element) => element.scrollIntoView({ block: 'center' }))
    assert.equal(await again.getByTitle(REMOVE).count(), 0, `${label}: stale "Queued" after leaving the reading list`)
    assert.equal(await again.getByTitle(ADD).count(), 1, `${label}: card does not offer Queue again`)
    return again
  }
  const title = (await (await fetch(`${api}/papers/${encodeURIComponent(paperId)}/details`)).json()).title

  // 2. IN-APP: Library → Reading list → Remove, then straight back to Discovery
  //    (hash navigation: same SPA, same query cache, inside its 60 s window).
  await page.evaluate(() => { window.location.hash = '#/library?tab=reading' })
  const titleNode = page.getByText(title.slice(0, 60)).first()
  await titleNode.waitFor({ timeout: 30_000 })
  const removeButton = (await titleNode.evaluateHandle((node) => {
    const isRemove = (button) => button.textContent?.trim() === 'Remove from List'
    let element = node
    while (element && ![...element.querySelectorAll('button')].some(isRemove)) element = element.parentElement
    return element ? [...element.querySelectorAll('button')].find(isRemove) : null
  })).asElement()
  assert.ok(removeButton, 'no "Remove from List" button on the Reading list card')
  await Promise.all([
    page.waitForResponse((r) => r.request().method() === 'PATCH' && r.url().includes(`/library/papers/${paperId}/reading-status`)),
    removeButton.click(),
  ])
  assert.equal(await readingStatus(paperId), null)
  await page.evaluate((id) => { window.location.hash = `#/discovery?lens=${id}` }, lensId)
  const inApp = await offersQueueAgain('in-app')
  await inApp.screenshot({ path: `${shots}/discovery-requeue-2-in-app.png` })
  await queueOnce(inApp)
  await inApp.getByTitle(REMOVE).waitFor({ timeout: 10_000 })
  assert.equal(await readingStatus(paperId), 'reading')

  // 3. RELOAD: it leaves the reading list elsewhere; a full reload must read
  //    the paper, not the recommendation's history stamp.
  await setReadingStatus(paperId, null)
  await page.reload({ waitUntil: 'domcontentloaded' })
  const reloaded = await offersQueueAgain('reload')
  await reloaded.screenshot({ path: `${shots}/discovery-requeue-3-reload.png` })
  await queueOnce(reloaded)
  await reloaded.getByTitle(REMOVE).waitFor({ timeout: 10_000 })
  assert.equal(await readingStatus(paperId), 'reading')

  assert.deepEqual(errors, [], `page errors: ${errors.join(' | ')}`)
  console.log(`OK discovery re-queue · paper ${paperId} · mutations: ${mutations.join(', ')}`)
} catch (error) {
  console.error(`FAILED · paper ${paperId} · mutations: ${mutations.join(', ') || 'none'}`)
  throw error
} finally {
  if (paperId && found !== undefined) {
    await setReadingStatus(paperId, found).catch((error) =>
      console.error(`could not restore reading_status for ${paperId}: ${error}`),
    )
  }
  await browser.close()
}
