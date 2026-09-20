/** Real app: valence buttons outside Feed/Discovery honour the ONE paper-action
 * contract. Requires a running dev stack.
 *
 * The bug this pins (2026-09-19): the Author panel's Add/Like/Love and the
 * Reading list's "Save to Library" posted to `POST /library/saved` with a
 * hand-picked star value — Save at 0★, Like/Love at 4/5★ with NO feedback
 * signal, so those likes never reached Discovery. Both now go through
 * `POST /papers/{id}/action`, which applies the backend's rating map
 * (add 3 · like 4 · love 5) and records the signal.
 *
 * WRITES: saves one Reading-list paper and likes one author publication, then
 * restores both through the app's own undo action. Run against a dev/isolated
 * profile, never prod.
 */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const api = process.env.ALMA_API ?? 'http://127.0.0.1:8001/api/v1'
const shots = process.env.ALMA_SHOT_DIR ?? '/tmp'

const json = async (method, path, body) => {
  const response = await fetch(`${api}${path}`, {
    method,
    headers: body ? { 'content-type': 'application/json' } : {},
    body: body ? JSON.stringify(body) : undefined,
  })
  assert.ok(response.ok, `${method} ${path} → ${response.status}`)
  return response.status === 204 ? null : response.json()
}
const details = (id) => json('GET', `/papers/${encodeURIComponent(id)}/details`)
const undoAll = (id) =>
  json('POST', `/papers/${encodeURIComponent(id)}/action`, { action: 'undo', surface: 'library', undo_aspect: 'all' })

const browser = await chromium.launch({ executablePath: process.env.ALMA_BROWSER_EXECUTABLE || undefined })
const errors = []
const touched = [] // [paperId, restore()] — undone in `finally`
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } })
  await page.addInitScript(() => {
    for (const key of ['library', 'authors', 'discovery']) localStorage.setItem(`alma.tour.${key}.completed`, 'done')
  })
  page.on('pageerror', (error) => errors.push(String(error)))

  /** Click `control`, expect exactly the action route with `action`, return the payload. */
  const clickExpectingAction = async (control, paperId, action) => {
    await control.evaluate((element) => element.scrollIntoView({ block: 'center' }))
    // Capture whatever write the click sends, so a regression names the wrong
    // route instead of timing out waiting for the right one.
    const [response] = await Promise.all([
      page.waitForResponse((r) => {
        const path = new URL(r.url()).pathname
        return r.request().method() !== 'GET' && path.startsWith('/api/v1/') && !/\/(seen|impressions)$/.test(path)
      }),
      control.click(),
    ])
    const path = new URL(response.url()).pathname
    assert.ok(path.endsWith(`/papers/${paperId}/action`), `${action} hit ${path}, not the action route`)
    assert.equal(response.request().postDataJSON().action, action)
    assert.equal(response.status(), 200)
  }

  // ── 1. Reading list → "Save to Library" is the `add` action (3★) ─────────
  const lenses = await json('GET', '/lenses')
  const recs = await json('GET', `/lenses/${lenses[0].id}/recommendations?limit=200`)
  const candidate = recs.find((r) => r.paper && r.paper.status !== 'library' && !r.paper.reading_status && !r.paper.rating)
  assert.ok(candidate, 'no untouched tracked paper to put on the reading list')
  const readId = candidate.paper_id
  const readTitle = (await details(readId)).title
  await json('PATCH', `/library/papers/${readId}/reading-status`, { reading_status: 'reading' })
  touched.push([readId, async () => {
    await undoAll(readId)
    await json('PATCH', `/library/papers/${readId}/reading-status`, { reading_status: null })
  }])

  await page.goto(`${base}/#/library?tab=reading`, { waitUntil: 'domcontentloaded' })
  const titleNode = page.getByText(readTitle.slice(0, 60)).first()
  await titleNode.waitFor({ timeout: 30_000 })
  const saveButton = (await titleNode.evaluateHandle((node) => {
    const isSave = (b) => b.textContent?.trim() === 'Save to Library'
    let element = node
    while (element && ![...element.querySelectorAll('button')].some(isSave)) element = element.parentElement
    return element ? [...element.querySelectorAll('button')].find(isSave) : null
  })).asElement()
  assert.ok(saveButton, 'no "Save to Library" on the Reading-list card')
  await clickExpectingAction(saveButton, readId, 'add')
  const saved = await details(readId)
  assert.equal(saved.status, 'library')
  assert.equal(saved.rating, 3, 'Save must store the contract rating (add = 3★), not 0★')
  await page.screenshot({ path: `${shots}/paper-actions-1-reading-save.png` })

  // ── 2. Author panel → Like is the `like` action (4★ + signal) ───────────
  // Snapshot the paper's exact state BEFORE the write lands, so the restore
  // puts back what was there (it may already be a Library paper).
  let original = null
  await page.route('**/api/v1/papers/*/action', async (route) => {
    const id = decodeURIComponent(new URL(route.request().url()).pathname.split('/papers/')[1].split('/')[0])
    if (original === null) original = { id, ...(await details(id)) }
    await route.continue()
  })
  const authors = await json('GET', '/authors')
  let likeId = null
  for (const author of authors.slice(0, 25)) {
    await page.goto(`${base}/#/authors?author=${encodeURIComponent(author.id)}`, { waitUntil: 'domcontentloaded' })
    const dialog = page.getByRole('dialog')
    try {
      await dialog.waitFor({ timeout: 15_000 })
    } catch {
      continue
    }
    await dialog.getByRole('tab', { name: 'Publications' }).click()
    await page.waitForTimeout(1500)
    const likeButtons = dialog.getByTitle('Like — save to library with a positive signal')
    const count = await likeButtons.count()
    for (let i = 0; i < count && likeId === null; i += 1) {
      const button = likeButtons.nth(i)
      if ((await button.getAttribute('aria-pressed')) === 'true') continue
      await button.evaluate((element) => element.scrollIntoView({ block: 'center' }))
      const [request] = await Promise.all([
        page.waitForRequest((r) => r.method() === 'POST' && /\/papers\/[^/]+\/action$/.test(new URL(r.url()).pathname)),
        button.click(),
      ])
      likeId = decodeURIComponent(new URL(request.url()).pathname.split('/papers/')[1].split('/')[0])
      assert.equal(request.postDataJSON().action, 'like')
      assert.equal(request.postDataJSON().surface, 'papers')
      assert.equal((await request.response()).status(), 200)
    }
    if (likeId) break
    await page.keyboard.press('Escape')
  }
  assert.ok(likeId, 'no author publication with an un-pressed Like in the first 25 authors')
  const before = original
  touched.push([likeId, async () => {
    // Undo exactly what the Like changed: its rating, and membership only if
    // the paper was not already in the Library.
    await json('POST', `/papers/${encodeURIComponent(likeId)}/action`, { action: 'undo', surface: 'library', undo_aspect: 'rating' })
    if (before.status !== 'library') {
      await json('POST', `/papers/${encodeURIComponent(likeId)}/action`, { action: 'undo', surface: 'library', undo_aspect: 'membership' })
    }
    if (before.rating) await json('PUT', `/library/saved/${encodeURIComponent(likeId)}?rating=${before.rating}`)
  }])
  await page.waitForTimeout(800)
  const liked = await details(likeId)
  assert.equal(liked.rating, 4, 'Like must store the contract rating (4★)')
  await page.screenshot({ path: `${shots}/paper-actions-2-author-like.png` })

  assert.deepEqual(errors, [], `page errors: ${errors.join(' | ')}`)
  console.log(`OK paper-action contract · reading save ${readId} · author like ${likeId}`)
} finally {
  for (const [id, restore] of touched.reverse()) {
    await restore().catch((error) => console.error(`restore failed for ${id}: ${error}`))
  }
  await browser.close()
}
