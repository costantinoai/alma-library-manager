/** Real isolated dev app. Reversible reading action and temporary monitor CRUD. */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'
const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const browser = await chromium.launch({ executablePath: process.env.ALMA_BROWSER_EXECUTABLE || undefined })
const errors = []
let monitorId, readingRestore
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
  page.on('pageerror', e => errors.push(String(e)))
  await page.addInitScript(() => {
    localStorage.setItem('alma.tour.feed.completed', 'done')
    localStorage.setItem('alma.feed.hideLibrary', '0')
  })
  const requests = []
  page.on('request', r => requests.push(r.url()))
  await page.goto(`${base}/#/feed`)
  const card = page.locator('[data-tour="feed-card"]').first()
  await card.waitFor({ timeout: 30_000 })
  const skip = page.getByRole('button', { name: 'Skip tour', exact: true })
  if (await skip.isVisible()) await skip.click()
  const fold = page.locator('summary', { hasText: 'Tune monitors' })
  assert.equal(await page.getByRole('button', { name: 'Add Monitor', exact: true }).count(), 0)
  assert.equal(await page.getByRole('button', { name: 'Refresh Inbox', exact: true }).isEnabled(), true)
  assert.equal(await card.getByRole('button', { name: 'Save to library', exact: true }).isVisible(), true)
  // Queue is orthogonal to preference and membership. Restore the exact old state.
  const responseWait = page.waitForResponse(r => r.url().endsWith('/reading-status') && r.request().method() === 'PATCH')
  await card.getByRole('button', { name: 'Add to reading list — decide later', exact: true }).click()
  const readingResponse = await responseWait
  assert.ok(readingResponse.ok())
  readingRestore = readingResponse.url()
  await card.getByRole('button', { name: 'Remove from reading list', exact: true }).waitFor()
  assert.equal(await card.isVisible(), true, 'Queue keeps the paper in Feed')
  const restored = await page.request.patch(readingRestore, { data: { reading_status: null } })
  assert.ok(restored.ok()); readingRestore = undefined
  await page.reload(); await card.waitFor()

  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 1000 })
    await page.waitForTimeout(250)
    await page.evaluate(() => document.querySelector('main')?.scrollTo(0, 0))
    await page.screenshot({ path: `/tmp/alma-68-feed-default-${width}.png` })
    await fold.focus(); await page.keyboard.press('Enter')
    await page.getByRole('button', { name: 'Add Monitor', exact: true }).waitFor()
    assert.equal(await page.getByRole('switch').first().isEnabled(), true)
    const input = page.getByPlaceholder('Display label', { exact: true }).first()
    await input.fill('Unsubmitted monitor draft')
    await fold.evaluate(el => el.scrollIntoView({ block: 'start' }))
    await page.screenshot({ path: `/tmp/alma-68-feed-expanded-${width}.png` })
    const bounds = await page.locator('[data-tour="feed-monitors"]').boundingBox()
    assert.ok(bounds.x >= 0 && bounds.x + bounds.width <= width + 1, 'Monitor panel fits viewport')
    await fold.click()
    await fold.click()
    assert.equal(await input.inputValue(), 'Unsubmitted monitor draft', 'Fold retains draft')
    await input.fill(''); await fold.click()
  }
  await page.setViewportSize({ width: 1440, height: 1000 })
  await fold.click()
  const label = `ALMa task 68 browser ${Date.now()}`
  await page.getByPlaceholder('Display label', { exact: true }).first().fill(label)
  await page.getByPlaceholder('e.g. (manifold OR topology) AND representations NOT images', { exact: true }).fill(label)
  const createdWait = page.waitForResponse(r => r.url().endsWith('/feed/monitors') && r.request().method() === 'POST')
  await page.getByRole('button', { name: 'Add Monitor', exact: true }).click()
  const created = await createdWait; assert.ok(created.ok())
  const payload = await created.json(); monitorId = payload.id
  assert.ok(monitorId)
  // Locate by the canonical row's unique Rule text, avoiding other monitor forms.
  const monitorRow = page.locator(`[data-monitor-id="${monitorId}"]`)
  await monitorRow.locator('summary').waitFor()
  assert.match(await monitorRow.locator('summary').innerText(), /Show/i, 'An open parent must not label a closed child Hide')
  await monitorRow.locator('summary').click()
  await monitorRow.getByRole('button', { name: 'Delete', exact: true }).waitFor()
  await monitorRow.getByLabel('Monitor name', { exact: true }).fill(`${label} edited`)
  await monitorRow.getByRole('checkbox').uncheck()
  const editedWait = page.waitForResponse(r => r.url().endsWith(`/feed/monitors/${monitorId}`) && ['PUT', 'PATCH'].includes(r.request().method()))
  await monitorRow.getByRole('button', { name: 'Save', exact: true }).click()
  const edited = await editedWait; assert.ok(edited.ok())
  assert.equal((await edited.json()).enabled, false)
  await monitorRow.locator('summary').getByText(`${label} edited`, { exact: true }).waitFor()
  const deleteWait = page.waitForResponse(r => r.url().endsWith(`/feed/monitors/${monitorId}`) && r.request().method() === 'DELETE')
  await monitorRow.getByRole('button', { name: 'Delete', exact: true }).click()
  assert.ok((await deleteWait).ok()); monitorId = undefined
  await fold.click()
  await page.goto(`${base}/#/feed?monitor=missing-monitor`)
  await page.getByText(/Monitor: missing-monitor/).waitFor()
  await page.getByRole('button', { name: 'Clear source filter' }).click()
  await card.waitFor()
  // Real page, controlled transport outage: no fake healthy state behind a fold.
  await page.route('**/api/v1/feed/monitors', route => route.fulfill({ status: 503, contentType: 'application/json', body: '{"detail":"unavailable"}' }))
  await page.reload()
  await page.getByText('Feed source or schedule status is unavailable.', { exact: true }).waitFor({ timeout: 30_000 })
  await page.unroute('**/api/v1/feed/monitors')
  await page.getByRole('button', { name: 'Retry', exact: true }).click()
  await page.getByText('Feed source or schedule status is unavailable.', { exact: true }).waitFor({ state: 'hidden' })
  await page.goto(`${base}/#/settings?anchor=feed-monitors`)
  await page.getByRole('button', { name: 'Tune Feed monitors — add, edit, or remove sources' }).click()
  await page.getByRole('button', { name: 'Add Monitor', exact: true }).waitFor()
  assert.deepEqual(errors, [])
  console.log('PASS Feed: default actions, reversible Queue, keyboard/mobile disclosure, retained drafts, real monitor create/delete, source filter clear, outage/retry, Settings deep link.')
} finally {
  const request = await browser.newContext()
  if (monitorId) await request.request.delete(`${base}/api/v1/feed/monitors/${monitorId}`)
  if (readingRestore) await request.request.patch(readingRestore, { data: { reading_status: null } })
  await browser.close()
}
