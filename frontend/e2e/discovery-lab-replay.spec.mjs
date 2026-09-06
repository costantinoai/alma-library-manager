/** Real Discovery → score explanation → immutable Lab replay.
 * Requires a dev profile with Library suggestions and a fitted, enabled Lab.
 * --refresh explicitly replaces ONE Library lens deck through the real UI;
 * without it, inspects the current deck (useful after a rendering-only fix).
 * Never changes Lab settings/answers or saves/dismisses papers.
 */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const refresh = process.argv.includes('--refresh')
assert.ok(['localhost', '127.0.0.1', '[::1]'].includes(new URL(base).hostname))
const browser = await chromium.launch({
  executablePath: process.env.ALMA_BROWSER_EXECUTABLE || undefined,
})
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } })
  const errors = []
  const mutations = []
  page.on('pageerror', (error) => errors.push(String(error)))
  page.on('request', (request) => {
    if (request.method() !== 'GET') mutations.push(new URL(request.url()).pathname)
  })
  async function get(path) {
    const response = await page.request.get(`${base}/api/v1${path}`, { timeout: 30_000 })
    assert.ok(response.ok(), `${path}: HTTP ${response.status()}`)
    return response.json()
  }
  async function until(label, read, accept, timeout = 180_000) {
    const deadline = Date.now() + timeout
    let last
    let nextLog = 0
    while (Date.now() < deadline) {
      last = await read()
      if (accept(last)) return last
      if (Date.now() >= nextLog) {
        console.log(`Waiting: ${label}${last?.message ? ` — ${last.message}` : ''}`)
        nextLog = Date.now() + 15_000
      }
      await new Promise((resolve) => setTimeout(resolve, 2000))
    }
    throw new Error(`${label} timed out: ${JSON.stringify(last).slice(0, 1000)}`)
  }

  const identity = await get('/extension/ping')
  assert.equal(identity.instance.profile, 'dev', 'never run this workflow on production')
  const settings = await get('/signal-lab/settings')
  assert.equal(settings.enabled, true, 'enable/fitting are prerequisites, never fabricated by the test')
  assert.equal((await get('/signal-lab/model')).ready, true)
  const initialLenses = await get('/lenses')
  const lens = initialLenses.find((item) => item.context_type === 'library_global')
  assert.ok(lens, 'a Library lens exists')
  const initialEval = await get('/signal-lab/eval')

  await page.goto(`${base}/#discovery`, { waitUntil: 'domcontentloaded' })
  // Everything page-level is scoped to the <main> landmark. The sidebar carries
  // a "Library" nav button too, so an unscoped exact-name click is ambiguous
  // between the nav item and the lens chip — and navigating away silently
  // leaves no Refresh control to press (seen 2026-09-06: the run failed as a
  // 30s "no POST" timeout, which reads like a broken button, not a bad selector).
  // The onboarding tour (and any popover a click opens) lays a full-screen
  // `fixed inset-0` shield over the page. Clicks then retry for 30s and fail as
  // "element intercepts pointer events", which reads like a broken control.
  // Clear the shield before every interaction instead.
  async function clearOverlays() {
    const skipTour = page.getByRole('button', { name: /Skip tour/i })
    if (await skipTour.count()) await skipTour.first().click()
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const shield = page.locator('div.fixed.inset-0')
      if ((await shield.count()) === 0) return
      await page.keyboard.press('Escape')
      await page.waitForTimeout(300)
    }
  }
  await page.waitForTimeout(2000)
  await clearOverlays()
  const main = page.getByRole('main')
  const lensChip = main.getByRole('button', { name: lens.name, exact: true }).first()
  await lensChip.waitFor({ timeout: 30_000 })
  await lensChip.click()
  const refreshButton = main.getByRole('button', { name: 'Refresh lens', exact: true })
  await refreshButton.waitFor({ timeout: 30_000 })
  await clearOverlays()
  assert.equal(await refreshButton.isEnabled(), true, 'Refresh lens is actionable')
  if (refresh) {
    const responsePromise = page.waitForResponse((response) => (
      response.request().method() === 'POST'
      && new URL(response.url()).pathname === `/api/v1/lenses/${lens.id}/refresh`
    ))
    await refreshButton.click()
    const response = await responsePromise
    assert.ok(response.ok())
    const job = await response.json()
    assert.notEqual(job.status, 'already_running', 'do not claim someone else’s refresh as this test')
    console.log(`Queued ${job.job_id}`)
    const done = await until('Discovery refresh', () => get(`/activity/${job.job_id}`),
      (value) => ['completed', 'failed', 'cancelled'].includes(value.status), 300_000)
    assert.equal(done.status, 'completed', JSON.stringify(done))
    console.log(`Completed ${job.job_id}`)
  }
  const currentLens = (await get('/lenses')).find((item) => item.id === lens.id)
  if (refresh) assert.notEqual(currentLens.last_suggestion_set_id, lens.last_suggestion_set_id)
  const setId = currentLens.last_suggestion_set_id
  const evaluation = await until('replay of current deck', () => get('/signal-lab/eval'),
    (value) => value.replay?.lenses.some((item) => item.lens_id === lens.id && item.suggestion_set_id === setId))
  const replay = evaluation.replay.lenses.find((item) => item.lens_id === lens.id)
  assert.equal(replay.status, 'ok', replay.reason)
  assert.ok(replay.lab_measured > 0)
  assert.ok(replay.parity.checked > 0)
  assert.equal(replay.parity.mismatched, 0)
  assert.equal(currentLens.last_retrieval_summary.feature_schema_version, evaluation.replay.feature_schema_version)
  assert.equal(currentLens.last_ranker_version, evaluation.replay.ranker_version)
  for (const old of initialEval.replay?.lenses ?? []) {
    if (old.lens_id !== lens.id && old.reason?.includes('predate')) {
      const unchanged = evaluation.replay.lenses.find((item) => item.lens_id === old.lens_id)
      assert.equal(unchanged.status, 'unassessable')
      assert.match(unchanged.reason, /predate.*refresh the lens/)
    }
  }
  const recommendations = await get(`/lenses/${lens.id}/recommendations?limit=100&hide_library=true`)
  // The row nests its paper (`row.paper.title`); pick the largest Lab effect in
  // the deck so the rendered row is the clearest instance, not the first one.
  const labPointsOf = (item) => item.score_breakdown?.explanation?.adjustments
    ?.find((row) => row.key === 'signal_lab')?.points ?? 0
  // Pick from the cards the deck ACTUALLY renders, not from the API order: the
  // page applies its own filters and paging, so a row that is 12th in this
  // response can be absent from the screen, and asserting on an off-screen card
  // only proves the test can scroll.
  await clearOverlays()
  const cards = page.locator('[class~="group/paper-card"]')
  await cards.first().waitFor({ timeout: 30_000 })
  const renderedText = await cards.evaluateAll((nodes) => nodes.map((node) => node.textContent ?? ''))
  const paper = recommendations
    .filter((item) => item.suggestion_set_id === setId
      && labPointsOf(item) !== 0
      && item.paper?.title
      && renderedText.some((text) => text.includes(item.paper.title)))
    .sort((a, b) => Math.abs(labPointsOf(b)) - Math.abs(labPointsOf(a)))[0]
  assert.ok(paper, 'a rendered card carries a non-zero Signal Lab contribution')
  const title = paper.paper.title
  const explanation = paper.score_breakdown.explanation
  const lab = explanation.adjustments.find((row) => row.key === 'signal_lab')
  assert.equal(explanation.adjustments.filter((row) => row.key === 'signal_lab').length, 1)
  assert.equal(lab.atoms.length, 2)
  assert.ok(Math.abs(lab.atoms.reduce((sum, atom) => sum + atom.points, 0) - lab.points) < 1e-4)
  const sum = explanation.families.reduce((total, family) => total + family.points, 0)
    + explanation.adjustments.reduce((total, row) => total + row.points, 0) + explanation.clipped
  assert.ok(Math.abs(sum - explanation.final_score) < 1e-4, 'explanation closes to the score')

  // The card is already on screen (it is how `paper` was chosen); no reload, no
  // second refresh, no invented fixture.
  const card = cards.filter({ hasText: title }).first()
  await card.waitFor({ timeout: 30_000 })
  await card.scrollIntoViewIfNeeded()
  await card.getByRole('button', { name: 'Why', exact: true }).click()
  const labRow = card.getByRole('button', { name: /Signal Lab/ })
  await labRow.waitFor({ state: 'visible' })
  assert.equal(await labRow.getAttribute('aria-expanded'), 'false')
  await labRow.click()
  assert.equal(await labRow.getAttribute('aria-expanded'), 'true')
  for (const atom of lab.atoms) assert.equal(await card.getByText(atom.label, { exact: true }).isVisible(), true)
  await card.scrollIntoViewIfNeeded()
  await page.screenshot({ path: '/tmp/alma-discovery-lab-replay.png' })
  await page.setViewportSize({ width: 390, height: 844 })
  await labRow.scrollIntoViewIfNeeded()
  const bounds = await labRow.boundingBox()
  assert.ok(bounds && bounds.x >= 0 && bounds.x + bounds.width <= 390)
  await page.screenshot({ path: '/tmp/alma-discovery-lab-replay-mobile.png' })
  assert.deepEqual(errors, [])
  assert.equal(mutations.filter((path) => path.endsWith('/refresh')).length, refresh ? 1 : 0)
  assert.deepEqual(mutations.filter((path) => path.includes('/signal-lab/') || path.endsWith('/action')), [])
  console.log(JSON.stringify({
    result: 'PASS', refreshed: refresh, profile: identity.instance.profile, setId,
    parity: replay.parity, churn: replay.churn, paper: title,
    labPoints: lab.points, atoms: lab.atoms,
  }))
} finally {
  await browser.close()
}
