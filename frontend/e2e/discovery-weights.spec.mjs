/** Real Settings page: the Discovery weights card is truthful and its sliders
 * are effective. Requires the isolated dev stack (`scripts/start-dev.sh`).
 *
 * WRITES: it saves Discovery weights twice (a change, then the change undone)
 * and restores the exact weights it found through the API in `finally`, so the
 * dev profile leaves as it came. No lens is refreshed — the card promises that
 * a save applies at the NEXT refresh, and this spec checks that promise too.
 *
 * What "truthful" means here, and what each check proves:
 *   1. the header "Typical paper scores N" is the API's `reference_score`;
 *   2. each row's "share" is that slider's fraction of the unsaved total, and
 *      the "· saved" suffix appears exactly while it differs from the ranker's
 *      `effective_weights` for the family;
 *   3. saving moves the API's effective weights and reference, and the card
 *      re-reads them (the suffix disappears, the header moves);
 *   4. saving fires no refresh: the only mutation is PUT /discovery/settings.
 */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'

const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const api = process.env.ALMA_API ?? 'http://127.0.0.1:8001/api/v1'
const shots = process.env.ALMA_SHOT_DIR ?? '/tmp'

const settingsUrl = `${api}/discovery/settings`
const getSettings = async () => (await fetch(settingsUrl)).json()
const putWeights = async (weights) => {
  const response = await fetch(settingsUrl, {
    method: 'PUT',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ weights }),
  })
  assert.equal(response.status, 200, `restore PUT returned ${response.status}`)
}
const pct = (v) => `${Math.round(v * 100)}%`

// The weights as found: restored verbatim in `finally`, never a hardcoded set.
const found = await getSettings()
const foundWeights = { ...found.weights }

const browser = await chromium.launch({
  executablePath: process.env.ALMA_BROWSER_EXECUTABLE || undefined,
})
const mutations = []
const errors = []
try {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } })
  page.on('pageerror', (error) => errors.push(String(error)))
  page.on('request', (request) => {
    if (request.url().includes('/api/v1/') && request.method() !== 'GET') {
      mutations.push(`${request.method()} ${new URL(request.url()).pathname}`)
    }
  })

  await page.goto(`${base}/#settings`, { waitUntil: 'domcontentloaded' })
  const card = page.locator('#discovery-weights')
  const header = card.getByText(/^Typical paper scores \d+$/)
  await header.waitFor({ timeout: 30_000 })
  await card.scrollIntoViewIfNeeded()

  const headerNumber = async () => Number((await header.innerText()).match(/\d+/)[0])

  // One slider row, located from its family label. The row is the bordered
  // box three levels above the label paragraph (label → min-w-0 → flex → row).
  const rowOf = (label) => {
    const title = card.locator('p', { hasText: new RegExp(`^${label}`) })
    const row = title.locator('xpath=../../..')
    return {
      title,
      input: row.locator('input[type="number"]'),
      shareText: async () => (await title.locator('span').first().innerText()).replace(/\s+/g, ' ').trim(),
      value: async () => Number(await row.locator('input[type="number"]').inputValue()),
    }
  }
  const recency = rowOf('Recency')

  // ── 1+2. As loaded: header = API reference, share = saved share, no suffix ──
  const before = await getSettings()
  assert.equal(await headerNumber(), Math.round(before.reference_score), 'header is the API reference')
  const sum = Object.values(before.weights).reduce((a, b) => a + b, 0)
  const recencyShare = before.weights.recency_boost / sum
  assert.equal(await recency.shareText(), `share ${pct(recencyShare)}`, 'share is the slider fraction of the total')
  assert.equal(pct(recencyShare), pct(before.effective_weights.recency), 'and matches the ranker at rest')
  await page.screenshot({ path: `${shots}/alma-weights-before.png` })

  // ── 2. Edit: the share re-computes live, and the saved share is disclosed ──
  const raised = 0.9
  const raisedSum = sum - before.weights.recency_boost + raised
  await recency.input.fill(String(raised))
  await page.waitForFunction(
    ([label, expected]) => {
      const p = [...document.querySelectorAll('#discovery-weights p')].find((el) => el.textContent.startsWith(label))
      return p && p.textContent.includes(expected)
    },
    ['Recency', `share ${pct(raised / raisedSum)}`],
  )
  assert.equal(
    await recency.shareText(),
    `share ${pct(raised / raisedSum)} · saved ${pct(before.effective_weights.recency)}`,
    'the unsaved share and the saved share are both shown while they differ',
  )
  assert.equal(await headerNumber(), Math.round(before.reference_score), 'the header reports SAVED weights until saved')
  await page.screenshot({ path: `${shots}/alma-weights-edited.png` })

  // ── 3. Save: the API moves, and the card re-reads what the API now says ──
  const save = card.getByRole('button', { name: 'Save Discovery Settings', exact: true })
  const saved = page.waitForResponse((r) => r.url().endsWith('/discovery/settings') && r.request().method() === 'PUT')
  await save.click()
  assert.equal((await saved).status(), 200)
  const after = await getSettings()
  assert.equal(after.weights.recency_boost, raised, 'the slider value is what the API stored')
  assert.equal(pct(after.effective_weights.recency), pct(raised / raisedSum), 'the effective weight is the rescaled slider')
  assert.notEqual(Math.round(after.reference_score), Math.round(before.reference_score), 'moving a weight moves the reference')
  await page.waitForFunction(
    (expected) => document.querySelector('#discovery-weights')?.textContent.includes(`Typical paper scores ${expected}`),
    Math.round(after.reference_score),
    { timeout: 10_000 },
  )
  assert.equal(await recency.shareText(), `share ${pct(raised / raisedSum)}`, 'saved share now equals the share: suffix gone')
  await page.screenshot({ path: `${shots}/alma-weights-saved.png` })

  // ── 4. Undo through the card, the same way a user would ──
  await recency.input.fill(String(foundWeights.recency_boost))
  const restored = page.waitForResponse((r) => r.url().endsWith('/discovery/settings') && r.request().method() === 'PUT')
  await save.click()
  assert.equal((await restored).status(), 200)
  const back = await getSettings()
  assert.deepEqual(back.weights, foundWeights, 'the card round-trips the weights it found')
  await page.waitForFunction(
    (expected) => document.querySelector('#discovery-weights')?.textContent.includes(`Typical paper scores ${expected}`),
    Math.round(found.reference_score),
    { timeout: 10_000 },
  )
  assert.equal(await recency.shareText(), `share ${pct(recencyShare)}`)

  // ── Saving fires no refresh: the promise "applies at the next refresh" holds ──
  assert.deepEqual([...new Set(mutations)], ['PUT /api/v1/discovery/settings'], `unexpected mutations: ${mutations}`)
  assert.deepEqual(errors, [])

  console.log(
    [
      `reference: ${before.reference_score.toFixed(1)} (found) → ${after.reference_score.toFixed(1)} (recency=${raised}) → ${back.reference_score.toFixed(1)} (restored)`,
      `recency effective: ${before.effective_weights.recency.toFixed(4)} → ${after.effective_weights.recency.toFixed(4)} → ${back.effective_weights.recency.toFixed(4)}`,
      `mutations: ${mutations.join(', ')}`,
      `screenshots: ${shots}/alma-weights-{before,edited,saved}.png`,
      'PASS',
    ].join('\n'),
  )
} finally {
  await browser.close()
  // Safety net: whatever happened above, dev leaves with the weights it had.
  await putWeights(foundWeights)
  const final = await getSettings()
  assert.deepEqual(final.weights, foundWeights, 'dev weights restored')
}
