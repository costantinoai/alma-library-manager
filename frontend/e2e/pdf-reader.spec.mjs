/**
 * Paper PDFs, driven for real on a phone-sized browser and a desktop one.
 *
 * Guards what unit tests cannot see:
 *   1. a paper card's PDF link is a real link that opens a NEW tab from the tap
 *      (phones block tabs opened after an async wait) and that tab ends on the
 *      served PDF (application/pdf, inline);
 *   2. the handoff page for a paper with no PDF says what each source said and
 *      offers Try again / Attach — it does not silently re-fetch;
 *   3. the detail dialog shows the kept PDF (chip + Open);
 *   4. the Import dialog has a "PDF files" tab with a drop zone.
 *
 * Needs a dev server with the Open-access PDFs plugin on and:
 *   PDF_PAPER     a paper id that HAS a kept PDF
 *   NO_PDF_PAPER  a paper id that was looked for and has none
 * Run:  ALMA_URL=http://127.0.0.1:5183 PDF_PAPER=… NO_PDF_PAPER=… npm run e2e:pdf
 * Screenshots go to $SHOTS_DIR (default /tmp) — LOOK at them.
 */
import { chromium, devices } from 'playwright'

const BASE = process.env.ALMA_URL ?? 'http://127.0.0.1:5173'
const PDF_PAPER = process.env.PDF_PAPER
const NO_PDF_PAPER = process.env.NO_PDF_PAPER
const SHOTS = process.env.SHOTS_DIR ?? '/tmp'
if (!PDF_PAPER || !NO_PDF_PAPER) {
  console.error('Set PDF_PAPER and NO_PDF_PAPER (paper ids).')
  process.exit(2)
}

const failures = []
const check = (ok, message) => {
  console.log(`${ok ? '  ok  ' : '  FAIL'} ${message}`)
  if (!ok) failures.push(message)
}

async function dismissTour(page) {
  for (let i = 0; i < 6; i++) {
    const skip = page.getByRole('button', { name: /skip tour/i }).first()
    if (!(await skip.count())) break
    await skip.click()
    await page.waitForTimeout(300)
  }
}

const browser = await chromium.launch()

// ── 1–2. Phone: tap the card's PDF link; the reader for a paper without one ──
const phone = await browser.newContext({ ...devices['iPhone 13'] })
const mobile = await phone.newPage()
const consoleErrors = []
mobile.on('pageerror', (e) => consoleErrors.push(String(e).slice(0, 200)))
await mobile.goto(`${BASE}/#/library`, { waitUntil: 'networkidle' })
await dismissTour(mobile)
await mobile.waitForTimeout(1200)

const pdfLinks = mobile.getByRole('link', { name: 'Open PDF' })
check((await pdfLinks.count()) > 0, 'Library cards show a PDF link when a PDF source is on')
const target = mobile.locator(`a[aria-label="Open PDF"][href*="${PDF_PAPER}"]`).first()
if (await target.count()) {
  check((await target.getAttribute('target')) === '_blank', 'the PDF link opens a new tab (from the tap itself)')
  const [tab] = await Promise.all([phone.waitForEvent('page'), target.tap()])
  const pdfResponse = await tab.waitForResponse((r) => r.url().includes(`/papers/${PDF_PAPER}/pdf`), { timeout: 20000 })
  check(pdfResponse.status() === 200, `the tab ends on the PDF (HTTP ${pdfResponse.status()})`)
  check(
    (pdfResponse.headers()['content-type'] ?? '').startsWith('application/pdf'),
    'served as application/pdf',
  )
  check((pdfResponse.headers()['content-disposition'] ?? '').startsWith('inline'), 'served inline')
  await tab.close()
} else {
  check(false, `no PDF link for ${PDF_PAPER} on the first Library screen (scroll/filter?)`)
}

const reader = await phone.newPage()
await reader.goto(`${BASE}/#/read?paper=${NO_PDF_PAPER}`, { waitUntil: 'networkidle' })
await reader.waitForTimeout(1500)
check((await reader.getByText('No PDF found for this paper yet.').count()) === 1, 'reader says no PDF was found')
check((await reader.getByRole('button', { name: /try again/i }).count()) === 1, 'reader offers Try again')
check((await reader.getByRole('button', { name: /attach a pdf/i }).count()) === 1, 'reader offers Attach')
check((await reader.getByText(/: (no copy listed|not a PDF|server refused|not set up)/).count()) > 0, 'reader lists what each source said')
await reader.screenshot({ path: `${SHOTS}/pdf-reader-none-390.png`, fullPage: true })
await mobile.screenshot({ path: `${SHOTS}/pdf-library-390.png`, fullPage: false })
await phone.close()

// ── 3–4. Desktop: detail dialog + Import dialog ──
const desk = await browser.newPage({ viewport: { width: 1440, height: 1000 } })
desk.on('pageerror', (e) => consoleErrors.push(String(e).slice(0, 200)))
await desk.goto(`${BASE}/#/library`, { waitUntil: 'networkidle' })
await dismissTour(desk)
await desk.waitForTimeout(1200)
const card = desk.locator(`a[aria-label="Open PDF"][href*="${PDF_PAPER}"]`).first()
if (await card.count()) {
  // Open the paper's detail dialog through its title (the card's own click target).
  const title = card.locator('xpath=ancestor::*[.//h3 or .//*[@role="heading"]][1]').locator('h3, [role="heading"]').first()
  if (await title.count()) await title.click()
  else await card.locator('xpath=../..').click()
  const dialog = desk.getByRole('dialog')
  await dialog.waitFor({ timeout: 8000 }).catch(() => {})
  check((await dialog.getByText(/^PDF · /).count()) > 0, 'detail dialog shows the kept PDF chip')
  check((await dialog.getByRole('link', { name: 'Open PDF' }).count()) > 0, 'detail dialog offers Open PDF')
  await desk.screenshot({ path: `${SHOTS}/pdf-detail-1440.png` })
  await desk.keyboard.press('Escape')
} else {
  check(false, `no PDF link for ${PDF_PAPER} on the desktop Library screen`)
}

const importButton = desk.getByRole('button', { name: /^import/i }).first()
if (await importButton.count()) {
  await importButton.click()
  await desk.getByRole('tab', { name: /pdf files/i }).click()
  await desk.waitForTimeout(600) // let the tab indicator settle before the screenshot
  check((await desk.getByText('Drop PDF files here or click to browse').count()) === 1, 'Import dialog has the PDF files tab')
  check(
    (await desk.getByRole('tab', { name: /pdf files/i }).getAttribute('aria-selected')) === 'true',
    'the PDF files tab is the selected one',
  )
  await desk.screenshot({ path: `${SHOTS}/pdf-import-tab-1440.png` })
} else {
  check(false, 'no Import button on the Library page')
}

check(consoleErrors.length === 0, `no page errors (${consoleErrors.join(' | ')})`)
await browser.close()
if (failures.length) {
  console.error(`\n${failures.length} check(s) failed`)
  process.exit(1)
}
console.log('\nall PDF checks passed')
