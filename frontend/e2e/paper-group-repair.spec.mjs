/** Real isolated app: canonical ingest, Settings repair, Activity and responsive saves. */
import assert from 'node:assert/strict'
import { chromium } from 'playwright'
const base = process.env.ALMA_URL ?? 'http://127.0.0.1:5194'
const api = process.env.ALMA_API ?? 'http://127.0.0.1:8022/api/v1'
const shotDir = process.env.ALMA_SHOT_DIR ?? '/tmp'
async function request(path, body, method = 'POST') {
  const response = await fetch(`${api}${path}`, body === undefined ? {} : {
    method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(body),
  })
  assert.ok(response.ok, `${path}: ${response.status} ${await response.clone().text()}`)
  return response.json()
}
async function settle(jobId, tries = 90) {
  for (let i = 0; i < tries; i++) {
    const job = (await request('/activity')).find(j => j.job_id === jobId)
    if (['completed', 'failed', 'cancelled', 'noop'].includes(job?.status)) return job
    await new Promise(r => setTimeout(r, 1000))
  }
  return undefined
}

const browser = await chromium.launch()
try {
  const page = await browser.newPage({viewport: {width: 1440, height: 1100}})
  await page.addInitScript(() => {
    for (const name of ['home','settings','library','discovery','health','feed']) localStorage.setItem(`alma.tour.${name}.completed`, 'done')
  })
  const errors = []
  page.on('pageerror', e => errors.push(String(e)))
  const nonce = Date.now()
  const title = `Paper group browser proof ${nonce}`
  // Order matters. A preprint arriving AFTER its published version is the case
  // ingest cannot settle — promotion runs for the row being written, and a
  // preprint never absorbs a journal — so the pair is left for the repair to
  // merge. (The other order is covered by the ingest tests.)
  const published = await request('/library/saved', {title, doi:`10.1000/${nonce}`, year:2026, rating:5})
  const pre = await request('/library/saved', {title, doi:`10.1101/${nonce}`, year:2026, notes:'Preserve this note'})
  // Reading state has ONE writer: PATCH /library/papers/{id}/reading-status.
  await request(`/library/papers/${pre.id}/reading-status`, {reading_status:'reading'}, 'PATCH')
  assert.notEqual(published.id, pre.id, 'distinct DOI must create a second row')
  const before = await request(`/papers?search=${encodeURIComponent(title)}&scope=library`)
  assert.equal(before.length, 2, 'two rows, one work — this is what repair is for')

  await page.goto(`${base}/#/settings?anchor=library-management`, {waitUntil:'domcontentloaded'})
  const previewButton = page.getByRole('button', {name:'Preview Paper Groups', exact:true})
  await previewButton.waitFor({timeout:30000})
  await previewButton.scrollIntoViewIfNeeded()

  // 1. Preview: says what it would do, changes nothing.
  const [previewResponse] = await Promise.all([
    page.waitForResponse(r => r.url().includes('/health/operations/paper_group_reconcile/run') && r.request().method()==='POST'),
    previewButton.click(),
  ])
  assert.equal(previewResponse.status(), 200)
  const previewLaunch = await previewResponse.json()
  assert.ok(previewLaunch.job_id, JSON.stringify(previewLaunch))
  const previewJob = await settle(previewLaunch.job_id)
  assert.equal(previewJob?.status, 'completed', JSON.stringify(previewJob))
  assert.match(previewJob.message, /Would repair/)
  const stillTwo = await request(`/papers?search=${encodeURIComponent(title)}&scope=library`)
  assert.equal(stillTwo.length, 2, 'a preview must not merge anything')

  // 2. Run: does exactly what the preview promised.
  const runButton = page.getByRole('button', {name:'Reconcile Paper Groups', exact:true})
  await runButton.scrollIntoViewIfNeeded()
  const [launchResponse] = await Promise.all([
    page.waitForResponse(r => r.url().includes('/health/operations/paper_group_reconcile/run') && r.request().method()==='POST'),
    runButton.click(),
  ])
  assert.equal(launchResponse.status(), 200)
  const launch = await launchResponse.json()
  assert.ok(launch.job_id, JSON.stringify(launch))
  const durations = []
  for (let i=0;i<8;i++) {
    const start = performance.now()
    await request(`/papers/${published.id}/action`, {action:'like', surface:'papers'})
    durations.push(performance.now()-start)
  }
  const job = await settle(launch.job_id)
  assert.equal(job?.status, 'completed', JSON.stringify(job))
  assert.match(job.message, /ambiguous/)
  const logs = await request(`/activity/${launch.job_id}/logs?limit=200`)
  assert.ok(logs.some(l => l.step === 'reconcile_summary'), 'summary visible in Activity')

  // One logical paper now, carrying the version's reading state and notes.
  const after = await request(`/papers?search=${encodeURIComponent(title)}&scope=library`)
  assert.equal(after.length, 1, 'the two rows are one work after repair')
  assert.equal(after[0].id, published.id, 'the published paper is the one that survives')
  assert.equal(after[0].reading_status, 'reading')
  assert.match(after[0].notes ?? '', /Preserve this note/)
  await page.screenshot({path:`${shotDir}/paper-groups-settings.png`,fullPage:true})
  await page.goto(`${base}/#/library`, {waitUntil:'domcontentloaded'})
  await page.waitForTimeout(2000)
  await page.screenshot({path:`${shotDir}/paper-groups-library.png`,fullPage:true})
  assert.deepEqual(errors, [])
  console.log(JSON.stringify({job_id:launch.job_id, message:job.message, foreground_ms:durations.map(Math.round), log_count:logs.length, published:published.id}))
} finally { await browser.close() }
