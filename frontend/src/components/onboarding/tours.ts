import type { TourStep } from './OnboardingTour'

/**
 * Per-page first-visit tour definitions. Targets are `data-tour` selectors
 * attached to real elements on each page; a step whose target can't be found
 * degrades gracefully to a centred card.
 *
 * **A target must be a small, stable anchor** — a header, a control, a single
 * card — never a whole result list. `OnboardingTour` trims an oversized
 * spotlight so a drifted target still reads, but a step pointed at a 6000px
 * section is highlighting nothing in particular. Prefer the section's header
 * row (`…-header`) over the section.
 */

export const HOME_TOUR: TourStep[] = [
  {
    target: '[data-tour="home-brief"]',
    title: 'Your daily brief',
    body: 'What arrived since midnight and what is still waiting — Feed, Discovery, alerts, Inbox, reading list. Every figure is a link to the page it counts.',
  },
  {
    target: '[data-tour="home-status"]',
    title: 'Machinery on the left, decisions on the right',
    body: 'The status rail says what ALMa is running for you and how recently. The chips beside it are the things waiting on YOU — click one to go and settle it.',
  },
  {
    target: '[data-tour="home-inbox"]',
    title: 'Papers you sent yourself',
    body: 'Send a paper from your phone and it lands here for triage. Save it, react to it, or press ✕ for "not now" — which clears it without recording any opinion.',
  },
  {
    target: '[data-tour="home-picked"]',
    title: "Today's shortlist",
    body: 'The strongest new arrivals from your monitors and Discovery, each carrying the reason it surfaced. The full lists live on Feed and Discovery.',
    side: 'top',
  },
]

export const FEED_TOUR: TourStep[] = [
  {
    target: '[data-tour="feed-hero"]',
    title: 'Your daily inbox',
    body: 'New papers from the authors, journals and keyword monitors you follow land here, newest first.',
  },
  {
    target: '[data-tour="feed-scope"]',
    title: 'Inbox and Journals are separate',
    body: 'A followed journal publishes far more than any one author, so its papers get their own subpage instead of flooding the inbox. Switch between them here.',
  },
  {
    target: '[data-tour="feed-card"]',
    title: 'React as you read',
    body: 'Save a paper to your library, like it to teach the ranker, or dismiss it to hide it. Preference and visibility stay separate.',
  },
  {
    target: '[data-tour="feed-monitors"]',
    title: 'Tune what you watch',
    body: 'Manage your keyword, author and journal monitors anytime — by default we track the authors you follow.',
    side: 'top',
  },
]

// The author MAP is not here any more — it moved to the Map page behind the
// Papers / Authors switcher (2026-07-27). This tour is about managing people.
export const AUTHORS_TOUR: TourStep[] = [
  {
    target: '[data-tour="authors-suggestions"]',
    title: 'People you might follow',
    body: 'Suggestions drawn from your own work, your saved papers, and the authors you already follow.',
  },
  {
    target: '[data-tour="authors-followed-header"]',
    title: 'Who you track',
    body: 'These authors are monitored — we pull their new work into your Feed and learn from their back catalogue.',
  },
  {
    target: '[data-tour="authors-attention-header"]',
    title: 'Needs a quick look',
    body: 'Sometimes two profiles look like the same person, or an identity needs confirming. Those show up here.',
    side: 'top',
  },
]

export const LIBRARY_TOUR: TourStep[] = [
  {
    target: '[data-tour="library-workflow"]',
    title: 'What needs a look',
    body: 'Saved papers with metadata gaps surface here, each row saying why it is flagged and what to do — your curation starting point.',
  },
  {
    target: '[data-tour="library-saved"]',
    title: 'Everything you have saved',
    body: 'Your curated collection. Add tags, collections, topics, notes, and ratings to organise it.',
  },
  {
    target: '[data-tour="library-card"]',
    title: 'Open any paper',
    body: 'Open any row for the abstract, citations, and similar papers — your jumping-off point for Discovery. This toggle swaps the compact table for full cards.',
  },
  {
    target: '[data-tour="library-imports"]',
    title: 'Bring papers in',
    body: 'Import a BibTeX file or a Zotero export, or paste DOIs — they land straight in your library.',
  },
  {
    target: '[data-tour="library-analytics"]',
    title: 'How your library is shaped',
    body: 'Source mix, topics, venues, reading progress and structure diagnostics — the charts that used to live on their own Insights page.',
    side: 'top',
  },
]

// Step order follows the page's own top-to-bottom order (lenses → performance
// → Branch Studio → results). A tour that jumps back up the page reads as
// broken even when every target resolves.
export const DISCOVERY_TOUR: TourStep[] = [
  {
    target: '[data-tour="discovery-lenses"]',
    title: 'Lenses focus the search',
    body: 'A lens is a saved lookout built from a set of papers. Switch lenses to point Discovery at different interests.',
  },
  {
    target: '[data-tour="discovery-performance"]',
    title: 'Is it working?',
    body: 'Lens performance reads your reactions back: how much of what you save came from the lens core versus its exploratory push.',
  },
  {
    target: '[data-tour="discovery-branches"]',
    title: 'Branches shape what surfaces',
    body: 'ALMa clusters each lens into branches — its sub-themes. Pin, boost, or mute them in Branch Studio to steer where the next refresh spends its effort.',
  },
  {
    target: '[data-tour="discovery-map"]',
    title: 'See where the suggestions sit',
    body: 'Your library, this lens’s suggestions, and the space between. Click a suggestion to jump to its row, or lasso a region to explore it as a Direction.',
  },
  {
    target: '[data-tour="discovery-card"]',
    title: 'Triage to teach',
    body: 'Save, dismiss, and react to recommendations — the more you triage, the sharper the next round.',
    side: 'top',
  },
]

export const MAP_TOUR: TourStep[] = [
  {
    target: '[data-tour="map-kind"]',
    title: 'Papers or authors',
    body: 'One territory, two views. Papers are placed by what they are about; authors sit at the centre of what they write. Everything else on the page is shared.',
  },
  {
    target: '[data-tour="map-plate"]',
    title: 'Your corpus as territory',
    body: 'Every paper placed by what it is about — near neighbours are semantically close. The words on the terrain name the region under them.',
  },
  {
    target: '[data-tour="map-scope"]',
    title: 'Corpus or Library',
    body: 'Corpus is every paper ALMa tracks; Library is only what you saved. Same map, filtered — never a second, disagreeing layout.',
  },
  {
    target: '[data-tour="map-select"]',
    title: 'Ask about an area',
    body: 'Turn on Select region and drag a box around a patch. ALMa characterises it — vocabulary, strongest papers, top authors — and you can turn it into a lens.',
  },
  {
    target: '[data-tour="map-inspector"]',
    title: 'The map’s vital signs',
    body: 'With nothing selected this reads the whole layout: papers, links, clusters, coverage. Click a paper and it becomes that paper, its cluster, and its neighbourhood.',
    side: 'top',
  },
]
