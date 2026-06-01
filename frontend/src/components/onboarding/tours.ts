import type { TourStep } from './OnboardingTour'

/**
 * Per-page first-visit tour definitions. Targets are `data-tour` selectors
 * attached to real elements on each page; a step whose target can't be found
 * degrades gracefully to a centred card.
 */

export const FEED_TOUR: TourStep[] = [
  {
    target: '[data-tour="feed-hero"]',
    title: 'Your daily inbox',
    body: 'New papers from the authors and keyword monitors you follow land here, newest first.',
  },
  {
    target: '[data-tour="feed-card"]',
    title: 'React as you read',
    body: 'Save a paper to your library, like it to teach the ranker, or dismiss it to hide it. Every action is signal.',
  },
  {
    target: '[data-tour="feed-monitors"]',
    title: 'Tune what you watch',
    body: 'Manage your keyword and author monitors anytime — by default we track the authors you follow.',
    side: 'top',
  },
]

export const AUTHORS_TOUR: TourStep[] = [
  {
    target: '[data-tour="authors-suggestions"]',
    title: 'People you might follow',
    body: 'Suggestions drawn from your own work, your saved papers, and the authors you already follow.',
  },
  {
    target: '[data-tour="authors-followed"]',
    title: 'Who you track',
    body: 'These authors are monitored — we pull their new work into your Feed and learn from their back catalogue.',
  },
  {
    target: '[data-tour="authors-attention"]',
    title: 'Needs a quick look',
    body: 'Sometimes two profiles look like the same person, or an identity needs confirming. Those show up here.',
    side: 'top',
  },
]

export const LIBRARY_TOUR: TourStep[] = [
  {
    target: '[data-tour="library-workflow"]',
    title: 'What needs a look',
    body: 'Saved papers with metadata gaps surface here, each with a one-tap fix — your curation starting point.',
  },
  {
    target: '[data-tour="library-saved"]',
    title: 'Everything you have saved',
    body: 'Your curated collection. Add tags, collections, topics, notes, and ratings to organise it.',
  },
  {
    target: '[data-tour="library-imports"]',
    title: 'Bring papers in',
    body: 'Import a BibTeX file or a Zotero export, or paste DOIs — they land straight in your library.',
  },
  {
    target: '[data-tour="library-card"]',
    title: 'Open any paper',
    body: 'Click a card for the abstract, citations, and similar papers — your jumping-off point for Discovery.',
    side: 'top',
  },
]

export const DISCOVERY_TOUR: TourStep[] = [
  {
    target: '[data-tour="discovery-lenses"]',
    title: 'Lenses focus the search',
    body: 'A lens is a saved lookout built from a set of papers. Switch lenses to point Discovery at different interests.',
  },
  {
    target: '[data-tour="discovery-branches"]',
    title: 'Branches shape what surfaces',
    body: 'ALMa clusters each lens into branches. Pin, boost, or mute them to steer where Discovery spends its effort.',
  },
  {
    target: '[data-tour="discovery-card"]',
    title: 'Triage to teach',
    body: 'Save, dismiss, and react to recommendations — the more you triage, the sharper the next round.',
    side: 'top',
  },
]
