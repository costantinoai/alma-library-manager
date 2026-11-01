import { byRarity, signalLabel, type SignalKind } from '@/components/shared/signalKinds'

/** Shape read from `recommendation.score_breakdown`. All fields optional —
 * the component renders nothing when every "why" signal is missing. */
export interface ProvenanceSignals {
  branchLabel?: string | null
  branchMode?: string | null
  /** The ranked selection left this card out; the branch's reserved slot kept
   *  it. Stated on the card so its place is not read as merit. */
  branchFloor?: boolean
  firstScreenLift?: boolean
  sourceType?: string | null
  sourceApi?: string | null
  matchedQuery?: string | null
  branchCoreTopics?: string[] | null
  branchExploreTopics?: string[] | null
  // T4 numeric provenance (stamped into `score_breakdown.provenance`
  // at retrieval time). All optional — the component skips any chip
  // whose value is missing or zero, so legacy pre-T4 rows render
  // silently without ghost chips.
  specterCosine?: number | null
  lexicalSimilarity?: number | null
  sharedAuthorsCount?: number | null
  sharedAuthorsSample?: string | null
  /** True when the candidate's embedding landed close to a
   *  disliked / removed seed. Rendered as a warning-tone
   *  chip so the user can weigh the hit. */
  negativeHit?: number | null
  /** Number of independent retrieval sources that surfaced this
   *  candidate (channels + distinct external source APIs). Renders as
   *  a "Suggested by N sources" chip when N ≥ 2. */
  consensusCount?: number | null
  /** Signed projected-feedback adjustment in [-1, 1]: net pull from
   *  the user's saved / disliked / removed papers, their authors, topics,
   *  venues, etc. Positive → "matches what you keep"; negative →
   *  "near things you've rejected". Rendered only when the
   *  magnitude clears a small noise floor. */
  projectedFeedbackRaw?: number | null
  /** Citation fabric (task 47 §7). Number of references this candidate shares
   *  with a saved/loved paper (bibliographic coupling) + that partner's title,
   *  and the number of papers that cite it together with a saved/loved paper
   *  (co-citation) + that partner's title. Each renders a chip only when the
   *  count is ≥ 1, with the evidence string on hover. */
  couplingCount?: number | null
  couplingPartnerTitle?: string | null
  cocitationCount?: number | null
  cocitationPartnerTitle?: string | null
}

export interface Chip {
  key: string
  /** The short word or two on the chip. */
  label: string
  /** Semantic category from the shared registry — decides colour AND glyph.
   *  Never hand-pick a tone here; the registry is the single owner of what a
   *  chip's colour means across the whole app. */
  kind: SignalKind
  /** What the chip means in full, with its exact figure, on hover or tap. */
  explain: string
  /** Compared across the list to find how rare this chip is. Counted chips
   *  share one key; a graded chip counts per grade, since "Same topic" says
   *  more than the "Near topic" on half the cards. */
  rarityKey: string
  /** Fetch machinery, not merit: always after the evidence. */
  plumbing?: boolean
}

/** Map a [0,1] similarity to a plain-language band (U-11). */
function similarityBand(v: number): string {
  if (v >= 0.8) return 'Same'
  if (v >= 0.6) return 'Near'
  if (v >= 0.4) return 'Some'
  return 'Far'
}
const BAND_WORDS: Record<string, string> = {
  Same: 'Very close',
  Near: 'Close',
  Some: 'Related',
  Far: 'Loosely related',
}

function wordingBand(v: number): string {
  const band = similarityBand(v)
  return band === 'Far' ? 'Few' : band
}

/** Retrieval lanes in a word: the prefix only repeats the mode or the source. */
function laneLabel(lane: string): string {
  return lane.replace(/^branch_|^graph_|^taste_|_local$|_win$/g, '').replace(/_/g, ' ')
}

export function buildChips(signals: ProvenanceSignals): Chip[] {
  const chips: Chip[] = []
  const add = (chip: Omit<Chip, 'rarityKey'> & { rarityKey?: string }) =>
    chips.push({ ...chip, rarityKey: chip.rarityKey ?? chip.key })

  // First: it changes how every other chip on this card should be read.
  if (signals.branchFloor) {
    add({
      key: 'branch-floor',
      label: 'Reserved',
      kind: 'branch',
      explain:
        'Reserved branch slot. The ranked selection left this one out, but every active branch keeps its best cards, so it stayed. Its score is unchanged.',
    })
  } else if (signals.firstScreenLift) {
    add({
      key: 'first-screen-lift',
      label: 'Moved up',
      kind: 'branch',
      explain:
        'Moved up for its branch. Every active branch shows one card before Show all, and this is the best of its branch. Its score is unchanged.',
    })
  }

  if (typeof signals.consensusCount === 'number' && signals.consensusCount >= 2) {
    add({
      key: 'consensus',
      label: signalLabel('consensus', signals.consensusCount),
      kind: 'consensus',
      explain: `Found by ${signals.consensusCount} independent retrieval sources.`,
    })
  }

  // Numeric "why" chips (T4). Rendered only when the underlying signal cleared
  // its threshold at scoring time.
  if (typeof signals.specterCosine === 'number' && signals.specterCosine > 0) {
    const band = similarityBand(signals.specterCosine)
    add({
      key: 'specter',
      label: signalLabel('topic', band),
      kind: 'topic',
      explain: `${BAND_WORDS[band]} topic to your library. SPECTER2 embedding cosine ${signals.specterCosine.toFixed(2)}.`,
      rarityKey: `specter:${band}`,
    })
  }
  if (typeof signals.lexicalSimilarity === 'number' && signals.lexicalSimilarity > 0.05) {
    const band = wordingBand(signals.lexicalSimilarity)
    add({
      key: 'lexical',
      label: signalLabel('wording', band),
      kind: 'wording',
      explain: `${BAND_WORDS[similarityBand(signals.lexicalSimilarity)]} wording to your library. Keyword similarity ${signals.lexicalSimilarity.toFixed(2)}.`,
      rarityKey: `lexical:${band}`,
    })
  }
  if (typeof signals.sharedAuthorsCount === 'number' && signals.sharedAuthorsCount > 0) {
    const sample = (signals.sharedAuthorsSample || '').trim()
    add({
      key: 'authors',
      label: signalLabel('authors', signals.sharedAuthorsCount),
      kind: 'authors',
      explain:
        signals.sharedAuthorsCount === 1 && sample
          ? `Shares an author with your library: ${sample}.`
          : `Shares ${signals.sharedAuthorsCount} authors with your library.`,
    })
  }
  if (typeof signals.negativeHit === 'number' && signals.negativeHit >= 0.35) {
    add({
      key: 'neg-hit',
      label: signalLabel('taste-avoid', ''),
      kind: 'taste-avoid',
      explain: `Near a paper you disliked. Proximity ${signals.negativeHit.toFixed(2)}.`,
    })
  }
  // Projected feedback: the signed pull from the user's per-paper history.
  // A threshold of 0.05 keeps the chip silent when the signal is near zero.
  if (
    typeof signals.projectedFeedbackRaw === 'number' &&
    Math.abs(signals.projectedFeedbackRaw) >= 0.05
  ) {
    const positive = signals.projectedFeedbackRaw > 0
    const pull = `${positive ? '+' : ''}${signals.projectedFeedbackRaw.toFixed(2)}`
    add({
      key: 'projected',
      label: positive ? signalLabel('taste-match', '') : 'Passed on',
      kind: positive ? 'taste-match' : 'taste-avoid',
      explain: positive
        ? `Matches what you save. Pull from your saves, ratings and removals: ${pull}.`
        : `Near things you pass on. Pull from your saves, ratings and removals: ${pull}.`,
      rarityKey: `projected:${positive}`,
    })
  }

  // Citation fabric: shared references (coupling) + cited-together (co-citation)
  // with the user's saved/loved papers. The partner's title is in the explanation.
  if (typeof signals.couplingCount === 'number' && signals.couplingCount >= 1) {
    const withTitle = signals.couplingPartnerTitle
      ? ` with “${signals.couplingPartnerTitle}”`
      : ' with your library'
    add({
      key: 'coupling',
      label: signalLabel('coupling', signals.couplingCount),
      kind: 'coupling',
      explain: `Shares ${signals.couplingCount} reference${signals.couplingCount === 1 ? '' : 's'}${withTitle}.`,
    })
  }
  if (typeof signals.cocitationCount === 'number' && signals.cocitationCount >= 1) {
    const withTitle = signals.cocitationPartnerTitle
      ? ` with “${signals.cocitationPartnerTitle}”`
      : ' with a saved paper'
    add({
      key: 'cocitation',
      label: signalLabel('cocitation', signals.cocitationCount),
      kind: 'cocitation',
      explain: `Cited together${withTitle} in ${signals.cocitationCount} paper${signals.cocitationCount === 1 ? '' : 's'}.`,
    })
  }

  // Retrieval plumbing — WHICH branch pursued it, HOW it was fetched, and
  // WHERE from. These explain the machinery, not the merit.
  if (signals.branchLabel) {
    add({
      key: 'branch',
      label: signals.branchLabel,
      kind: 'branch',
      explain: `Lens branch: ${signals.branchLabel}.`,
      rarityKey: `branch:${signals.branchLabel}`,
    })
  }
  if (signals.branchMode) {
    const mode = signals.branchMode.replace(/_/g, ' ')
    add({
      key: 'mode',
      label: mode,
      kind: 'channel',
      explain: `Branch mode: ${mode}.`,
      rarityKey: `mode:${mode}`,
      plumbing: true,
    })
  }
  if (signals.sourceType) {
    const lane = signals.sourceType.replace(/_/g, ' ')
    const label = laneLabel(signals.sourceType)
    // "branch core" beside mode "core" says the same word twice.
    if (label !== (signals.branchMode ?? '').replace(/_/g, ' ')) {
      add({
        key: 'source',
        label,
        kind: 'channel',
        explain: `Retrieval lane: ${lane}.`,
        rarityKey: `source:${lane}`,
        plumbing: true,
      })
    }
  }
  if (signals.sourceApi) {
    add({
      key: 'api',
      label: signals.sourceApi,
      kind: 'source',
      explain: `Fetched from ${signals.sourceApi}.`,
      rarityKey: `api:${signals.sourceApi}`,
      plumbing: true,
    })
  }
  return chips
}

/** What `byRarity` compares, for every card in the visible list. */
export function provenanceRarityKeys(signals: ProvenanceSignals): string[] {
  return buildChips(signals).map((chip) => chip.rarityKey)
}

/** Evidence first, rarest first, machinery last. */
export function orderChips(chips: Chip[], counts?: Map<string, number>): Chip[] {
  const ordered = byRarity(chips, (chip) => chip.rarityKey, counts)
  return [...ordered.filter((chip) => !chip.plumbing), ...ordered.filter((chip) => chip.plumbing)]
}
