import type { AddonManifest } from '../registry'

const autoTagger: AddonManifest = {
  id: 'auto_tagger',
  operations: {
    prefix: 'tags.',
    label: 'Tag suggestions',
    queryRoots: ['library-tags', 'library-saved', 'tag-suggestions', 'tag-merge-suggestions', 'papers'],
  },
  slots: {
    'tags.tune': () =>
      import('./TagSuggestionsPanel').then((m) => ({ default: m.TagSuggestionsPanel })),
  },
}

export default autoTagger
