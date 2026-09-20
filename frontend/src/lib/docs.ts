/** The published documentation site — the one place the app spells its address. */
export const DOCS_BASE_URL = 'https://costantinoai.github.io/alma-library-manager/'

/**
 * A link target for help copy: absolute URLs pass through, a docs-relative
 * path (`/user-guide/reading-pdfs/#shadow-libraries`, as the backend's
 * plugin manifests write them) resolves against the documentation site.
 */
export function docsUrl(pathOrUrl: string): string {
  if (/^https?:\/\//i.test(pathOrUrl)) return pathOrUrl
  return new URL(pathOrUrl.replace(/^\/+/, ''), DOCS_BASE_URL).toString()
}
