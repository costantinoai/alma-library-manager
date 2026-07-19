#!/usr/bin/env bash
#
# ALMa release — the ONE command that cuts a release, fully local.
#
#   scripts/release.sh 0.22.0            # full release
#   scripts/release.sh 0.22.0 --check    # preflight only, no writes
#   scripts/release.sh 0.22.0 --skip-tests
#
# Replaces the extension/hooks/pre-push hook (per-clone, silent when
# missing) with an explicit, resumable pipeline:
#
#   preflight -> bump -> commit -> tag -> sign connector (from the TAG,
#   via git archive) -> push -> GitHub release (curated notes) -> upload
#
# Trust model (deliberate):
#   - The AMO signing key lives ONLY in ~/.config/alma/amo.env (0600).
#     It is never stored with GitHub or any CI. Signing itself happens on
#     addons.mozilla.org (Mozilla issued the key — that counterparty is
#     inherent to shipping a signed Firefox add-on).
#   - GitHub only hosts the repo and the public artifacts; `gh` uses your
#     existing local auth. No repository/CI secrets exist.
#   - The signing toolchain (web-ext) is version-pinned in
#     extension/release.sh to bound local supply-chain exposure.
#
# Resumable: every step is guarded by an "already done?" probe, so
# re-running the same version after a partial failure continues where it
# stopped (AMO signs a given version exactly once — a re-run must reuse
# the signed artifact, not re-sign).
#
# Release notes are a COMMITTED file: docs/releases/v<version>.md must
# exist before you run this. That keeps notes curated, reviewed, and
# versioned instead of ad-hoc.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

REPO="costantinoai/alma-library-manager"
CREDS="$HOME/.config/alma/amo.env"

VERSION="${1:-}"
shift || true
CHECK_ONLY=0
SKIP_TESTS=0
while [ $# -gt 0 ]; do
  case "$1" in
    --check) CHECK_ONLY=1 ;;
    --skip-tests) SKIP_TESTS=1 ;;
    -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
  shift
done

[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Usage: scripts/release.sh X.Y.Z [--check] [--skip-tests]"; exit 2; }
TAG="v$VERSION"
NOTES="docs/releases/$TAG.md"
XPI="dist/alma-connector-$VERSION.xpi"

say()  { printf '\033[1m[release]\033[0m %s\n' "$*"; }
fail() { printf '\033[31m[release] FAIL:\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Preflight — every gate that must hold before ANY write.
# ---------------------------------------------------------------------------
say "preflight for $TAG"

current_branch="$(git rev-parse --abbrev-ref HEAD)"
[ "$current_branch" = "main" ] || fail "on '$current_branch', releases cut from main"

# Bump commit for THIS version may already exist (resume); otherwise the
# tree must be clean so the bump commit contains exactly the bump.
if [ -z "$(git status --porcelain)" ]; then :; else
  fail "working tree not clean — commit or stash first ('tests/' and 'tasks/' are gitignored and don't count)"
fi

current_version="$(grep -m1 -E '^version *= *"' pyproject.toml | sed -E 's/^version *= *"([^"]+)".*/\1/')"
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  [ "$current_version" = "$VERSION" ] || fail "tag $TAG exists but pyproject says $current_version — inconsistent state"
  say "tag $TAG already exists — resume mode"
else
  [ "$current_version" != "$VERSION" ] || fail "pyproject already at $VERSION but tag $TAG missing — commit/tag by hand or pick a new version"
fi

[ -f "$NOTES" ] || fail "release notes missing: write $NOTES first (they become the GitHub release body)"
command -v gh >/dev/null || fail "gh (GitHub CLI) not installed"
gh auth status >/dev/null 2>&1 || fail "gh not authenticated"
[ -f "$CREDS" ] || fail "AMO credentials missing: $CREDS"
command -v npx >/dev/null || fail "npx (Node) not installed"
command -v docker >/dev/null || say "note: docker not found — fine, GHCR build runs on GitHub"

say "preflight OK"
[ "$CHECK_ONLY" -eq 1 ] && { say "--check: stopping before any writes"; exit 0; }

# ---------------------------------------------------------------------------
# Tests — the release gate. Full backend suite + frontend typecheck/tests.
# ---------------------------------------------------------------------------
if [ "$SKIP_TESTS" -eq 1 ]; then
  say "SKIPPING tests (--skip-tests)"
elif git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  say "resume mode — tests already gated this version, skipping"
else
  say "backend suite (this takes ~20 min)…"
  .venv/bin/python -m pytest tests -q -m "not network" || fail "backend suite red"
  say "frontend typecheck + tests…"
  (cd frontend && npx tsc --noEmit && npx vitest run) || fail "frontend red"
fi

# ---------------------------------------------------------------------------
# Bump + commit + tag (skipped entirely in resume mode).
# ---------------------------------------------------------------------------
if ! git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  say "bumping to $VERSION"
  sed -i -E "s/^version = \"[^\"]+\"/version = \"$VERSION\"/" pyproject.toml
  node -e 'const fs=require("fs");for(const f of ["extension/manifest.json","extension/package.json"]){const m=JSON.parse(fs.readFileSync(f,"utf8"));m.version=process.argv[1];fs.writeFileSync(f,JSON.stringify(m,null,2)+"\n")}' "$VERSION"
  git add pyproject.toml extension/manifest.json extension/package.json "$NOTES"
  git commit -m "chore(release): $TAG"
  git tag "$TAG"
  say "committed + tagged $TAG"
fi

# ---------------------------------------------------------------------------
# Connector — signed FROM THE TAG. Reuses an existing artifact on resume
# (AMO refuses to re-sign a version).
# ---------------------------------------------------------------------------
if [ -f "$XPI" ]; then
  say "signed connector already present: $XPI (resume)"
else
  say "building + signing connector from $TAG"
  extension/release.sh --local --version "$VERSION" --from-tag "$TAG" || fail "connector build/sign failed"
  [ -f "$XPI" ] || fail "expected artifact missing: $XPI"
fi

# ---------------------------------------------------------------------------
# Publish — push, then release + artifact. Each step no-ops when done.
# ---------------------------------------------------------------------------
say "pushing main + $TAG (triggers the GHCR image build)"
git push origin main "$TAG"

if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  say "release $TAG exists — syncing notes + artifact"
  gh release edit "$TAG" --repo "$REPO" --notes-file "$NOTES"
else
  gh release create "$TAG" --repo "$REPO" --title "$TAG" --notes-file "$NOTES"
fi
gh release upload "$TAG" "$XPI" --repo "$REPO" --clobber

say "done: https://github.com/$REPO/releases/tag/$TAG"
say "next: watch the GHCR build (gh run list --workflow=docker-publish.yml),"
say "      then deploy with scripts/deploy-prod.sh $VERSION"
