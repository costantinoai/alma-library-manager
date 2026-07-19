#!/usr/bin/env bash
#
# Build (and optionally publish) the ALMa Firefox connector — always LOCAL
# signing, distributed as a plain GitHub Release artifact.
#
#   extension/release.sh                 build + sign locally, then (after a
#                                        y/N confirm) upload the .xpi to the
#                                        v<version> GitHub Release.
#   extension/release.sh --local         build + sign only — no upload, no
#                                        git/GitHub writes. Artifact in dist/.
#   extension/release.sh --version X     use version X (the pre-push hook
#                                        passes the tag's version).
#   extension/release.sh --target REF    commit-ish for a release/tag created
#                                        by `gh` when it doesn't exist yet.
#
# Signing is local; the only WRITE (uploading / creating the release+tag) is
# gated behind a confirmation read from the terminal. No auto-update: each
# release just carries the signed .xpi, which users download to update.
#
# Requirements: npx (Node); for upload also gh (GitHub CLI, authenticated);
# a free AMO API key in ~/.config/alma/amo.env (chmod 600) as
#   export AMO_JWT_ISSUER=...   export AMO_JWT_SECRET=...
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXT="$ROOT/extension"
OUT="$ROOT/dist"
REPO="costantinoai/alma-library-manager"
CREDS="$HOME/.config/alma/amo.env"

# Pinned signing toolchain. A floating `npx web-ext` would run whatever npm
# published last night with the AMO key in its environment; pinning bounds
# the local supply-chain exposure to a version we've actually used.
WEB_EXT="web-ext@10.5.0"

LOCAL_ONLY=0
VERSION=""
TARGET=""
FROM_TAG=""
while [ $# -gt 0 ]; do
  case "$1" in
    --local) LOCAL_ONLY=1 ;;
    --version) VERSION="${2:?--version needs a value}"; shift ;;
    --target) TARGET="${2:?--target needs a value}"; shift ;;
    --from-tag) FROM_TAG="${2:?--from-tag needs a tag}"; shift ;;
    -h|--help) sed -n '2,27p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1 (try --local / --version X / --from-tag vX / --help)"; exit 2 ;;
  esac
  shift
done

cd "$ROOT"

# Credentials (kept outside the repo).
[ -f "$CREDS" ] && . "$CREDS"
: "${AMO_JWT_ISSUER:?set AMO_JWT_ISSUER (in $CREDS or your shell)}"
: "${AMO_JWT_SECRET:?set AMO_JWT_SECRET (in $CREDS or your shell)}"

# Version: explicit (--version), else the ALMa version (pyproject) so the
# connector version always matches the release.
if [ -z "$VERSION" ]; then
  VERSION="$(grep -m1 -E '^version *= *"' "$ROOT/pyproject.toml" | sed -E 's/^version *= *"([^"]+)".*/\1/')"
fi
[ -n "$VERSION" ] || { echo "Could not determine version"; exit 1; }
TAG="v$VERSION"
XPI="$OUT/alma-connector-$VERSION.xpi"
echo "ALMa connector $VERSION"

# --- Build the signed artifact ---------------------------------------------
# Stage a clean copy so we set the version WITHOUT touching the working tree,
# and never ship dev/test/tooling files. Keys go to web-ext via WEB_EXT_* env
# (never on the command line).
#
# With --from-tag the stage comes from `git archive <tag>` instead of the
# working tree, so the signed artifact provably matches the tagged source
# even when the tree is dirty. scripts/release.sh always uses this form.
mkdir -p "$OUT"
STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT
if [ -n "$FROM_TAG" ]; then
  git rev-parse -q --verify "refs/tags/$FROM_TAG" >/dev/null \
    || { echo "Tag not found: $FROM_TAG"; exit 1; }
  git archive "$FROM_TAG" extension | tar -x -C "$STAGE" --strip-components=1
else
  cp -r "$EXT/." "$STAGE/"
fi
rm -rf "$STAGE/test" "$STAGE/hooks" "$STAGE/node_modules" \
       "$STAGE/release.sh" "$STAGE/README.md" "$STAGE/package.json" "$STAGE/package-lock.json"
node -e 'const fs=require("fs");const m=JSON.parse(fs.readFileSync(process.argv[1],"utf8"));m.version=process.argv[2];fs.writeFileSync(process.argv[1],JSON.stringify(m,null,2)+"\n")' "$STAGE/manifest.json" "$VERSION"

npx --yes "$WEB_EXT" lint --source-dir "$STAGE"
WEB_EXT_API_KEY="$AMO_JWT_ISSUER" WEB_EXT_API_SECRET="$AMO_JWT_SECRET" \
  npx --yes "$WEB_EXT" sign --source-dir "$STAGE" --channel=unlisted --artifacts-dir "$OUT"
cp "$(ls -t "$OUT"/*.xpi | head -1)" "$XPI"
echo "Signed -> $XPI"

if [ "$LOCAL_ONLY" -eq 1 ]; then
  echo
  echo "Local build only — nothing uploaded. Install via:"
  echo "  about:addons -> gear -> Install Add-on From File -> $XPI"
  exit 0
fi

# --- Publish (the one write, behind a confirmation) ------------------------
command -v gh >/dev/null || { echo "gh (GitHub CLI) is required to upload: https://cli.github.com"; exit 1; }
[ -e /dev/tty ] || { echo "No terminal for confirmation; artifact kept at $XPI."; exit 1; }

if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  ACTION="upload the .xpi to the existing release $TAG"
  EXISTS=1
else
  ACTION="create release + tag $TAG (writes a new tag to origin) and upload the .xpi"
  EXISTS=0
fi
echo
echo "Ready to PUBLISH:"
echo "  • $ACTION"
printf "Proceed? [y/N] "
read -r ans </dev/tty
case "$ans" in
  y|Y|yes|YES) ;;
  *) echo "Aborted — signed artifact kept at $XPI (nothing was pushed)."; exit 0 ;;
esac

if [ "$EXISTS" -eq 1 ]; then
  gh release upload "$TAG" "$XPI" --repo "$REPO" --clobber
else
  if [ -n "$TARGET" ]; then
    gh release create "$TAG" "$XPI" --repo "$REPO" --title "$TAG" --notes "ALMa $TAG" --target "$TARGET"
  else
    gh release create "$TAG" "$XPI" --repo "$REPO" --title "$TAG" --notes "ALMa $TAG"
  fi
fi
echo "Done — alma-connector-$VERSION.xpi is attached to release $TAG."
