#!/usr/bin/env bash
#
# Release / build the ALMa Firefox connector — LOCAL signing.
#
#   extension/release.sh            full release: build the signed .xpi, then
#                                   (after a confirmation) publish it to the
#                                   v<version> GitHub Release + push the
#                                   auto-update manifest on main.
#   extension/release.sh --local    build the signed .xpi only — no GitHub
#                                   upload, no git writes. Just an installable
#                                   artifact in dist/.
#
# Read-only/local git (switch to main, pull --ff-only) happens without
# prompting; every WRITE — creating the release/tag, pushing to main — is
# gated behind a y/N confirmation.
#
# Requirements:
#   - npx (Node) — pulls `web-ext` on demand
#   - a free AMO API key in ~/.config/alma/amo.env (chmod 600), as:
#       export AMO_JWT_ISSUER=...
#       export AMO_JWT_SECRET=...
#     (or already exported in your shell)
#   - for a full release only: gh (GitHub CLI), authenticated (`gh auth login`)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXT="$ROOT/extension"
OUT="$ROOT/dist"
REPO="costantinoai/alma-library-manager"
ADDON_ID="alma-connector@costantinoai.github.io"
CREDS="$HOME/.config/alma/amo.env"
IGNORE=(--ignore-files "test/**" "updates.json" "README.md" "release.sh" \
        "package.json" "package-lock.json" "node_modules/**")

LOCAL_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --local) LOCAL_ONLY=1 ;;
    -h|--help) sed -n '2,28p' "$0"; exit 0 ;;
    *) echo "Unknown option: $arg (try --local or --help)"; exit 2 ;;
  esac
done

cd "$ROOT"

# Credentials (kept outside the repo).
[ -f "$CREDS" ] && . "$CREDS"
: "${AMO_JWT_ISSUER:?set AMO_JWT_ISSUER (in $CREDS or your shell)}"
: "${AMO_JWT_SECRET:?set AMO_JWT_SECRET (in $CREDS or your shell)}"

if [ "$LOCAL_ONLY" -eq 1 ]; then
  # Sign whatever is checked out, at its current manifest version. No git.
  VERSION="$(node -e 'process.stdout.write(require(process.argv[1]).version)' "$EXT/manifest.json")"
else
  command -v gh >/dev/null || { echo "gh (GitHub CLI) is required for a full release: https://cli.github.com"; exit 1; }
  echo "Switching to an up-to-date main…"
  git switch main          # local; no remote write
  git pull --ff-only       # read-only fast-forward (allowed)
  [ -f "$EXT/manifest.json" ] || { echo "Connector not found on main — merge it to main first."; exit 1; }
  VERSION="$(grep -m1 -E '^version *= *"' "$ROOT/pyproject.toml" | sed -E 's/^version *= *"([^"]+)".*/\1/')"
  [ -n "$VERSION" ] || { echo "Could not read version from pyproject.toml"; exit 1; }
  # connector version == ALMa version
  node -e 'const fs=require("fs");const m=JSON.parse(fs.readFileSync(process.argv[1],"utf8"));m.version=process.argv[2];fs.writeFileSync(process.argv[1],JSON.stringify(m,null,2)+"\n")' "$EXT/manifest.json" "$VERSION"
fi

TAG="v$VERSION"
XPI="$OUT/alma-connector-$VERSION.xpi"
URL="https://github.com/$REPO/releases/download/$TAG/alma-connector-$VERSION.xpi"
echo "ALMa connector $VERSION"

# --- Build the signed artifact (local; no writes beyond dist/) -------------
# Keys go to web-ext via WEB_EXT_* env, never on the command line.
mkdir -p "$OUT"
npx --yes web-ext lint --source-dir "$EXT" "${IGNORE[@]}"
WEB_EXT_API_KEY="$AMO_JWT_ISSUER" WEB_EXT_API_SECRET="$AMO_JWT_SECRET" \
  npx --yes web-ext sign --source-dir "$EXT" "${IGNORE[@]}" \
  --channel=unlisted --artifacts-dir "$OUT"
cp "$(ls -t "$OUT"/*.xpi | head -1)" "$XPI"
echo "Signed -> $XPI"

if [ "$LOCAL_ONLY" -eq 1 ]; then
  echo
  echo "Local build only — nothing pushed. Install it via:"
  echo "  about:addons -> gear -> Install Add-on From File -> $XPI"
  exit 0
fi

# --- Publish (every write gated by a confirmation) -------------------------
if [ ! -t 0 ]; then
  echo "Refusing to publish without an interactive confirmation."
  echo "The signed artifact is at $XPI. Use --local for non-interactive signing."
  exit 1
fi

if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  ACTION="upload the .xpi to the existing release $TAG"
else
  ACTION="create release + tag $TAG (writes a new tag to origin) and upload the .xpi"
fi
echo
echo "Ready to PUBLISH:"
echo "  • $ACTION"
echo "  • commit + push extension/updates.json (+ manifest) to origin/main"
printf "Proceed? [y/N] "
read -r ans
case "$ans" in
  y|Y|yes|YES) ;;
  *) echo "Aborted — the signed artifact is kept at $XPI (nothing was pushed)."; exit 0 ;;
esac

if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  gh release upload "$TAG" "$XPI" --repo "$REPO" --clobber
else
  gh release create "$TAG" "$XPI" --repo "$REPO" --title "$TAG" --notes "ALMa $TAG"
fi
echo "Uploaded to release $TAG"

# Register the version for auto-update, then commit + push it on main.
node -e '
  const fs = require("fs");
  const [p, id, v, url] = process.argv.slice(1);
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const updates = (j.addons[id].updates || []).filter((e) => e.version !== v);
  updates.push({ version: v, update_link: url });
  j.addons[id].updates = updates;
  fs.writeFileSync(p, JSON.stringify(j, null, 2) + "\n");
' "$EXT/updates.json" "$ADDON_ID" "$VERSION" "$URL"

git add "$EXT/manifest.json" "$EXT/updates.json"
if git diff --cached --quiet; then
  echo "manifest/updates.json already current — nothing to push."
else
  git commit -m "chore(connector): release $VERSION"
  git push origin main
  echo "Pushed manifest + updates.json for $VERSION to main."
fi

echo "Done — alma-connector-$VERSION.xpi is on release $TAG; installed copies will auto-update."
