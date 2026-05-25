#!/usr/bin/env bash
#
# Release the ALMa Firefox connector — LOCAL signing, GitHub-Releases
# distribution, self-hosted auto-update. One command:
#
#   1. stamp the connector version = ALMa version (pyproject.toml)
#   2. lint + sign as an UNLISTED add-on via AMO (automated, no review)
#   3. upload the signed alma-connector-<version>.xpi to the GitHub Release
#      for the matching v<version> tag (creating the release if needed)
#   4. record the version -> asset URL in extension/updates.json
#      (so installed copies auto-update via the manifest update_url)
#
# Then commit + push extension/manifest.json + extension/updates.json on
# `main` (the script prints the exact commands — it does NOT push for you).
#
# Requirements:
#   - npx (Node) — pulls `web-ext` on demand
#   - gh (GitHub CLI), authenticated: `gh auth login`
#   - a free AMO API key (addons.mozilla.org -> Developer Hub -> Manage API
#     Keys) exported in your shell — keep these OUT of the repo:
#       export AMO_JWT_ISSUER=...   AMO_JWT_SECRET=...
#
# Usage:  extension/release.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
EXT="$ROOT/extension"
OUT="$ROOT/dist"
REPO="costantinoai/alma-library-manager"
ADDON_ID="alma-connector@costantinoai.github.io"
IGNORE=(--ignore-files "test/**" "updates.json" "README.md" "release.sh" \
        "package.json" "package-lock.json" "node_modules/**")

: "${AMO_JWT_ISSUER:?export AMO_JWT_ISSUER (AMO API key issuer)}"
: "${AMO_JWT_SECRET:?export AMO_JWT_SECRET (AMO API key secret)}"
command -v gh >/dev/null || { echo "gh (GitHub CLI) is required: https://cli.github.com"; exit 1; }

VERSION="$(grep -m1 -E '^version *= *"' "$ROOT/pyproject.toml" | sed -E 's/^version *= *"([^"]+)".*/\1/')"
[ -n "$VERSION" ] || { echo "Could not read version from pyproject.toml"; exit 1; }
TAG="v$VERSION"
XPI="$OUT/alma-connector-$VERSION.xpi"
URL="https://github.com/$REPO/releases/download/$TAG/alma-connector-$VERSION.xpi"

BRANCH="$(git -C "$ROOT" rev-parse --abbrev-ref HEAD)"
echo "ALMa connector release $TAG  (branch: $BRANCH)"
[ "$BRANCH" = "main" ] || echo "  ! Not on main — updates.json must land on main for auto-update to work."

# 1. connector version == ALMa version
node -e 'const fs=require("fs");const m=JSON.parse(fs.readFileSync(process.argv[1],"utf8"));m.version=process.argv[2];fs.writeFileSync(process.argv[1],JSON.stringify(m,null,2)+"\n")' "$EXT/manifest.json" "$VERSION"

# 2. lint + sign (unlisted)
# Pass the API key/secret to web-ext via its WEB_EXT_* env vars rather than
# --api-key/--api-secret, so they never appear in the process command line
# (ps) or shell history.
mkdir -p "$OUT"
npx --yes web-ext lint --source-dir "$EXT" "${IGNORE[@]}"
WEB_EXT_API_KEY="$AMO_JWT_ISSUER" WEB_EXT_API_SECRET="$AMO_JWT_SECRET" \
  npx --yes web-ext sign --source-dir "$EXT" "${IGNORE[@]}" \
  --channel=unlisted --artifacts-dir "$OUT"
cp "$(ls -t "$OUT"/*.xpi | head -1)" "$XPI"
echo "Signed -> $XPI"

# 3. attach to the GitHub Release (create it if it doesn't exist yet)
if gh release view "$TAG" --repo "$REPO" >/dev/null 2>&1; then
  gh release upload "$TAG" "$XPI" --repo "$REPO" --clobber
else
  gh release create "$TAG" "$XPI" --repo "$REPO" --title "$TAG" --notes "ALMa $TAG"
fi
echo "Uploaded to release $TAG"

# 4. register the version for auto-update
node -e '
  const fs = require("fs");
  const [p, id, v, url] = process.argv.slice(1);
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const updates = (j.addons[id].updates || []).filter((e) => e.version !== v);
  updates.push({ version: v, update_link: url });
  j.addons[id].updates = updates;
  fs.writeFileSync(p, JSON.stringify(j, null, 2) + "\n");
' "$EXT/updates.json" "$ADDON_ID" "$VERSION" "$URL"
echo "updates.json -> $VERSION"

cat <<EOF

Done. Now commit the version + auto-update manifest on main so installed
copies pick up the new build:

  git add extension/manifest.json extension/updates.json
  git commit -m "chore(connector): release $VERSION"
  git push origin main
EOF
