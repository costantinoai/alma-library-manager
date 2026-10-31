#!/usr/bin/env bash
#
# Deploy a released ALMa version to the local prod container.
#
#   scripts/deploy-prod.sh 0.22.0          # pull + replace + verify
#   scripts/deploy-prod.sh 0.22.0 --lite   # force the lite flavor
#   scripts/deploy-prod.sh stable          # release channel (Watchtower
#                                          # then keeps it current)
#
# The deployment itself is setup.sh, the one Compose path: it pulls first, keeps
# the settings in ~/alma-docker/.env (ALMA_HOME), carries over an older
# container's settings once, and fails unless ALMa turns healthy. This script
# adds what a release needs on top: a named version, and the check that it runs.
#
# Exits non-zero unless the container is healthy and, for a concrete version,
# reports that exact release. `stable` accepts whichever release the channel
# resolved to. Images before 0.21.1 report the API contract version instead, so
# they fail this check.
set -euo pipefail

VERSION="${1:-}"
shift || true
FLAVOR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --gpu) FLAVOR="gpu" ;;
    --lite) FLAVOR="lite" ;;
    --cpu) FLAVOR="cpu" ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
  shift
done
[[ "$VERSION" =~ ^([0-9]+\.[0-9]+\.[0-9]+|stable)$ ]] || { echo "Usage: scripts/deploy-prod.sh X.Y.Z|stable [--gpu|--lite|--cpu]"; exit 2; }

# ALMA_FLAVOR empty keeps the saved flavor; setup.sh autodetects only without one.
ALMA_VERSION="$VERSION" ALMA_FLAVOR="$FLAVOR" bash "$(dirname "$0")/../setup.sh"

ALMA_HOME="${ALMA_HOME:-$HOME/alma-docker}"
body="$(cd "$ALMA_HOME" && docker compose exec -T alma curl -fsS -m 5 http://localhost:8000/api/v1/health)"
echo "[deploy] health: $body"
if [ "$VERSION" = "stable" ]; then
  echo "[deploy] OK — stable channel; health reports the concrete release above"
elif echo "$body" | grep -qF "\"version\":\"$VERSION\""; then
  echo "[deploy] OK — running $VERSION"
else
  echo "[deploy] FAILED — asked for $VERSION but the running release is: $body" >&2
  exit 1
fi
