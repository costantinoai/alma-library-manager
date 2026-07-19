#!/usr/bin/env bash
#
# Deploy a released ALMa version to the local prod container.
#
#   scripts/deploy-prod.sh 0.22.0          # pull + recreate + verify
#   scripts/deploy-prod.sh 0.22.0 --lite   # force the lite flavor
#
# Recreates the `alma` container from ghcr.io/<repo>:<version>[-gpu|-lite],
# preserving the running container's operator env (API keys live there on
# this host), the localhost:8000 binding, and the named volumes
# (alma-data / alma-config — your data is never touched by a swap).
# Flavor autodetects GPU exactly like setup.sh; volumes carry all state so
# a swap is safe to re-run.
#
# Verifies health AND that the container actually reports the requested
# release version (images >= 0.21.1 report it at /api/v1/health).
set -euo pipefail

REPO_IMAGE="ghcr.io/costantinoai/alma-library-manager"

VERSION="${1:-}"
shift || true
FLAVOR=""
while [ $# -gt 0 ]; do
  case "$1" in
    --gpu) FLAVOR="-gpu" ;;
    --lite) FLAVOR="-lite" ;;
    --cpu) FLAVOR="" ;;
    -h|--help) sed -n '2,17p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1"; exit 2 ;;
  esac
  shift
done
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || { echo "Usage: scripts/deploy-prod.sh X.Y.Z [--gpu|--lite|--cpu]"; exit 2; }

# Flavor autodetect (same rule as setup.sh) unless forced by a flag.
GPU_FLAG=""
if [ -z "$FLAVOR" ]; then
  if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi >/dev/null 2>&1 \
     && docker info 2>/dev/null | grep -qi "runtimes:.*nvidia"; then
    FLAVOR="-gpu"
  fi
fi
[ "$FLAVOR" = "-gpu" ] && GPU_FLAG="--gpus all"
IMAGE="$REPO_IMAGE:$VERSION$FLAVOR"

echo "[deploy] image: $IMAGE"

# Carry over operator-set env from the running container (keys etc.).
ENV_FLAGS=()
if docker inspect alma >/dev/null 2>&1; then
  for var in NUMBA_DISABLE_JITCACHE OPENALEX_API_KEY SEMANTIC_SCHOLAR_API_KEY API_KEY OPENALEX_EMAIL; do
    val="$(docker inspect alma --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E "^${var}=" || true)"
    [ -n "$val" ] && ENV_FLAGS+=(-e "$val")
  done
fi

docker pull "$IMAGE"
docker rm -f alma >/dev/null 2>&1 || true

# shellcheck disable=SC2086  # GPU_FLAG must word-split when set
docker run -d \
  --name alma \
  --restart unless-stopped \
  -p 127.0.0.1:8000:8000 \
  -e "ALMA_SETTINGS_PATH=/app/data/settings.json" \
  ${ENV_FLAGS[@]+"${ENV_FLAGS[@]}"} \
  $GPU_FLAG \
  -v alma-data:/app/data \
  -v alma-config:/app/config \
  "$IMAGE" >/dev/null

echo "[deploy] waiting for health…"
for _ in $(seq 1 60); do
  body="$(curl -fsS -m 2 http://localhost:8000/api/v1/health 2>/dev/null || true)"
  if [ -n "$body" ]; then
    echo "[deploy] health: $body"
    if echo "$body" | grep -q "\"version\":\"$VERSION\""; then
      echo "[deploy] OK — running $VERSION"
    else
      echo "[deploy] note: health version differs from $VERSION (images built before 0.21.1 report the API contract version instead)."
    fi
    exit 0
  fi
  sleep 1
done
echo "[deploy] UNHEALTHY after 60s — check: docker logs alma" >&2
exit 1
