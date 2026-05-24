#!/usr/bin/env bash
# =============================================================================
# ALMa — self-hosted installer (Linux / macOS)
# =============================================================================
# Pulls the prebuilt image from GitHub Container Registry, prompts for your
# OpenAlex email, picks the right image variant for your hardware
# (GPU / CPU / lite), and starts ALMa with named Docker volumes so your
# library survives upgrades.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.sh | bash
#
# Re-run the same command to update — the script detects an existing install
# and pulls the latest image before restarting.
# =============================================================================
set -euo pipefail

# ---- pretty output ---------------------------------------------------------
if [ -t 1 ]; then
  RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; BLUE=$'\033[0;34m'
  YELLOW=$'\033[1;33m'; BOLD=$'\033[1m'; NC=$'\033[0m'
else
  RED=""; GREEN=""; BLUE=""; YELLOW=""; BOLD=""; NC=""
fi

step()    { printf "${BLUE}[%s/%s]${NC} %s\n" "$1" "$2" "$3"; }
ok()      { printf "${GREEN}✓${NC} %s\n" "$1"; }
warn()    { printf "${YELLOW}⚠${NC} %s\n" "$1"; }
err()     { printf "${RED}✗${NC} %s\n" "$1" >&2; }
banner()  {
  printf "${BLUE}${BOLD}\n"
  echo "  ╔═══════════════════════════════════════════╗"
  echo "  ║              ALMa  Installer              ║"
  echo "  ║   Another Library Manager — self-hosted   ║"
  echo "  ╚═══════════════════════════════════════════╝"
  printf "${NC}\n"
}

banner

# ---- 1. Docker check -------------------------------------------------------
step 1 5 "Checking Docker installation..."
if ! command -v docker >/dev/null 2>&1; then
  err "Docker not found on PATH."
  echo
  echo "Install Docker first:"
  echo "  Linux:        https://docs.docker.com/engine/install/"
  echo "  macOS:        https://docs.docker.com/desktop/install/mac-install/"
  echo "  Raspberry Pi: https://docs.docker.com/engine/install/debian/"
  echo
  echo "Then re-run this installer."
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  err "Docker is installed but the daemon isn't responding."
  echo "  Start Docker (open Docker Desktop, or 'sudo systemctl start docker') and re-run."
  exit 1
fi
ok "Docker is running ($(docker --version))"
echo

# ---- 2. Pick image variant -------------------------------------------------
step 2 5 "Choosing the right image for your hardware..."
IMAGE_BASE="ghcr.io/costantinoai/alma-library-manager"
GPU_FLAG=""
VARIANT_LABEL=""

# GPU detection — NVIDIA host with the Container Toolkit
if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi >/dev/null 2>&1; then
  if docker info 2>/dev/null | grep -qi "runtimes:.*nvidia"; then
    TAG="latest-gpu"
    GPU_FLAG="--gpus all"
    VARIANT_LABEL="GPU (CUDA, ~3.2 GB image)"
  else
    warn "NVIDIA GPU detected but the NVIDIA Container Toolkit isn't registered with Docker."
    warn "Install it to enable GPU acceleration: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/"
    TAG="latest"
    VARIANT_LABEL="CPU (~1.4 GB image)"
  fi
elif [ "$(uname -m)" = "armv7l" ] || [ "$(uname -m)" = "armv6l" ]; then
  TAG="latest-lite"
  VARIANT_LABEL="lite — no torch (~1.2 GB image, for Raspberry Pi)"
else
  TAG="latest"
  VARIANT_LABEL="CPU (~1.4 GB image)"
fi

# Honour ALMA_IMAGE_TAG override
if [ -n "${ALMA_IMAGE_TAG:-}" ]; then
  TAG="$ALMA_IMAGE_TAG"
  VARIANT_LABEL="custom (ALMA_IMAGE_TAG=$TAG)"
  GPU_FLAG=""
  [[ "$TAG" == *gpu* ]] && GPU_FLAG="--gpus all"
fi

IMAGE="${IMAGE_BASE}:${TAG}"
ok "Selected: ${BOLD}${IMAGE}${NC} — ${VARIANT_LABEL}"
echo

# ---- 3. OpenAlex email -----------------------------------------------------
step 3 5 "Configuring OpenAlex polite-pool email..."
OPENALEX_EMAIL="${OPENALEX_EMAIL:-}"
if [ -z "$OPENALEX_EMAIL" ]; then
  echo "ALMa identifies itself to OpenAlex with your email address — this is"
  echo "free, requires no signup, and avoids anonymous rate limits."
  echo
  if [ -t 0 ]; then
    while true; do
      read -r -p "Your email: " OPENALEX_EMAIL
      [ -n "$OPENALEX_EMAIL" ] && break
      err "Email cannot be empty."
    done
  else
    # Non-interactive (piped from curl): fall back to a placeholder + warn
    OPENALEX_EMAIL="anonymous@example.com"
    warn "Running non-interactively — set OPENALEX_EMAIL=you@example.com in the env"
    warn "or update it later from Settings → External APIs in the UI."
  fi
fi
ok "OpenAlex email: $OPENALEX_EMAIL"
echo

# ---- 4. Stop existing container (if any) -----------------------------------
step 4 5 "Pulling image and (re)starting container..."
if docker ps -a --format '{{.Names}}' | grep -qx alma; then
  warn "Existing 'alma' container found — stopping and removing it (your data in the named volumes is preserved)."
  docker rm -f alma >/dev/null
fi

docker pull "$IMAGE"

# ---- 5. Run ---------------------------------------------------------------
# shellcheck disable=SC2086  # we want GPU_FLAG to word-split when set
# Bind to localhost only by default — ALMa has no auth in single-user mode.
# Set BIND_ADDR=0.0.0.0 to reach it from other devices (e.g. a headless Pi);
# do so only on a trusted network. See the README "Exposing on your network".
BIND_ADDR="${BIND_ADDR:-127.0.0.1}"
if [ "$BIND_ADDR" != "127.0.0.1" ]; then
  warn "Binding to ${BIND_ADDR} — ALMa will be reachable from other devices."
  warn "ALMa has no auth by default; only do this on a trusted network."
fi

docker run -d \
  --name alma \
  --restart unless-stopped \
  -p "${BIND_ADDR}:8000:8000" \
  -e "OPENALEX_EMAIL=$OPENALEX_EMAIL" \
  -e "ALMA_SETTINGS_PATH=/app/data/settings.json" \
  -v alma-data:/app/data \
  -v alma-config:/app/config \
  $GPU_FLAG \
  "$IMAGE" >/dev/null

echo
step 5 5 "Verifying..."
# Wait up to 30s for the health endpoint
for _ in $(seq 1 30); do
  if curl -fsS "http://localhost:8000/api/v1/health" >/dev/null 2>&1; then
    HEALTHY=1; break
  fi
  sleep 1
done

if [ "${HEALTHY:-}" = "1" ]; then
  ok "ALMa is up."
else
  warn "Container started but the health endpoint didn't respond within 30s — check 'docker logs alma'."
fi
echo

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "${GREEN}${BOLD}Setup complete.${NC}\n"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo
echo "  Open ALMa:    http://localhost:8000"
echo
echo "  Logs:         docker logs -f alma"
echo "  Stop:         docker stop alma"
echo "  Start:        docker start alma"
echo "  Update:       re-run this installer"
echo "  Uninstall:    docker rm -f alma && docker volume rm alma-data alma-config"
echo
echo "  Your library lives in the 'alma-data' Docker volume — it survives"
echo "  container removal and image upgrades. Only 'docker volume rm' wipes it."
echo
