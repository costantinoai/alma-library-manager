#!/usr/bin/env bash
# =============================================================================
# ALMa — self-hosted installer (Linux / macOS)
# =============================================================================
# A thin front end to docker-compose.yml. It writes the Compose files and a
# private .env into ~/alma-docker (ALMA_HOME to choose another folder), picks
# the image for your hardware once, then runs `docker compose pull` and `up`.
# The running container is only touched after the pull has succeeded. The
# required OpenAlex API key is added after boot via Settings → Connections
# (kept in the volume's secret store, never in `docker inspect`).
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.sh | bash
#
# Re-run the same command to update. Settings are read from the environment and
# saved in .env, so a later run keeps them:
#   ALMA_ALLOWED_ORIGINS  addresses you open ALMa at, beyond localhost and IPs
#   BIND_ADDR             host address for the port (default 127.0.0.1)
#   ALMA_IMAGE_TAG        exact image tag; or ALMA_VERSION + ALMA_FLAVOR (cpu|gpu|lite)
#   OPENALEX_EMAIL        optional contact email
# =============================================================================
set -euo pipefail
umask 077  # .env can hold API keys

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

IMAGE_REPO="ghcr.io/costantinoai/alma-library-manager"
RAW_URL="https://raw.githubusercontent.com/costantinoai/alma-library-manager/main"
ALMA_HOME="${ALMA_HOME:-$HOME/alma-docker}"
ENV_FILE="$ALMA_HOME/.env"

# Compose runs from the folder, exactly as you would by hand: that is where it
# finds the files and .env.
compose() { (cd "$ALMA_HOME" && docker compose "$@"); }

env_get() {  # NAME: the value saved in .env, or nothing
  sed -n "s/^$1=//p" "$ENV_FILE" | tail -n 1 | sed "s/^'//; s/'\$//"
}
env_set() {  # NAME VALUE. Single quotes keep Compose from reading $ in a value.
  { grep -v "^$1=" "$ENV_FILE" || true; printf "%s='%s'\n" "$1" "$2"; } > "$ENV_FILE.new"
  mv "$ENV_FILE.new" "$ENV_FILE"
}

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
if ! docker compose version >/dev/null 2>&1; then
  err "Docker Compose v2 not found ('docker compose')."
  echo "  Install it: https://docs.docker.com/compose/install/"
  exit 1
fi
ok "Docker is running ($(docker --version))"
echo

# ---- 2. Write the deployment folder ----------------------------------------
step 2 5 "Preparing $ALMA_HOME..."
mkdir -p "$ALMA_HOME"
touch "$ENV_FILE"
for file in docker-compose.yml docker-compose.gpu.yml; do
  # Run from a checkout, use its files. Piped from curl, BASH_SOURCE is empty.
  source_dir="$(dirname "${BASH_SOURCE[0]:-}")"
  if [ -f "${BASH_SOURCE[0]:-}" ] && [ -f "$source_dir/$file" ]; then
    cp "$source_dir/$file" "$ALMA_HOME/$file.new"
  else
    curl -fsSL "$RAW_URL/$file" -o "$ALMA_HOME/$file.new"
  fi
  mv "$ALMA_HOME/$file.new" "$ALMA_HOME/$file"
done

# An install that Compose does not own here: an older `docker run`, a
# deploy-prod.sh container, or a checkout under another project name. Its
# settings move into .env; the container itself goes only after the pull.
CONFIG="$(compose config)"
CONTAINER="$(printf '%s\n' "$CONFIG" | sed -n 's/^ *container_name: //p' | head -n 1)"
PROJECT="$(printf '%s\n' "$CONFIG" | sed -n 's/^name: //p' | head -n 1)"
OLDER=""
if docker inspect "$CONTAINER" >/dev/null 2>&1; then
  owner="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' "$CONTAINER")"
  [ "$owner" = "$PROJECT" ] || OLDER="$CONTAINER"
fi

declare -a CARRIED=()  # NAME=value lines from the older install, not yet saved
if [ -n "$OLDER" ]; then
  warn "Found an older '$OLDER' container. Its settings will carry over."
  old_image="$(docker inspect -f '{{.Config.Image}}' "$OLDER")"
  # Only what was set on the container, not what the image itself defines.
  while IFS= read -r line; do
    [ -n "$line" ] && CARRIED+=("$line")
  done < <(comm -23 \
    <(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$OLDER" | sort) \
    <(docker image inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$old_image" | sort))
  old_bind="$(docker inspect -f '{{range (index .HostConfig.PortBindings "8000/tcp")}}{{.HostIp}}{{end}}' "$OLDER")"
  if [ -n "$old_bind" ] && [ "$old_bind" != "127.0.0.1" ]; then
    CARRIED+=("BIND_ADDR=$old_bind")
  fi
  # Stay on the channel: a `latest` database can be too new for `stable`.
  case "$old_image" in
    "$IMAGE_REPO":*) CARRIED+=("ALMA_IMAGE_TAG=${old_image##*:}") ;;
  esac
fi
carried_value() {  # NAME: the value from the older install, or nothing
  [ "${#CARRIED[@]}" -gt 0 ] || return 0
  printf '%s\n' "${CARRIED[@]}" | sed -n "s/^$1=//p" | tail -n 1
}
ok "Deployment folder ready"
echo

# ---- 3. Pick the image ------------------------------------------------------
# A saved choice stands: a re-run must not move an install to another flavor.
step 3 5 "Choosing the right image for your hardware..."
detect_flavor() {
  if command -v nvidia-smi >/dev/null 2>&1 && nvidia-smi >/dev/null 2>&1; then
    if docker info 2>/dev/null | grep -qi "runtimes:.*nvidia"; then
      echo gpu
      return
    fi
    warn "NVIDIA GPU detected but the NVIDIA Container Toolkit isn't registered with Docker." >&2
    warn "Install it to enable GPU acceleration: https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/" >&2
  elif [ "$(uname -m)" = "armv7l" ] || [ "$(uname -m)" = "armv6l" ]; then
    echo lite
    return
  fi
  echo cpu
}

TAG="${ALMA_IMAGE_TAG:-}"
if [ -z "$TAG" ] && [ -z "${ALMA_VERSION:-}" ]; then
  TAG="$(env_get ALMA_IMAGE_TAG)"
  [ -n "$TAG" ] || TAG="$(carried_value ALMA_IMAGE_TAG)"
fi
if [ -z "$TAG" ]; then
  flavor="${ALMA_FLAVOR:-$(detect_flavor)}"
  TAG="${ALMA_VERSION:-stable}"
  case "$flavor" in
    gpu)  TAG="$TAG-gpu" ;;
    lite) TAG="$TAG-lite" ;;
  esac
fi

STACK="docker-compose.yml"
case "$TAG" in *gpu*) STACK="$STACK:docker-compose.gpu.yml" ;; esac
[ -f "$ALMA_HOME/docker-compose.override.yml" ] && STACK="$STACK:docker-compose.override.yml"

# The chosen values go to Compose through the environment, and into .env only
# once the pull has worked, so a bad tag does not stick.
export ALMA_IMAGE_TAG="$TAG" COMPOSE_FILE="$STACK"
ok "Selected: ${BOLD}${IMAGE_REPO}:${TAG}${NC}"
echo

# ---- 4. Pull while the old container keeps serving --------------------------
step 4 5 "Pulling the image..."
if ! compose pull; then
  err "Pull failed. Nothing was stopped; the running ALMa is unchanged."
  exit 1
fi

# OpenAlex requires an API key on every request (since 2026-02-13). We do NOT
# prompt for it here: the key is set after boot via Settings → Connections,
# which persists it to the 0600 secret store inside the alma-data volume — so
# it never lands in `docker inspect` or shell history.
if [ "${#CARRIED[@]}" -gt 0 ]; then
  for line in "${CARRIED[@]}"; do
    name="${line%%=*}"
    case "$name" in
      ALMA_IMAGE_TAG) ;;
      *) [ -n "$(env_get "$name")" ] || env_set "$name" "${line#*=}" ;;
    esac
  done
fi
env_set ALMA_IMAGE_TAG "$TAG"
env_set COMPOSE_FILE "$STACK"
for name in ALMA_ALLOWED_ORIGINS BIND_ADDR OPENALEX_EMAIL; do
  if [ -n "${!name:-}" ]; then env_set "$name" "${!name}"; fi
done

BIND_NOW="$(env_get BIND_ADDR)"
if [ -n "$BIND_NOW" ] && [ "$BIND_NOW" != "127.0.0.1" ]; then
  warn "Binding to ${BIND_NOW} — ALMa will be reachable from other devices."
  warn "ALMa has no login; only do this on a trusted private network."
fi

# ---- 5. Start and wait for health ------------------------------------------
step 5 5 "Starting ALMa..."
if [ -n "$OLDER" ]; then
  docker stop "$OLDER" >/dev/null
  docker rm "$OLDER" >/dev/null
fi

# A first boot may migrate the database, which is why the wait is long. A
# health check that keeps failing ends it sooner.
if ! compose up -d --wait --wait-timeout 300; then
  err "ALMa did not become healthy. UNHEALTHY — check: docker logs $CONTAINER"
  compose exec -T alma curl -sS -m 5 http://localhost:8000/api/v1/health >&2 || true
  echo >&2
  exit 1
fi
ok "ALMa is up."
echo

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
printf "${GREEN}${BOLD}Setup complete.${NC}\n"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo
echo "  Open ALMa:    http://localhost:8000"
echo
printf "  ${BOLD}Next step (required):${NC} add your OpenAlex API key\n"
echo "    Settings → Connections → OpenAlex, then 'Save connection settings'."
echo "    Free key (~30s): https://openalex.org/settings/api"
echo "    A Semantic Scholar key (Settings → Connections) is recommended too."
echo "    Keys are stored in the alma-data volume's 0600 secret store."
echo
echo "  Settings:     $ENV_FILE"
echo "  Logs:         docker logs -f alma"
echo "  Stop:         docker stop alma"
echo "  Start:        docker start alma"
echo "  Update:       re-run this installer"
echo "  Uninstall:    cd $ALMA_HOME && docker compose down && docker volume rm alma-data alma-config"
echo
echo "  Your library lives in the 'alma-data' Docker volume — it survives"
echo "  container removal and image upgrades. Only 'docker volume rm' wipes it."
echo
