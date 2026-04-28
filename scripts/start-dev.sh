#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Auto-activate a virtual environment if not already in one
if [[ -z "${VIRTUAL_ENV:-}" && -z "${CONDA_PREFIX:-}" ]]; then
  for venv in "${ROOT_DIR}/.venv-scholarbot-ai" "${ROOT_DIR}/.venv" "${ROOT_DIR}/venv"; do
    if [[ -f "${venv}/bin/activate" ]]; then
      echo "[dev] Activating virtualenv: ${venv}"
      # shellcheck disable=SC1091
      source "${venv}/bin/activate"
      break
    fi
  done
fi

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
FRONTEND_URL="${FRONTEND_URL:-http://localhost:${FRONTEND_PORT}}"
BACKEND_PYTHON="${BACKEND_PYTHON:-}"
BACKEND_RELOAD="${BACKEND_RELOAD:-false}"

# uvicorn process count. Stays at 1: APScheduler's BackgroundScheduler
# (jobs / deep refresh / feed sweeps) and the OpenAlex client live
# in-process and are not multi-process safe — multiple uvicorn workers
# would each run their own scheduler and double-fire jobs. Real
# concurrency comes from the in-process thread pool tuned below.
BACKEND_WORKERS="${BACKEND_WORKERS:-1}"

# In-process concurrency cap for `_deep_refresh_all_impl`'s
# ThreadPoolExecutor. 4 is the polite-pool sweet spot for OpenAlex
# (≈10 req/s ceiling) with comfortable SQLite WAL headroom. Bump to
# 6–8 if you've moved off the polite pool and have spare CPU; the
# code clamps to [1, 16] and gracefully falls back on bad values.
ALMA_DEEP_REFRESH_WORKERS="${ALMA_DEEP_REFRESH_WORKERS:-4}"
export ALMA_DEEP_REFRESH_WORKERS

if [[ -z "${BACKEND_PYTHON}" ]]; then
  if [[ -x "${ROOT_DIR}/.venv-scholarbot-ai/bin/python" ]]; then
    BACKEND_PYTHON="${ROOT_DIR}/.venv-scholarbot-ai/bin/python"
  else
    BACKEND_PYTHON="python"
  fi
fi

# Kill any stale processes on our ports before starting.
#
# uvicorn catches SIGTERM and runs a graceful shutdown that can take
# 2-4s — longer than the bind retry window of the next start, so a
# plain `kill` (SIGTERM) followed by a short sleep often leaves the
# port held when we try to bind it again. Two-phase kill: SIGTERM
# first, poll for ~2s, then SIGKILL anything still holding the port.
kill_port() {
  local port="$1" pids leftover i
  pids="$(lsof -ti :"${port}" 2>/dev/null || true)"
  if [[ -z "${pids}" ]]; then
    return 0
  fi
  echo "[dev] Killing stale process(es) on port ${port}: ${pids}"
  # shellcheck disable=SC2086
  kill ${pids} 2>/dev/null || true
  for i in 1 2 3 4; do
    sleep 0.5
    leftover="$(lsof -ti :"${port}" 2>/dev/null || true)"
    if [[ -z "${leftover}" ]]; then
      return 0
    fi
  done
  echo "[dev] Port ${port} still held by ${leftover}, sending SIGKILL"
  # shellcheck disable=SC2086
  kill -9 ${leftover} 2>/dev/null || true
  sleep 0.5
}

kill_port "${BACKEND_PORT}"
kill_port "${FRONTEND_PORT}"

BACK_PID=""
FRONT_PID=""

cleanup() {
  local exit_code=$?
  trap - EXIT INT TERM

  echo "[dev] Shutting down..."

  # Kill process groups so child processes (node, python) also die
  if [[ -n "${BACK_PID}" ]] && kill -0 "${BACK_PID}" 2>/dev/null; then
    kill -- -"${BACK_PID}" 2>/dev/null || kill "${BACK_PID}" 2>/dev/null || true
  fi
  if [[ -n "${FRONT_PID}" ]] && kill -0 "${FRONT_PID}" 2>/dev/null; then
    kill -- -"${FRONT_PID}" 2>/dev/null || kill "${FRONT_PID}" 2>/dev/null || true
  fi

  wait "${BACK_PID}" "${FRONT_PID}" 2>/dev/null || true

  # Final sweep: if anything is still lingering on our ports, force-kill it
  local remaining
  remaining="$(lsof -ti :"${BACKEND_PORT}" -ti :"${FRONTEND_PORT}" 2>/dev/null || true)"
  if [[ -n "${remaining}" ]]; then
    # shellcheck disable=SC2086
    kill -9 ${remaining} 2>/dev/null || true
  fi

  exit "${exit_code}"
}

open_browser() {
  local url="$1"
  if command -v xdg-open >/dev/null 2>&1; then
    xdg-open "${url}" >/dev/null 2>&1 || true
  elif command -v open >/dev/null 2>&1; then
    open "${url}" >/dev/null 2>&1 || true
  elif command -v sensible-browser >/dev/null 2>&1; then
    sensible-browser "${url}" >/dev/null 2>&1 || true
  else
    echo "[dev] Could not auto-open browser. Open this URL manually: ${url}"
  fi
}

trap cleanup EXIT INT TERM

echo "[dev] Starting backend on port ${BACKEND_PORT}..."
echo "[dev] Backend Python: ${BACKEND_PYTHON}"
echo "[dev] uvicorn workers=${BACKEND_WORKERS} · deep-refresh threads=${ALMA_DEEP_REFRESH_WORKERS}"
(
  cd "${ROOT_DIR}"
  export PYTHONPATH="${ROOT_DIR}/src${PYTHONPATH:+:${PYTHONPATH}}"
  UVICORN_ARGS=(--host 0.0.0.0 --port "${BACKEND_PORT}")
  if [[ "${BACKEND_RELOAD}" == "true" ]]; then
    UVICORN_ARGS+=(--reload --reload-dir "${ROOT_DIR}/src")
  else
    UVICORN_ARGS+=(--workers "${BACKEND_WORKERS}")
  fi
  exec "${BACKEND_PYTHON}" -m uvicorn alma.api.app:app "${UVICORN_ARGS[@]}"
) > >(sed -u 's/^/[backend] /') 2> >(sed -u 's/^/[backend] /' >&2) &
BACK_PID=$!

echo "[dev] Starting frontend on port ${FRONTEND_PORT}..."
(
  cd "${ROOT_DIR}/frontend"
  exec npx vite --port "${FRONTEND_PORT}" --strictPort
) > >(sed -u 's/^/[frontend] /') 2> >(sed -u 's/^/[frontend] /' >&2) &
FRONT_PID=$!

# Wait for Vite to be ready before opening browser
echo "[dev] Waiting for frontend to be ready..."
for i in $(seq 1 30); do
  if curl -s -o /dev/null -w "" "http://localhost:${FRONTEND_PORT}/" 2>/dev/null; then
    echo "[dev] Frontend ready after ~${i}s"
    break
  fi
  if ! kill -0 "${FRONT_PID}" 2>/dev/null; then
    echo "[dev] Frontend process died. Check output above for errors."
    exit 1
  fi
  sleep 1
done

echo "[dev] Opening ${FRONTEND_URL} in browser..."
open_browser "${FRONTEND_URL}"

echo "[dev] Services are running. Press Ctrl+C to stop both."

set +e
wait -n "${BACK_PID}" "${FRONT_PID}"
status=$?
set -e

echo "[dev] One service exited (status ${status}). Stopping the other..."
exit "${status}"
