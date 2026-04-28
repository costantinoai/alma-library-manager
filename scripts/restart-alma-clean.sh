#!/usr/bin/env bash
set -Eeuo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT_DIR}/logs"
mkdir -p "${LOG_DIR}"

BACKEND_PORT="${BACKEND_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-5173}"
BACKEND_HOST="${BACKEND_HOST:-0.0.0.0}"
FRONTEND_HOST="${FRONTEND_HOST:-0.0.0.0}"

BACKEND_PID_FILE="${LOG_DIR}/alma.backend.pid"
FRONTEND_PID_FILE="${LOG_DIR}/alma.frontend.pid"
BACKEND_LOG="${LOG_DIR}/backend.dev.log"
FRONTEND_LOG="${LOG_DIR}/frontend.dev.log"

BACKEND_PYTHON="${BACKEND_PYTHON:-${ROOT_DIR}/.venv-scholarbot-ai/bin/python}"
if [[ ! -x "${BACKEND_PYTHON}" ]]; then
  BACKEND_PYTHON="python"
fi

if command -v nproc >/dev/null 2>&1; then
  CPU_CORES="$(nproc)"
else
  CPU_CORES="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)"
fi
if ! [[ "${CPU_CORES}" =~ ^[0-9]+$ ]]; then
  CPU_CORES=4
fi

THREADS_PER_CORE="$(lscpu 2>/dev/null | awk -F: '/Thread\(s\) per core/{gsub(/ /, "", $2); print $2; exit}')"
if ! [[ "${THREADS_PER_CORE:-}" =~ ^[0-9]+$ ]] || (( THREADS_PER_CORE < 1 )); then
  THREADS_PER_CORE=2
fi

PHYSICAL_CORES=$((CPU_CORES / THREADS_PER_CORE))
if (( PHYSICAL_CORES < 1 )); then
  PHYSICAL_CORES=1
fi

# Keep headroom for operation subprocesses and CPU-heavy background tasks.
DEFAULT_RESERVED_CORES=4
if (( PHYSICAL_CORES <= 4 )); then
  DEFAULT_RESERVED_CORES=2
fi
RESERVED_CORES="${ALMA_RESERVED_CORES:-${DEFAULT_RESERVED_CORES}}"
if ! [[ "${RESERVED_CORES}" =~ ^[0-9]+$ ]]; then
  RESERVED_CORES="${DEFAULT_RESERVED_CORES}"
fi

CALC_WORKERS=1
DEFAULT_MAX_WORKERS=1
MAX_WORKERS="${ALMA_MAX_BACKEND_WORKERS:-${DEFAULT_MAX_WORKERS}}"
if ! [[ "${MAX_WORKERS}" =~ ^[0-9]+$ ]]; then
  MAX_WORKERS="${DEFAULT_MAX_WORKERS}"
fi
if (( MAX_WORKERS < 1 )); then
  MAX_WORKERS=1
fi
if (( CALC_WORKERS > MAX_WORKERS )); then
  CALC_WORKERS="${MAX_WORKERS}"
fi

BACKEND_WORKERS="${BACKEND_WORKERS:-${CALC_WORKERS}}"
if ! [[ "${BACKEND_WORKERS}" =~ ^[0-9]+$ ]]; then
  BACKEND_WORKERS=1
fi
if (( BACKEND_WORKERS < 1 )); then
  BACKEND_WORKERS=1
fi

# Avoid oversubscribing CPU-heavy libraries in each process.
export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"
export NUMEXPR_NUM_THREADS="${NUMEXPR_NUM_THREADS:-1}"
export VECLIB_MAXIMUM_THREADS="${VECLIB_MAXIMUM_THREADS:-1}"

kill_pid_file() {
  local pid_file="$1"
  if [[ -f "${pid_file}" ]]; then
    local pid
    pid="$(cat "${pid_file}" 2>/dev/null || true)"
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" 2>/dev/null || true
      sleep 0.4
      kill -9 "${pid}" 2>/dev/null || true
    fi
    rm -f "${pid_file}"
  fi
}

kill_on_port() {
  local port="$1"
  local pids
  pids="$(lsof -ti :"${port}" 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    echo "[alma] Killing stale process(es) on port ${port}: ${pids}"
    # shellcheck disable=SC2086
    kill ${pids} 2>/dev/null || true
    sleep 0.5
    pids="$(lsof -ti :"${port}" 2>/dev/null || true)"
    if [[ -n "${pids}" ]]; then
      # shellcheck disable=SC2086
      kill -9 ${pids} 2>/dev/null || true
    fi
  fi
}

kill_by_pattern() {
  local pattern="$1"
  local pids
  pids="$(pgrep -f "${pattern}" 2>/dev/null || true)"
  if [[ -n "${pids}" ]]; then
    echo "[alma] Killing process pattern '${pattern}': ${pids}"
    # shellcheck disable=SC2086
    kill ${pids} 2>/dev/null || true
    sleep 0.4
    pids="$(pgrep -f "${pattern}" 2>/dev/null || true)"
    if [[ -n "${pids}" ]]; then
      # shellcheck disable=SC2086
      kill -9 ${pids} 2>/dev/null || true
    fi
  fi
}

wait_for_http() {
  local url="$1"
  local timeout="${2:-30}"
  local i
  for i in $(seq 1 "${timeout}"); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "[alma] Stopping existing ALMa services..."
kill_pid_file "${BACKEND_PID_FILE}"
kill_pid_file "${FRONTEND_PID_FILE}"
kill_on_port "${BACKEND_PORT}"
kill_on_port "${FRONTEND_PORT}"
kill_by_pattern "uvicorn alma.api.app:app"
kill_by_pattern "vite --port ${FRONTEND_PORT}"

echo "[alma] Rebuilding local runtime caches..."
(cd "${ROOT_DIR}" && "${BACKEND_PYTHON}" -m compileall -q src)
rm -rf "${ROOT_DIR}/frontend/node_modules/.vite"

echo "[alma] Starting backend..."
setsid nohup bash -c '
  cd "$1" || exit 1
  export PYTHONPATH="$1/src${PYTHONPATH:+:${PYTHONPATH}}"
  exec "$2" -m uvicorn alma.api.app:app \
    --host "$3" \
    --port "$4" \
    --workers "$5"
' bash "${ROOT_DIR}" "${BACKEND_PYTHON}" "${BACKEND_HOST}" "${BACKEND_PORT}" "${BACKEND_WORKERS}" >> "${BACKEND_LOG}" 2>&1 &
BACK_PID=$!
echo "${BACK_PID}" > "${BACKEND_PID_FILE}"

echo "[alma] Starting frontend..."
setsid nohup bash -c '
  cd "$1/frontend" || exit 1
  exec npx vite \
    --host "$2" \
    --port "$3" \
    --strictPort \
    --force
' bash "${ROOT_DIR}" "${FRONTEND_HOST}" "${FRONTEND_PORT}" >> "${FRONTEND_LOG}" 2>&1 &
FRONT_PID=$!
echo "${FRONT_PID}" > "${FRONTEND_PID_FILE}"

echo "[alma] Waiting for services..."
if ! wait_for_http "http://127.0.0.1:${BACKEND_PORT}/api/v1/health" 45; then
  echo "[alma] Backend failed to become healthy. Last logs:"
  tail -n 80 "${BACKEND_LOG}" || true
  exit 1
fi

if ! wait_for_http "http://127.0.0.1:${FRONTEND_PORT}" 45; then
  echo "[alma] Frontend failed to become healthy. Last logs:"
  tail -n 80 "${FRONTEND_LOG}" || true
  exit 1
fi

echo "[alma] Restart complete."
echo "[alma] Backend:  http://127.0.0.1:${BACKEND_PORT}  (workers=${BACKEND_WORKERS}, logical_cores=${CPU_CORES}, physical_cores=${PHYSICAL_CORES}, reserved=${RESERVED_CORES}, max=${MAX_WORKERS})"
echo "[alma] Frontend: http://127.0.0.1:${FRONTEND_PORT}"
echo "[alma] Logs: ${BACKEND_LOG} / ${FRONTEND_LOG}"
