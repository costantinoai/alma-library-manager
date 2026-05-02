# =============================================================================
# ALMa — Production Dockerfile (multi-variant: `normal` / `lite`)
# =============================================================================
# Layout principle: keep the package source at /app in both builder and
# runtime. This lets us install the package editable (`pip install -e .`)
# so that __file__-based path lookups inside the code (e.g., locating .env
# at the project root) continue to resolve to /app, the same location the
# Settings UI writes to via python-dotenv's set_key.
#
# Two variants, controlled by the `VARIANT` build arg:
#   - VARIANT=normal (default) — adds transformers + adapters on top of
#     core. This pulls torch transitively (~1.5 GB) and unlocks the
#     local SPECTER2 encoder so embeddings can be computed without a
#     cloud API. Use this on a desktop / server with ≥4 GB RAM.
#   - VARIANT=lite             — core only. No torch / transformers /
#     adapters. Discovery branches (HDBSCAN/KMeans), the Insights
#     Graph (UMAP/t-SNE), and TF-IDF similarity all still work — those
#     deps live in core. What you lose is the local embedding encoder;
#     in lite you'd configure embeddings via OpenAI in Settings (or
#     rely on Semantic Scholar's cached vectors). Suitable for
#     Raspberry Pi or other low-memory hosts.
#
# Build:
#   docker build --build-arg VARIANT=normal -t alma:normal .
#   docker build --build-arg VARIANT=lite   -t alma:lite   .
# Or via compose: see docker-compose.yml + the ALMA_VARIANT env var.
#
# Image contents (read-only):
#   /app/src/                 Python source (editable-installed into venv)
#   /app/pyproject.toml       Needed for the editable install to remain valid
#   /app/frontend/dist/       Built React SPA, served by FastAPI
#   /opt/venv                 Pinned dependencies (variant-dependent)
#
# NEVER baked into the image:
#   .env                      Secrets — bind-mounted from host at runtime
#   data/                     SQLite + caches — bind-mounted from host
#   config/                   Plugin configs — bind-mounted from host
#   settings.json             User preferences — bind-mounted from host
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: Frontend — Build the React SPA
# -----------------------------------------------------------------------------
FROM node:20-slim AS frontend

WORKDIR /frontend
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
# Use the docker-specific build that skips `tsc -b` — Vite's bundler
# strips types regardless, and we don't want in-progress type errors
# on host branches to break image builds.
RUN npm run build:docker

# -----------------------------------------------------------------------------
# Stage 2: Builder — Install Python dependencies into a venv
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS builder

# Variant selector. `normal` includes the AI stack; `lite` skips it.
ARG VARIANT=normal

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build deps for native wheels (numpy/scikit-learn/hdbscan/umap-learn).
# Most ship manylinux wheels, but g++ keeps fallbacks safe. Even the
# lite variant builds numpy from source on some ARM hosts, so keep
# the toolchain in the builder stage regardless of variant.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        g++ \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install third-party deps in two layers so the cache stays warm even
# when toggling variants. Core deps come first (always installed); the
# AI layer is conditional.
WORKDIR /app
COPY requirements-core.txt ./
RUN pip install --no-cache-dir -r requirements-core.txt

COPY requirements-ai.txt ./
RUN if [ "$VARIANT" = "normal" ]; then \
        pip install --no-cache-dir -r requirements-ai.txt ; \
    elif [ "$VARIANT" = "lite" ]; then \
        echo "[alma:lite] Skipping local SPECTER2 encoder (transformers / adapters / torch)" ; \
    else \
        echo "ERROR: Unknown VARIANT='$VARIANT'. Expected 'normal' or 'lite'." >&2 ; \
        exit 1 ; \
    fi

# Install the alma package itself, editable, so __file__ resolves to
# /app/src/alma/... and project-root lookups land on /app.
COPY pyproject.toml ./
COPY src/ ./src/
RUN pip install --no-cache-dir --no-deps -e .

# -----------------------------------------------------------------------------
# Stage 3: Runtime — Minimal production image
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# Re-declare in this stage so the runtime LABEL + ENV pick it up. Without
# this, the ARG from the builder stage is invisible here.
ARG VARIANT=normal

LABEL org.opencontainers.image.title="ALMa" \
      org.opencontainers.image.description="ALMa — personal academic library with AI-powered discovery" \
      org.opencontainers.image.licenses="CC-BY-NC-4.0" \
      org.opencontainers.image.source="https://github.com/costantinoai/alma" \
      alma.variant="${VARIANT}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYTHONPYCACHEPREFIX=/tmp/pycache \
    ENVIRONMENT=production \
    ALMA_VARIANT="${VARIANT}" \
    PATH="/opt/venv/bin:$PATH"

# curl for the healthcheck; ca-certificates for TLS to upstream APIs.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /app

# Bring the venv and the source layout from the builder. The editable
# install in /opt/venv references /app/src — both paths exist here, so
# imports resolve cleanly.
COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/pyproject.toml ./pyproject.toml
COPY --from=builder /app/src ./src

# Built frontend SPA, served by FastAPI (see app.py:_frontend_dist).
COPY --from=frontend /frontend/dist ./frontend/dist

# Non-root default user for `docker run` (no --user flag). Compose
# overrides this with the host UID via `user: ${UID}:${GID}` so files
# in bind-mounted dirs stay owned by the user.
#
# Code at /app/src and /app/frontend is read-only at runtime: chmod
# world-readable so any UID can `import alma` and serve assets.
# Writable bind-mount targets (data, config) get a permissive mode so
# the chosen runtime UID can write into them; the bind mounts then
# override these with the host's actual ownership.
RUN groupadd --gid 10001 appgroup && \
    useradd --uid 10001 --gid appgroup --shell /bin/false --no-create-home appuser && \
    mkdir -p /app/data /app/config /tmp/pycache && \
    chmod -R a+rX /app/src /app/frontend /app/pyproject.toml /opt/venv && \
    chmod 1777 /app/data /app/config /tmp/pycache

# Settings file lives in the writable data volume, not /app/. The
# project root is read-only at runtime (root:root 755) so appuser
# cannot create /app/settings.json on first boot. config.py honours
# ALMA_SETTINGS_PATH to override that location; pointing it at the
# data volume means settings persist alongside scholar.db without
# requiring a host bind-mount.
ENV ALMA_SETTINGS_PATH=/app/data/settings.json

# HuggingFace cache. Local SPECTER2 (transformers + adapters) downloads
# config + tokenizer + model weights from HF on first use. The default
# `~/.cache/huggingface` lives under root's home (`/root/.cache`) which
# is part of the read-only image layer — `transformers.AutoConfig.
# from_pretrained` blows up with "Read-only file system" before it can
# parse the config, surfacing as a misleading "Unrecognized model in
# allenai/specter2_base" error. Routing the cache to the writable data
# volume lets the model download once and persist across restarts.
ENV HF_HOME=/app/data/.hf-cache \
    TRANSFORMERS_CACHE=/app/data/.hf-cache \
    HF_HUB_CACHE=/app/data/.hf-cache

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -fsS http://localhost:8000/api/v1/health || exit 1

USER appuser:appgroup

EXPOSE 8000

# --workers=1 — SQLite is single-writer; we don't want multiple uvicorn
# workers fighting over scholar.db. Scale via async, not processes.
CMD ["python", "-m", "uvicorn", "alma.api.app:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--proxy-headers", \
     "--forwarded-allow-ips", "*"]
