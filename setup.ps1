# =============================================================================
# ALMa — self-hosted installer (Windows)
# =============================================================================
# Pulls the prebuilt image from GitHub Container Registry, picks the right
# image variant for your hardware (GPU / CPU), and starts ALMa with named
# Docker volumes so your library survives upgrades. The required OpenAlex
# API key is added after boot via Settings -> Connections (kept in the
# volume's secret store, never in 'docker inspect').
#
# Usage (PowerShell — Run as a regular user, not Administrator):
#   irm https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.ps1 | iex
#
# Re-run the same command to update — the script detects an existing install
# and pulls the latest image before restarting.
# =============================================================================
#Requires -Version 5.1
$ErrorActionPreference = "Stop"

function Write-Step($n, $total, $msg) { Write-Host "[$n/$total] $msg" -ForegroundColor Blue }
function Write-Ok($msg)               { Write-Host "OK  $msg" -ForegroundColor Green }
function Write-Warn2($msg)            { Write-Host "WARN $msg" -ForegroundColor Yellow }
function Write-Err2($msg)             { Write-Host "ERR  $msg" -ForegroundColor Red }

Write-Host ""
Write-Host "  +-------------------------------------------+" -ForegroundColor Blue
Write-Host "  |              ALMa  Installer              |" -ForegroundColor Blue
Write-Host "  |   Another Library Manager - self-hosted   |" -ForegroundColor Blue
Write-Host "  +-------------------------------------------+" -ForegroundColor Blue
Write-Host ""

# ---- 1. Docker check -------------------------------------------------------
Write-Step 1 5 "Checking Docker installation..."
$dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
if (-not $dockerCmd) {
    Write-Err2 "Docker not found on PATH."
    Write-Host ""
    Write-Host "Install Docker Desktop for Windows first:"
    Write-Host "  https://docs.docker.com/desktop/install/windows-install/"
    Write-Host ""
    Write-Host "Then re-open PowerShell and re-run this installer."
    exit 1
}
try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { throw }
} catch {
    Write-Err2 "Docker is installed but the daemon isn't responding."
    Write-Host "  Start Docker Desktop and wait for the whale icon to settle, then re-run."
    exit 1
}
$dockerVersion = (docker --version) -join ""
Write-Ok "Docker is running ($dockerVersion)"
Write-Host ""

# ---- 2. Pick image variant -------------------------------------------------
Write-Step 2 5 "Choosing the right image for your hardware..."
$imageBase = "ghcr.io/costantinoai/alma-library-manager"
$gpuArgs   = @()
$tag       = "latest"
$variantLabel = "CPU (~1.4 GB image)"

# GPU detection — NVIDIA host with the Container Toolkit
$nvidia = Get-Command nvidia-smi -ErrorAction SilentlyContinue
$hasNvidia = $false
if ($nvidia) {
    try { & nvidia-smi *> $null; if ($LASTEXITCODE -eq 0) { $hasNvidia = $true } } catch {}
}
if ($hasNvidia) {
    $dockerInfo = (docker info 2>$null) -join "`n"
    if ($dockerInfo -match "(?i)runtimes:.*nvidia") {
        $tag = "latest-gpu"
        $gpuArgs = @("--gpus", "all")
        $variantLabel = "GPU (CUDA, ~3.2 GB image)"
    } else {
        Write-Warn2 "NVIDIA GPU detected but the NVIDIA Container Toolkit isn't registered with Docker."
        Write-Warn2 "On Windows, enable WSL2 GPU passthrough: https://docs.nvidia.com/cuda/wsl-user-guide/"
    }
}

# Honour ALMA_IMAGE_TAG override
if ($env:ALMA_IMAGE_TAG) {
    $tag = $env:ALMA_IMAGE_TAG
    $variantLabel = "custom (ALMA_IMAGE_TAG=$tag)"
    $gpuArgs = @()
    if ($tag -like "*gpu*") { $gpuArgs = @("--gpus", "all") }
}

$image = "$imageBase`:$tag"
Write-Ok "Selected: $image  -  $variantLabel"
Write-Host ""

# ---- 3. OpenAlex credentials -----------------------------------------------
# OpenAlex requires an API key on every request (since 2026-02-13). We do
# NOT prompt for it here: the key is added after boot via Settings ->
# Connections, which persists it to the 0600 secret store inside the
# alma-data volume - so it never lands in 'docker inspect' or shell history.
# An optional contact email (no longer affects rate limits) is passed
# through only if it's already in the environment.
Write-Step 3 5 "Configuring OpenAlex access..."
$openalexEmail = $env:OPENALEX_EMAIL
$emailArgs = @()
if ($openalexEmail) {
    $emailArgs = @("-e", "OPENALEX_EMAIL=$openalexEmail")
    Write-Ok "Optional contact email: $openalexEmail"
}
Write-Host "OpenAlex now requires a free API key - you'll add it in the UI after"
Write-Host "boot (Settings -> Connections). Get one in ~30s at openalex.org/settings/api."
Write-Host ""

# ---- 4. Stop existing container (if any) -----------------------------------
Write-Step 4 5 "Pulling image and (re)starting container..."
$existing = (docker ps -a --format "{{.Names}}" 2>$null) -split "`n" | Where-Object { $_ -eq "alma" }
if ($existing) {
    Write-Warn2 "Existing 'alma' container found - stopping and removing it (your data in the named volumes is preserved)."
    docker rm -f alma | Out-Null
}

docker pull $image
if ($LASTEXITCODE -ne 0) { Write-Err2 "Pull failed."; exit 1 }

# ---- 5. Run ---------------------------------------------------------------
# Bind to localhost only by default - ALMa has no auth in single-user mode.
# Set $env:BIND_ADDR="0.0.0.0" to reach it from other devices (trusted
# networks only). See the README "Exposing on your network".
$bindAddr = if ($env:BIND_ADDR) { $env:BIND_ADDR } else { "127.0.0.1" }
if ($bindAddr -ne "127.0.0.1") {
    Write-Warn2 "Binding to $bindAddr - ALMa will be reachable from other devices."
    Write-Warn2 "ALMa has no auth by default; only do this on a trusted network."
}

$runArgs = @(
    "run", "-d",
    "--name", "alma",
    "--restart", "unless-stopped",
    "-p", "${bindAddr}:8000:8000",
    "-e", "ALMA_SETTINGS_PATH=/app/data/settings.json",
    "-v", "alma-data:/app/data",
    "-v", "alma-config:/app/config"
) + $emailArgs + $gpuArgs + @($image)

& docker @runArgs | Out-Null
if ($LASTEXITCODE -ne 0) { Write-Err2 "Container failed to start."; exit 1 }

Write-Host ""
Write-Step 5 5 "Verifying..."
$healthy = $false
for ($i = 0; $i -lt 30; $i++) {
    try {
        $resp = Invoke-WebRequest -Uri "http://localhost:8000/api/v1/health" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($resp.StatusCode -eq 200) { $healthy = $true; break }
    } catch {}
    Start-Sleep -Seconds 1
}

if ($healthy) {
    Write-Ok "ALMa is up."
} else {
    Write-Warn2 "Container started but the health endpoint didn't respond within 30s - check 'docker logs alma'."
}

Write-Host ""
Write-Host "----------------------------------------------------------------"
Write-Host "Setup complete." -ForegroundColor Green
Write-Host "----------------------------------------------------------------"
Write-Host ""
Write-Host "  Open ALMa:    http://localhost:8000"
Write-Host ""
Write-Host "  Next step (required): add your OpenAlex API key" -ForegroundColor Yellow
Write-Host "    Settings -> Connections -> OpenAlex, then 'Save connection settings'."
Write-Host "    Free key (~30s): https://openalex.org/settings/api"
Write-Host "    A Semantic Scholar key (Settings -> Connections) is recommended too."
Write-Host "    Keys are stored in the alma-data volume's 0600 secret store."
Write-Host ""
Write-Host "  Logs:         docker logs -f alma"
Write-Host "  Stop:         docker stop alma"
Write-Host "  Start:        docker start alma"
Write-Host "  Update:       re-run this installer"
Write-Host "  Uninstall:    docker rm -f alma; docker volume rm alma-data alma-config"
Write-Host ""
Write-Host "  Your library lives in the 'alma-data' Docker volume - it survives"
Write-Host "  container removal and image upgrades. Only 'docker volume rm' wipes it."
Write-Host ""
