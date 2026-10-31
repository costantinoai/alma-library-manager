# =============================================================================
# ALMa — self-hosted installer (Windows)
# =============================================================================
# A thin front end to docker-compose.yml. It writes the Compose files and a
# private .env into ~\alma-docker ($env:ALMA_HOME to choose another folder),
# picks the image for your hardware once, then runs `docker compose pull` and
# `up`. The running container is only touched after the pull has succeeded.
# The required OpenAlex API key is added after boot via Settings -> Connections
# (kept in the volume's secret store, never in 'docker inspect').
#
# Usage (PowerShell — Run as a regular user, not Administrator):
#   irm https://raw.githubusercontent.com/costantinoai/alma-library-manager/main/setup.ps1 | iex
#
# Re-run the same command to update. Settings are read from the environment and
# saved in .env, so a later run keeps them:
#   ALMA_ALLOWED_ORIGINS  addresses you open ALMa at, beyond localhost and IPs
#   BIND_ADDR             host address for the port (default 127.0.0.1)
#   ALMA_IMAGE_TAG        exact image tag; or ALMA_VERSION + ALMA_FLAVOR (cpu|gpu|lite)
#   OPENALEX_EMAIL        optional contact email
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

$imageRepo = "ghcr.io/costantinoai/alma-library-manager"
$rawUrl    = "https://raw.githubusercontent.com/costantinoai/alma-library-manager/main"
$almaHome  = if ($env:ALMA_HOME) { $env:ALMA_HOME } else { Join-Path $HOME "alma-docker" }
$envFile   = Join-Path $almaHome ".env"

function Get-Saved($name) {
    if (-not (Test-Path $envFile)) { return "" }
    $line = Get-Content $envFile -Encoding UTF8 | Where-Object { $_ -like "$name=*" } | Select-Object -Last 1
    if (-not $line) { return "" }
    $saved = $line.Substring($name.Length + 1)
    if ($saved -match '^"(.*)"$') { return ($Matches[1] -replace '\\(.)', '$1') }
    return $saved.Trim("'")
}
function Set-Saved($name, $value) {
    # Double quotes with \ " $ escaped: Compose reads any value back as written.
    $escaped = "$value" -replace '([\\"$])', '\$1'
    $lines = @(Get-Content $envFile -Encoding UTF8 | Where-Object { $_ -notlike "$name=*" }) + "$name=`"$escaped`""
    # Compose reads .env as UTF-8. .NET writes it without the BOM of PowerShell 5.1.
    [IO.File]::WriteAllLines($envFile, [string[]]$lines, (New-Object Text.UTF8Encoding $false))
}

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
try {
    docker compose version *> $null
    if ($LASTEXITCODE -ne 0) { throw }
} catch {
    Write-Err2 "Docker Compose v2 not found ('docker compose'). Update Docker Desktop."
    exit 1
}
$dockerVersion = (docker --version) -join ""
Write-Ok "Docker is running ($dockerVersion)"
Write-Host ""

# ---- 2. Write the deployment folder ----------------------------------------
Write-Step 2 5 "Preparing $almaHome..."
New-Item -ItemType Directory -Force -Path $almaHome | Out-Null
if (-not (Test-Path $envFile)) { New-Item -ItemType File -Path $envFile | Out-Null }
# .env can hold API keys: only you may read it.
& icacls $envFile /inheritance:r /grant:r "${env:USERNAME}:F" | Out-Null

foreach ($file in @("docker-compose.yml", "docker-compose.gpu.yml")) {
    $target = Join-Path $almaHome $file
    # Run from a checkout, use its files. Piped through iex there is no script path.
    if ($PSScriptRoot -and (Test-Path (Join-Path $PSScriptRoot $file))) {
        Copy-Item (Join-Path $PSScriptRoot $file) "$target.new" -Force
    } else {
        Invoke-WebRequest -Uri "$rawUrl/$file" -OutFile "$target.new" -UseBasicParsing
    }
    Move-Item "$target.new" $target -Force
}

# An install that Compose does not own here: an older `docker run`, or a
# checkout under another project name. Its settings move into .env; the
# container itself goes only after the pull.
Push-Location $almaHome
$config = (docker compose config) -split "`r?`n"
Pop-Location
if ($LASTEXITCODE -ne 0) { Write-Err2 "Could not read the Compose files."; exit 1 }
$container = (($config | Where-Object { $_ -match '^\s*container_name: ' } | Select-Object -First 1) -replace '^\s*container_name: ', '').Trim()
$project   = (($config | Where-Object { $_ -match '^name: ' } | Select-Object -First 1) -replace '^name: ', '').Trim()

# An external network is not Compose's to create. If one is missing the start
# would fail after the running container has gone, so stop here instead.
$section = ""; $network = ""; $externalNetworks = @()
foreach ($line in $config) {
    if ($line -match '^\S') { $section = $line.Trim(); continue }
    if ($section -ne "networks:") { continue }
    if ($line -match '^    name: (.+)') { $network = $Matches[1].Trim() }
    if ($line -match '^    external: true') { $externalNetworks += $network }
}
foreach ($network in $externalNetworks) {
    docker network inspect $network *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Err2 "The Compose files need the external network '$network', and it does not exist."
        Write-Host "  Create it (docker network create $network) and re-run. Nothing was stopped."
        exit 1
    }
}

$older = ""
$rollback = ""
$carried = @{}
$existing = (docker ps -a --format "{{.Names}}") -split "`n" | Where-Object { $_ -eq $container }
if ($existing) {
    $owner = (docker inspect -f '{{index .Config.Labels "com.docker.compose.project"}}' $container | Out-String).Trim()
    if ($owner -ne $project) { $older = $container }
    # The exact image it runs, as a tag value, for the way back if the new one
    # fails. A moved tag such as `stable` would only find the new image again.
    $running = ((docker inspect -f '{{.Config.Image}}' $container | Out-String).Trim() -split "/")[-1]
    $running = (($running -split "@")[0] -split ":", 2)[-1]
    $imageId = (docker inspect -f '{{.Image}}' $container | Out-String).Trim()
    $digest = (docker image inspect -f '{{index .RepoDigests 0}}' $imageId 2>$null | Out-String).Trim()
    if ($digest -match "@") { $rollback = "$running@" + ($digest -split "@")[-1] }
}
if ($older) {
    Write-Warn2 "Found an older '$older' container. Its settings will carry over."
    $oldImage = (docker inspect -f '{{.Config.Image}}' $older | Out-String).Trim()
    # Only what was set on the container, not what its image defines. By ID: the
    # tag may have moved to an image with other values.
    $format = '{{range .Config.Env}}{{println .}}{{end}}'
    $imageEnv = @(docker image inspect -f $format $imageId)
    foreach ($line in @(docker inspect -f $format $older)) {
        if ($line -and ($imageEnv -notcontains $line)) {
            $name, $value = $line -split "=", 2
            $carried[$name] = $value
        }
    }
    $oldBind = (docker inspect -f '{{range (index .HostConfig.PortBindings "8000/tcp")}}{{.HostIp}}{{end}}' $older | Out-String).Trim()
    if ($oldBind -and $oldBind -ne "127.0.0.1") { $carried["BIND_ADDR"] = $oldBind }
    # Stay on the channel: a `latest` database can be too new for `stable`.
    if ($oldImage -like "${imageRepo}:*") { $carried["ALMA_IMAGE_TAG"] = $oldImage.Split(":")[-1] }
}
Write-Ok "Deployment folder ready"
Write-Host ""

# ---- 3. Pick the image ------------------------------------------------------
# A saved choice stands: a re-run must not move an install to another flavor.
Write-Step 3 5 "Choosing the right image for your hardware..."
function Find-Flavor {
    $nvidia = Get-Command nvidia-smi -ErrorAction SilentlyContinue
    $hasNvidia = $false
    if ($nvidia) {
        try { & nvidia-smi *> $null; if ($LASTEXITCODE -eq 0) { $hasNvidia = $true } } catch {}
    }
    if ($hasNvidia) {
        $dockerInfo = (docker info 2>$null) -join "`n"
        if ($dockerInfo -match "(?i)runtimes:.*nvidia") { return "gpu" }
        Write-Warn2 "NVIDIA GPU detected but the NVIDIA Container Toolkit isn't registered with Docker."
        Write-Warn2 "On Windows, enable WSL2 GPU passthrough: https://docs.nvidia.com/cuda/wsl-user-guide/"
    }
    return "cpu"
}

$tag = $env:ALMA_IMAGE_TAG
if (-not $tag -and -not $env:ALMA_VERSION) {
    $tag = Get-Saved "ALMA_IMAGE_TAG"
    if (-not $tag) { $tag = $carried["ALMA_IMAGE_TAG"] }
}
if (-not $tag) {
    $flavor = if ($env:ALMA_FLAVOR) { $env:ALMA_FLAVOR } else { Find-Flavor }
    $tag = if ($env:ALMA_VERSION) { $env:ALMA_VERSION } else { "stable" }
    if ($flavor -eq "gpu")  { $tag = "$tag-gpu" }
    if ($flavor -eq "lite") { $tag = "$tag-lite" }
}

$stack = "docker-compose.yml"
if ($tag -like "*gpu*") { $stack = "$stack;docker-compose.gpu.yml" }
if (Test-Path (Join-Path $almaHome "docker-compose.override.yml")) { $stack = "$stack;docker-compose.override.yml" }

# The chosen values go to Compose through the environment, and into .env only
# once the pull has worked, so a bad tag does not stick.
$env:ALMA_IMAGE_TAG = $tag
$env:COMPOSE_FILE = $stack
Write-Ok "Selected: ${imageRepo}:$tag"
Write-Host ""

# ---- 4. Pull while the old container keeps serving --------------------------
Write-Step 4 5 "Pulling the image..."
Push-Location $almaHome
docker compose pull
$pulled = $LASTEXITCODE
Pop-Location
if ($pulled -ne 0) {
    Write-Err2 "Pull failed. Nothing was stopped; the running ALMa is unchanged."
    exit 1
}

# OpenAlex requires an API key on every request (since 2026-02-13). We do NOT
# prompt for it here: the key is added after boot via Settings -> Connections,
# which persists it to the 0600 secret store inside the alma-data volume - so
# it never lands in 'docker inspect' or shell history.
foreach ($name in @($carried.Keys)) {
    if ($name -ne "ALMA_IMAGE_TAG" -and -not (Get-Saved $name)) { Set-Saved $name $carried[$name] }
}
Set-Saved "ALMA_IMAGE_TAG" $tag
Set-Saved "COMPOSE_FILE" $stack
foreach ($name in @("ALMA_ALLOWED_ORIGINS", "BIND_ADDR", "OPENALEX_EMAIL")) {
    $value = [Environment]::GetEnvironmentVariable($name)
    if ($value) { Set-Saved $name $value }
}

$bindNow = Get-Saved "BIND_ADDR"
if ($bindNow -and $bindNow -ne "127.0.0.1") {
    Write-Warn2 "Binding to $bindNow - ALMa will be reachable from other devices."
    Write-Warn2 "ALMa has no login; only do this on a trusted private network."
}

# ---- 5. Start and wait for health ------------------------------------------
Write-Step 5 5 "Starting ALMa..."
if ($older) {
    docker stop $older | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Err2 "Could not stop '$older'."; exit 1 }
    docker rm $older | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Err2 "Could not remove '$older'."; exit 1 }
}

# A first boot may migrate the database, which is why the wait is long. A
# health check that keeps failing ends it sooner.
Push-Location $almaHome
docker compose up -d --wait --wait-timeout 300
$started = $LASTEXITCODE
Pop-Location
if ($started -ne 0) {
    Write-Err2 "ALMa did not become healthy. Check: docker logs $container"
    if ($rollback) {
        Write-Host "The previous container was replaced; your data is still in the volumes."
        Write-Host "To go back to the image that was running:"
        Write-Host "  cd `"$almaHome`""
        Write-Host "  (Get-Content .env) -replace '^ALMA_IMAGE_TAG=.*', 'ALMA_IMAGE_TAG=`"$rollback`"' | Set-Content .env"
        Write-Host "  docker compose up -d --wait"
        Write-Host "That pins this exact image. To follow a tag again, re-run the installer with"
        Write-Host "ALMA_IMAGE_TAG set to it."
        Write-Host "If the new version already changed the database, the old one cannot use it:"
        Write-Host "restore a backup, https://costantinoai.github.io/alma-library-manager/operations/backups/#restore"
    }
    exit 1
}
Write-Ok "ALMa is up."

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
Write-Host "  Settings:     $envFile"
Write-Host "  Logs:         docker logs -f alma"
Write-Host "  Stop:         docker stop alma"
Write-Host "  Start:        docker start alma"
Write-Host "  Update:       re-run this installer"
Write-Host "  Uninstall:    cd $almaHome; docker compose down; docker volume rm alma-data alma-config"
Write-Host ""
Write-Host "  Your library lives in the 'alma-data' Docker volume - it survives"
Write-Host "  container removal and image upgrades. Only 'docker volume rm' wipes it."
Write-Host ""
