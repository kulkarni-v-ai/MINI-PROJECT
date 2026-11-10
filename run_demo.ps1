# ============================================================================
# run_demo.ps1 - One-Command Live Cluster & Web Visualizer Launcher (Windows)
#
# Works immediately from either freshly built binaries (build\Release\) OR
# zero-dependency portable binaries (deploy\bin\) on a clean Windows system.
#
# Usage:
#   powershell -NoProfile -ExecutionPolicy Bypass -File .\run_demo.ps1
#   powershell -NoProfile -ExecutionPolicy Bypass -File .\run_demo.ps1 -Peers 5 -Reset
# ============================================================================
param(
    [int]$Peers = 4,
    [int]$TcpPort = 5000,
    [int]$HttpPort = 8080,
    [switch]$Reset,
    [switch]$NoBrowser
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
Set-Location $root

if ($Peers -lt 3 -or $Peers -gt 8) {
    Write-Error "Please specify -Peers between 3 and 8 (recommended: 4 or 5)."
    exit 1
}

$binDir = Join-Path $root "build\Release"
if (-not (Test-Path (Join-Path $binDir "controller.exe"))) {
    $binDir = Join-Path $root "deploy\bin"
}
$ctrlExe = Join-Path $binDir "controller.exe"
$peerExe = Join-Path $binDir "peer.exe"
$rfeExe  = Join-Path $binDir "rfe.exe"

if (-not (Test-Path $ctrlExe) -or -not (Test-Path $peerExe)) {
    Write-Error "Binaries not found in build\Release or deploy\bin. Build with CMake or download complete repo."
    exit 1
}

Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "  RESILIENT MULTI-PEER FILE STORAGE & RECOVERY ENGINE - LIVE DEMO " -ForegroundColor Cyan
Write-Host "==================================================================" -ForegroundColor Cyan
Write-Host "Using binaries : $binDir" -ForegroundColor DarkGray

if ($Reset) {
    Write-Host "Resetting previous metadata, peer chunks & output staging..." -ForegroundColor Yellow
    & $ctrlExe reset
}

$procs = @()
try {
    # 1. Start Central Coordination Server (TCP Control Plane + PowerShell Web Monitor)
    $coord = Start-Process -FilePath $ctrlExe `
        -ArgumentList "coordinator", "$TcpPort", "0.0.0.0", "$HttpPort" `
        -WorkingDirectory $root -PassThru
    $procs += $coord
    Write-Host "[Coordinator] Started PID $($coord.Id) -> TCP :$TcpPort | Web Monitor http://127.0.0.1:$HttpPort/" -ForegroundColor Green

    Start-Sleep -Milliseconds 600

    # 2. Start participating storage peers as real independent OS daemons
    $names = @("PEER_A", "PEER_B", "PEER_C", "PEER_D", "PEER_E", "PEER_F", "PEER_G", "PEER_H")
    for ($i = 0; $i -lt $Peers; $i++) {
        $pName = $names[$i]
        $pPort = $TcpPort + 1 + $i
        $p = Start-Process -FilePath $peerExe `
            -ArgumentList $pName, "127.0.0.1", "$pPort", "127.0.0.1", "$TcpPort" `
            -WorkingDirectory $root -PassThru
        $procs += $p
        Write-Host "[$pName]       Started PID $($p.Id) -> 127.0.0.1:$pPort" -ForegroundColor Green
    }

    # 3. Wait for all peers to register ONLINE
    $deadline = (Get-Date).AddSeconds(6)
    $online = 0
    while ((Get-Date) -lt $deadline) {
        try {
            $st = Invoke-RestMethod -Uri "http://127.0.0.1:$HttpPort/api/status" -TimeoutSec 2
            $online = [int]$st.peers_online
            if ($online -ge $Peers) { break }
        } catch {}
        Start-Sleep -Milliseconds 250
    }

    Write-Host "`nCluster Ready: $online / $Peers peers ONLINE" -ForegroundColor Cyan
    Write-Host "PowerShell Web Monitor : http://127.0.0.1:$HttpPort/  (Token: rfe-demo-token)" -ForegroundColor Yellow
    Write-Host "Quick CLI commands in another terminal:" -ForegroundColor White
    Write-Host "  $rfeExe connect 127.0.0.1:$TcpPort --token rfe-demo-token" -ForegroundColor Gray
    Write-Host "  $rfeExe peers" -ForegroundColor Gray
    Write-Host "  $rfeExe upload .\README.md" -ForegroundColor Gray
    Write-Host "  $rfeExe info F001" -ForegroundColor Gray
    Write-Host "  $rfeExe download F001" -ForegroundColor Gray
    Write-Host "`nTip: Close any PEER_x window (or Ctrl+C) to trigger live HPFD + HRERA recovery!" -ForegroundColor DarkCyan

    if (-not $NoBrowser) {
        Start-Process "http://127.0.0.1:$HttpPort/"
    }

    Write-Host "`nPress [ENTER] in this window to stop all Coordinator and Peer processes cleanly..." -ForegroundColor Magenta
    [void](Read-Host)
}
finally {
    Write-Host "`nStopping Coordinator & Peer processes..." -ForegroundColor Yellow
    foreach ($p in $procs) {
        if ($p -and -not $p.HasExited) {
            Stop-Process -Id $p.Id -Force -ErrorAction SilentlyContinue
        }
    }
    Write-Host "All processes stopped cleanly." -ForegroundColor Green
}
