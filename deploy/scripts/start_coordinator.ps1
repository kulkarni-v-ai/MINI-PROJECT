# ============================================================================
# start_coordinator.ps1 - Launch Central Coordination Server on PC4
# Usage: .\start_coordinator.ps1 [-BindAddress "0.0.0.0"] [-Port 5000] [-HttpPort 8080]
# ============================================================================
param(
    [string]$BindAddress = "0.0.0.0",
    [int]$Port = 5000,
    [int]$HttpPort = 8080
)

$binDir = Join-Path (Split-Path -Parent $PSScriptRoot) "bin"
$exe = Join-Path $binDir "controller.exe"

if (-not (Test-Path $exe)) {
    Write-Error "controller.exe not found at $exe"
    exit 1
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  STARTING CENTRAL COORDINATION SERVER (PC4)              " -ForegroundColor Cyan
Write-Host "  TCP Control Plane: $BindAddress : $Port                 " -ForegroundColor Cyan
Write-Host "  HTTP Web Console:  http://<PC4_IP>:$HttpPort/           " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location (Split-Path -Parent $PSScriptRoot)
& $exe coordinator $Port $BindAddress $HttpPort
