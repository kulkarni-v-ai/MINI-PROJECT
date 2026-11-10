# ============================================================================
# start_peer_A.ps1 - Launch Peer A on PC1
# Usage: .\start_peer_A.ps1 [-PeerIP "192.168.1.101"] [-PeerPort 5001] [-CoordIP "192.168.1.100"] [-CoordPort 5000]
# ============================================================================
param(
    [string]$PeerIP = "192.168.1.101",
    [int]$PeerPort = 5001,
    [string]$CoordIP = "192.168.1.100",
    [int]$CoordPort = 5000
)

$binDir = Join-Path (Split-Path -Parent $PSScriptRoot) "bin"
$exe = Join-Path $binDir "peer.exe"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  STARTING PEER A (PC1)                                  " -ForegroundColor Cyan
Write-Host "  Endpoint:     $PeerIP : $PeerPort                      " -ForegroundColor Cyan
Write-Host "  Coordinator:  $CoordIP : $CoordPort                    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location (Split-Path -Parent $PSScriptRoot)
& $exe PEER_A $PeerIP $PeerPort $CoordIP $CoordPort
