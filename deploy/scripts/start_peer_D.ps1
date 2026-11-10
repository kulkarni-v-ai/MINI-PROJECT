# ============================================================================
# start_peer_D.ps1 - Launch Replacement Peer D (Runs on PC4 or any spare PC)
# Usage: .\start_peer_D.ps1 [-PeerIP "192.168.1.100"] [-PeerPort 5004] [-CoordIP "192.168.1.100"] [-CoordPort 5000]
# ============================================================================
param(
    [string]$PeerIP = "192.168.1.100",
    [int]$PeerPort = 5004,
    [string]$CoordIP = "192.168.1.100",
    [int]$CoordPort = 5000
)

$binDir = Join-Path (Split-Path -Parent $PSScriptRoot) "bin"
$exe = Join-Path $binDir "peer.exe"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  STARTING REPLACEMENT PEER D                            " -ForegroundColor Cyan
Write-Host "  Endpoint:     $PeerIP : $PeerPort                      " -ForegroundColor Cyan
Write-Host "  Coordinator:  $CoordIP : $CoordPort                    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location (Split-Path -Parent $PSScriptRoot)
& $exe PEER_D $PeerIP $PeerPort $CoordIP $CoordPort
