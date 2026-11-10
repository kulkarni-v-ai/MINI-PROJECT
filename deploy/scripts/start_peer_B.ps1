# ============================================================================
# start_peer_B.ps1 - Launch Peer B on PC2
# Usage: .\start_peer_B.ps1 [-PeerIP "192.168.1.102"] [-PeerPort 5002] [-CoordIP "192.168.1.100"] [-CoordPort 5000]
# ============================================================================
param(
    [string]$PeerIP = "192.168.1.102",
    [int]$PeerPort = 5002,
    [string]$CoordIP = "192.168.1.100",
    [int]$CoordPort = 5000
)

$binDir = Join-Path (Split-Path -Parent $PSScriptRoot) "bin"
$exe = Join-Path $binDir "peer.exe"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  STARTING PEER B (PC2)                                  " -ForegroundColor Cyan
Write-Host "  Endpoint:     $PeerIP : $PeerPort                      " -ForegroundColor Cyan
Write-Host "  Coordinator:  $CoordIP : $CoordPort                    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location (Split-Path -Parent $PSScriptRoot)
& $exe PEER_B $PeerIP $PeerPort $CoordIP $CoordPort
