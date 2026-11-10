# ============================================================================
# start_peer_C.ps1 - Launch Peer C on PC3
# Usage: .\start_peer_C.ps1 [-PeerIP "192.168.1.103"] [-PeerPort 5003] [-CoordIP "192.168.1.100"] [-CoordPort 5000]
# ============================================================================
param(
    [string]$PeerIP = "192.168.1.103",
    [int]$PeerPort = 5003,
    [string]$CoordIP = "192.168.1.100",
    [int]$CoordPort = 5000
)

$binDir = Join-Path (Split-Path -Parent $PSScriptRoot) "bin"
$exe = Join-Path $binDir "peer.exe"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "  STARTING PEER C (PC3)                                  " -ForegroundColor Cyan
Write-Host "  Endpoint:     $PeerIP : $PeerPort                      " -ForegroundColor Cyan
Write-Host "  Coordinator:  $CoordIP : $CoordPort                    " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Set-Location (Split-Path -Parent $PSScriptRoot)
& $exe PEER_C $PeerIP $PeerPort $CoordIP $CoordPort
