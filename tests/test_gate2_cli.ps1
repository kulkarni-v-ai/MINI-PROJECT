# ============================================================================
# test_gate2_cli.ps1 - End-to-end test of native rfe.exe CLI & HTTP Console
# ============================================================================
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "==================== Testing Native rfe.exe CLI & HTTP API ====================" -ForegroundColor Cyan

# Start Coordinator on TCP 5700 & HTTP 8081 in background
$coordProc = Start-Process -FilePath ".\build\Release\controller.exe" -ArgumentList "coordinator", "5700", "127.0.0.1", "8081" -PassThru -WindowStyle Hidden
Write-Host "Started Coordinator PID $($coordProc.Id) on TCP 5700, HTTP 8081"

# Start 3 peers
$peerA = Start-Process -FilePath ".\build\Release\peer.exe" -ArgumentList "PEER_A", "127.0.0.1", "5701", "127.0.0.1", "5700" -PassThru -WindowStyle Hidden
$peerB = Start-Process -FilePath ".\build\Release\peer.exe" -ArgumentList "PEER_B", "127.0.0.1", "5702", "127.0.0.1", "5700" -PassThru -WindowStyle Hidden
$peerC = Start-Process -FilePath ".\build\Release\peer.exe" -ArgumentList "PEER_C", "127.0.0.1", "5703", "127.0.0.1", "5700" -PassThru -WindowStyle Hidden
$peerD = $null
Write-Host "Started Peers PEER_A (5701), PEER_B (5702), PEER_C (5703)"

try {
    Start-Sleep -Milliseconds 1300

    # 1. rfe connect (authenticates session)
    Write-Host "`n>>> rfe connect 127.0.0.1:5700 --user demo-op --token rfe-demo-token" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" connect 127.0.0.1:5700 --user demo-op --token rfe-demo-token
    if ($LASTEXITCODE -ne 0) { throw "rfe connect failed" }

    # 2. rfe peers
    Write-Host "`n>>> rfe peers" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" peers
    if ($LASTEXITCODE -ne 0) { throw "rfe peers failed" }

    # 3. Create test binary payload (>100 KB for multi-stripe RS) and rfe upload
    New-Item -ItemType Directory -Force -Path "output" | Out-Null
    $testFile = "output\cli_demo_input.bin"
    $bytes = New-Object byte[] 150000
    (New-Object System.Random 42).NextBytes($bytes)
    [System.IO.File]::WriteAllBytes((Join-Path $root $testFile), $bytes)
    $origHash = (Get-FileHash $testFile -Algorithm SHA256).Hash.ToLower()

    Write-Host "`n>>> rfe upload $testFile" -ForegroundColor Yellow
    $uploadOut = & ".\build\Release\rfe.exe" upload $testFile | Out-String
    Write-Host $uploadOut
    if ($LASTEXITCODE -ne 0) { throw "rfe upload failed" }

    $match = [regex]::Match($uploadOut, "File ID:\s+([A-Z0-9]+)")
    if (-not $match.Success) { throw "Could not extract File ID from upload output" }
    $fileId = $match.Groups[1].Value
    Write-Host "Captured File ID: $fileId" -ForegroundColor Green

    # 4. rfe files, search, info
    Write-Host "`n>>> rfe files" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" files

    Write-Host "`n>>> rfe search cli_demo" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" search cli_demo

    Write-Host "`n>>> rfe info $fileId" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" info $fileId

    # 5. Check HTTP Web Console /api/status
    Write-Host "`n>>> GET http://127.0.0.1:8081/api/status" -ForegroundColor Yellow
    $httpStatus = Invoke-RestMethod -Uri "http://127.0.0.1:8081/api/status"
    Write-Host ($httpStatus | ConvertTo-Json -Compress)
    if ($httpStatus.peers_online -ne 3) { throw "HTTP /api/status expected 3 peers_online" }

    # 6. rfe download (peer-based RS reconstruction) + SHA-256 check
    Write-Host "`n>>> rfe download $fileId" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" download $fileId
    if ($LASTEXITCODE -ne 0) { throw "rfe download failed" }
    $dlHash = (Get-FileHash "output\cli_demo_input.bin" -Algorithm SHA256).Hash.ToLower()
    if ($dlHash -ne $origHash) { throw "Downloaded file SHA-256 mismatch!" }

    # 7. rfe download --source central
    Write-Host "`n>>> rfe download $fileId --source central" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" download $fileId --source central
    if ($LASTEXITCODE -ne 0) { throw "rfe download --source central failed" }

    # 8. Simulate Peer B crash & verify 'rfe peers' accurately reports OFFLINE
    Write-Host "`n>>> Stopping PEER_B and waiting 2.5s for heartbeat timeout (threshold = 2 x 1000ms)..." -ForegroundColor Yellow
    Stop-Process -Id $peerB.Id -Force
    Start-Sleep -Milliseconds 2500

    $peersAfter = & ".\build\Release\rfe.exe" peers | Out-String
    Write-Host $peersAfter
    if ($peersAfter -notmatch "PEER_B.*OFFLINE") {
        throw "rfe peers did not report PEER_B as OFFLINE!"
    }

    # 9. Download cleanly from surviving PEER_A + PEER_C while PEER_B is offline
    Write-Host "`n>>> rfe download $fileId (with PEER_B offline)" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" download $fileId
    if ($LASTEXITCODE -ne 0) { throw "Degraded rfe download failed" }

    # 10. Start replacement PEER_D & repair
    $peerD = Start-Process -FilePath ".\build\Release\peer.exe" -ArgumentList "PEER_D", "127.0.0.1", "5704", "127.0.0.1", "5700" -PassThru -WindowStyle Hidden
    Start-Sleep -Milliseconds 1500
    Write-Host "`n>>> rfe repair $fileId" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" repair $fileId

    Write-Host "`n>>> rfe status & rfe transfers & rfe events" -ForegroundColor Yellow
    & ".\build\Release\rfe.exe" status
    & ".\build\Release\rfe.exe" transfers
    & ".\build\Release\rfe.exe" events 10

    Write-Host "`n================ ALL CLI & HTTP E2E CHECKS PASSED! ================" -ForegroundColor Green
}
finally {
    Write-Host "`nStopping background processes..."
    Stop-Process -Id $peerA.Id -Force -ErrorAction SilentlyContinue
    if ($peerB) { Stop-Process -Id $peerB.Id -Force -ErrorAction SilentlyContinue }
    Stop-Process -Id $peerC.Id -Force -ErrorAction SilentlyContinue
    if ($peerD) { Stop-Process -Id $peerD.Id -Force -ErrorAction SilentlyContinue }
    Stop-Process -Id $coordProc.Id -Force -ErrorAction SilentlyContinue
    Write-Host "Background processes stopped."
}
