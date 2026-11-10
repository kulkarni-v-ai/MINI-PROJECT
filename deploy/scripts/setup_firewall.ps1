# ============================================================================
# setup_firewall.ps1 - Open Windows Defender Firewall for RFE TCP & HTTP traffic
# Run this script once in Administrator PowerShell on each PC
# ============================================================================
Write-Host "Configuring Windows Defender Firewall for Resilient File Engine..." -ForegroundColor Cyan

# Remove old rules if they exist
Remove-NetFirewallRule -DisplayName "RFE Engine Inbound TCP (Ports 5000-5010)" -ErrorAction SilentlyContinue
Remove-NetFirewallRule -DisplayName "RFE Engine Inbound HTTP (Port 8080)" -ErrorAction SilentlyContinue

# Add inbound allow rule for Coordinator & Peer binary TCP ports (5000-5010)
New-NetFirewallRule -DisplayName "RFE Engine Inbound TCP (Ports 5000-5010)" `
                    -Direction Inbound `
                    -LocalPort 5000-5010 `
                    -Protocol TCP `
                    -Action Allow `
                    -Profile Any | Out-Null

# Add inbound allow rule for HTTP Upload API & Web Console (Port 8080)
New-NetFirewallRule -DisplayName "RFE Engine Inbound HTTP (Port 8080)" `
                    -Direction Inbound `
                    -LocalPort 8080 `
                    -Protocol TCP `
                    -Action Allow `
                    -Profile Any | Out-Null

Write-Host "Firewall rules created successfully! TCP Ports 5000-5010 and 8080 are now open." -ForegroundColor Green
