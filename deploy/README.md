# Resilient File Engine — Portable Windows Deployment Package

This self-contained package includes precompiled native Windows x64 binaries, bundled OpenSSL 3 runtime, configuration, and helper PowerShell scripts.

## Contents

- `bin/`
  - `controller.exe`: Central Coordination Server (Control Plane + Recovery Orchestrator + HTTP Web Console) & test suite
  - `peer.exe`: Independent Windows storage peer process
  - `rfe.exe`: Client/Operator CLI
  - `libcrypto-3-x64.dll`: Bundled OpenSSL 3 SHA-256 runtime
- `config.json` & `config/config.json`: Cluster configuration (`http_port: 8080`, `auth_token: "rfe-demo-token"`, `rs_k: 2`, `rs_m: 1`, `auto_recover: true`)
- `scripts/`
  - `setup_firewall.ps1`: Opens Windows Firewall TCP ports `5000-5010` and HTTP `8080` (Run once as Admin)
  - `start_coordinator.ps1`: Starts Coordinator on PC4 (TCP `:5000`, HTTP Web Console `:8080`)
  - `start_peer_A.ps1`, `start_peer_B.ps1`, `start_peer_C.ps1`, `start_peer_D.ps1`: Start peers (pass your actual LAN `-PeerIP` and `-CoordIP`)

## Quick Start

1. Copy the `deploy/` folder to each Windows PC.
2. On every PC (Administrator PowerShell), run once:
   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\setup_firewall.ps1
   ```
3. On **PC4 (Coordinator)**:
   ```powershell
   .\bin\controller.exe coordinator 5000 0.0.0.0 8080
   ```
4. On **PC1, PC2, PC3 (Peers)** — replace `<PCx_IP>` and `<PC4_IP>` with actual IPv4 addresses:
   ```powershell
   .\bin\peer.exe PEER_A <PC1_IP> 5001 <PC4_IP> 5000
   .\bin\peer.exe PEER_B <PC2_IP> 5002 <PC4_IP> 5000
   .\bin\peer.exe PEER_C <PC3_IP> 5003 <PC4_IP> 5000
   ```
5. On your **Client PC**:
   - Browser Web Console: open `http://<PC4_IP>:8080/`
   - CLI:
     ```powershell
     .\bin\rfe.exe connect <PC4_IP>:5000 --token rfe-demo-token
     .\bin\rfe.exe peers
     .\bin\rfe.exe upload "C:\path\to\sample.bin"
     .\bin\rfe.exe files
     .\bin\rfe.exe info F001
     .\bin\rfe.exe download F001
     .\bin\rfe.exe events
     ```

See `GATE2_WINDOWS_DEPLOYMENT.md` in the repository root for full multi-laptop demonstration scenarios.
