# Resilient Multi-Peer File Distribution & Recovery Engine
## Gate-2 Windows 4-PC Physical Demonstration Runbook

> **Document Status:** Final Demonstration Specification  
> **Target Environment:** Windows 10 / Windows 11 x64, Native C++17, MSVC, Winsock TCP + HTTP/1.1  
> **Physical Demonstration Setup:** 4 Laptops connected over local Wi-Fi or LAN Ethernet switch  
> **Important Note:** Automated multi-process loopback testing (`T01`–`T12_AUTH_HTTP` + CLI/HTTP E2E script) passed 100% locally. Physical multi-laptop validation requires following this runbook on the live team hardware.

---

## 1. Physical Architecture & Role Assignment

The Gate-2 demonstration uses four physical Windows computers:

```
                      +------------------------------------------+
                      |                   PC4                    |
                      |   Coordinator + Central Safety Staging   |
                      |   IP: 192.168.1.100 (Example LAN IP)     |
                      |   Port: 5000 (TCP Listener)              |
                      +------------------------------------------+
                                    ^        ^        ^
                         Heartbeats |        |        | Heartbeats
                      Registrations |        |        | Registrations
                                    v        v        v
         +--------------------+  +--------------------+  +--------------------+
         |        PC1         |  |        PC2         |  |        PC3         |
         |       Peer A       |  |       Peer B       |  |       Peer C       |
         | IP: 192.168.1.101  |  | IP: 192.168.1.102  |  | IP: 192.168.1.103  |
         | Port: 5001 (TCP)   |  | Port: 5002 (TCP)   |  | Port: 5003 (TCP)   |
         | Storage: storage/  |  | Storage: storage/  |  | Storage: storage/  |
         +--------------------+  +--------------------+  +--------------------+
                   ^                        ^                        ^
                   |                        |                        |
                   +------------------------+------------------------+
                               Direct Shard Transfers
                             (TCP GET_CHUNK, 0 Relay)
                                        |
                                        v
                      +------------------------------------------+
                      |           USER CLI (rfe.exe)             |
                      |    (Runs on PC4 or any spare PC)         |
                      +------------------------------------------+
```

| Physical Device | Role | Process Executable | Default Port | Storage Location |
|---|---|---|---|---|
| **PC4** | **Coordinator + Safety Staging** | `controller.exe coordinator` | `5000` | `storage/central/`, `metadata/` |
| **PC1** | **Storage Peer A** | `peer.exe PEER_A` | `5001` | `storage/peer_PEER_A/` |
| **PC2** | **Storage Peer B** | `peer.exe PEER_B` | `5002` | `storage/peer_PEER_B/` |
| **PC3** | **Storage Peer C** | `peer.exe PEER_C` | `5003` | `storage/peer_PEER_C/` |
| **Optional PC4** | **Replacement Peer D** | `peer.exe PEER_D` | `5004` | `storage/peer_PEER_D/` |
| **Client** | **Operator Terminal** | `rfe.exe` | Ephemeral | `output/` |

---

## 2. Step 1: Network & IPv4 Configuration

Connect all four laptops to the same Wi-Fi router or Ethernet switch.

### 2.1 Discover LAN IPv4 Address on Each PC
On each PC, open PowerShell and run:
```powershell
Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notlike "*Loopback*" -and $_.IPAddress -notlike "169.254*" } | Select-Object IPAddress, InterfaceAlias
```
Note down each IP address:
- PC4 (Coordinator): `COORDINATOR_IP` (e.g. `192.168.1.100`)
- PC1 (Peer A): `PEER_A_IP` (e.g. `192.168.1.101`)
- PC2 (Peer B): `PEER_B_IP` (e.g. `192.168.1.102`)
- PC3 (Peer C): `PEER_C_IP` (e.g. `192.168.1.103`)

### 2.2 Verify Cross-PC Ping Reachability
From PC1, PC2, and PC3, ping PC4:
```powershell
Test-Connection -ComputerName <COORDINATOR_IP> -Count 2
```

---

## 3. Step 2: Windows Defender Firewall Configuration

Windows blocks incoming TCP connections by default on non-domain networks.

**Run this command once in Administrator PowerShell on ALL FOUR PCs (or run `deploy\scripts\setup_firewall.ps1` with `-ExecutionPolicy Bypass`):**
```powershell
New-NetFirewallRule -DisplayName "RFE Engine Inbound TCP (Ports 5000-5010)" `
                    -Direction Inbound `
                    -LocalPort 5000-5010 `
                    -Protocol TCP `
                    -Action Allow `
                    -Profile Any
New-NetFirewallRule -DisplayName "RFE Engine Inbound HTTP (Port 8080)" `
                    -Direction Inbound `
                    -LocalPort 8080 `
                    -Protocol TCP `
                    -Action Allow `
                    -Profile Any
```
*(Alternatively: `powershell -NoProfile -ExecutionPolicy Bypass -File .\deploy\scripts\setup_firewall.ps1`).*

---

## 4. Step 3: Starting the Demonstration Cluster

### 4.1 On PC4: Start the Coordinator Service + HTTP Web Console
Open PowerShell inside `deploy\` on PC4:
```powershell
.\bin\controller.exe coordinator 5000 0.0.0.0 8080
```
Expected output:
```
[Coordinator] HTTP Upload & Console API ACTIVE on http://127.0.0.1:8080
[Coordinator] Registration listener ACTIVE on 0.0.0.0:5000 (RS(3,2), heartbeat=500ms, timeout=1500ms, auto_recover=ON)
```
Browser Web Console is now live on `http://<COORDINATOR_IP>:8080/` (cluster auth token: `rfe-demo-token`).

### 4.2 On PC1: Start Peer A
Open PowerShell on PC1:
```powershell
.\deploy\bin\peer.exe PEER_A <PEER_A_IP> 5001 <COORDINATOR_IP> 5000
```
Example:
```powershell
.\deploy\bin\peer.exe PEER_A 192.168.1.101 5001 192.168.1.100 5000
```

### 4.3 On PC2: Start Peer B
Open PowerShell on PC2:
```powershell
.\deploy\bin\peer.exe PEER_B <PEER_B_IP> 5002 <COORDINATOR_IP> 5000
```

### 4.4 On PC3: Start Peer C
Open PowerShell on PC3:
```powershell
.\deploy\bin\peer.exe PEER_C <PEER_C_IP> 5003 <COORDINATOR_IP> 5000
```

### 4.5 Verify Dynamic Registration on PC4
On the Coordinator console on PC4, verify the three peer registrations:
```
[Coordinator] Peer PEER_A registered at 192.168.1.101:5001 (storage=100.41 GB)
[Coordinator] Peer PEER_B registered at 192.168.1.102:5002 (storage=100.41 GB)
[Coordinator] Peer PEER_C registered at 192.168.1.103:5003 (storage=100.41 GB)
```

---

## 5. Step 4: Operating the Cluster via Native CLI (`rfe.exe`)

Open a terminal on your operator machine (can be PC4 or any laptop).

### 5.1 Connect & Authenticate CLI Session with Coordinator
```powershell
.\deploy\bin\rfe.exe connect <COORDINATOR_IP>:5000 --token rfe-demo-token
```
Output:
```
[RFE] Authenticating session at 192.168.1.100:5000...
[RFE] Connected & authenticated with Coordinator at 192.168.1.100:5000 (latency: 2 ms)
[RFE] Session ID: S-863ad1d19c84 (client=cli-operator)
[RFE] Web Console: http://192.168.1.100:8080/
```

### 5.2 Verify Live Peer Registry and Workloads
```powershell
.\deploy\bin\rfe.exe peers
```
Output:
```
=================================== LIVE PEER REGISTRY ===================================
PEER ID ADDRESS:PORT          HEALTH      AVAIL STORAGE     TRANSFERS     BUSY?   LAST HEARTBEAT
------------------------------------------------------------------------------------------
PEER_A  192.168.1.101:5001    ONLINE      100.41 GB         0 / 10        NO      80 ms ago
PEER_B  192.168.1.102:5002    ONLINE      100.41 GB         0 / 10        NO      75 ms ago
PEER_C  192.168.1.103:5003    ONLINE      100.41 GB         0 / 10        NO      90 ms ago
==========================================================================================
```

### 5.3 Upload a File (Immediate Adaptive Chunk + SHA-256 + RS(3,2) + Peer Distribution)
*(You can also upload directly from any browser via the Web Console `http://<COORDINATOR_IP>:8080/` or pass `--defer` to stage centrally first.)*
```powershell
.\deploy\bin\rfe.exe upload "C:\Files\presentation.mp4"
```
Output:
```
========================== FILE UPLOAD SUCCESS ==========================
  File ID:      F001
  Filename:     presentation.mp4
  Size:         14.20 MB (14889728 bytes)
  SHA-256:      e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
  Status:       AVAILABLE
  Coding:       RS(3,2)  Stripes: 8  Shard Size: 1.00 MB
  Staged Path:  storage/central/F001_presentation.mp4
  Upload Time:  2026-11-10T12:50:05Z
=========================================================================
```

### 5.4 Check Catalogue & Shard Map
```powershell
.\deploy\bin\rfe.exe files
.\deploy\bin\rfe.exe info F001
```
Output shows `F001` with status `AVAILABLE`, `8` stripes, and every shard marked `HEALTHY` across `PEER_A`, `PEER_B`, `PEER_C`.

---

## 6. Step 5: Direct Peer Download & Central Fallback

### 6.1 Direct Peer-to-Client Download
```powershell
.\deploy\bin\rfe.exe download F001
```
What happens internally:
1. Coordinator returns manifest and live peer locations for `F001` (or runs on-demand RS(3,2) encoding if `--defer` was used during upload).
2. Client connects **directly to Peer A, Peer B, Peer C over TCP** (Coordinator never relays shard payloads).
3. Client prioritizes `HEALTHY` shards on `ONLINE` peers, fetches $\ge K$ valid shards per stripe, verifies each shard's SHA-256 (reporting any `CORRUPT`/`MISSING` shard to Coordinator), decodes locally via RS(3,2), and verifies final SHA-256 integrity.

Output:
```
[RFE] Requesting download for F001 (source: MULTI-PEER TCP DIRECT)...
=========================== DOWNLOAD COMPLETE ===========================
  File ID:         F001
  Output File:     output/presentation.mp4
  File Size:       14.20 MB
  Stripes:         8
  Shards Fetched:  16 (Retrieved directly from peers)
  Integrity Check: MATCH VERIFIED (e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855)
  Message:         Direct peer-to-client download and RS(3,2) reconstruction complete. Integrity verified.
=========================================================================
```

### 6.2 Verify Peer Storage on PC1, PC2, PC3
Open PowerShell on PC1, PC2, PC3:
```powershell
Get-ChildItem storage\peer_PEER_A\
```
Observe real `.chunk` files stored on each peer:
- PC1 stores `F001-S000-D0.chunk`, `F001-S001-P0.chunk`, etc.
- PC2 stores `F001-S000-D1.chunk`, `F001-S001-D0.chunk`, etc.
- PC3 stores `F001-S000-P0.chunk`, `F001-S001-D1.chunk`, etc.

### 6.3 Verify Central Safety Fallback
```powershell
.\deploy\bin\rfe.exe download F001 --source central
```
Output confirms file retrieved directly from Coordinator central staging safety copy without touching peers.

---

## 7. Step 6: Live Fault Tolerance & Durability Demonstration

This sequence proves the real fault tolerance of RS(3,2) erasure coding and peer-to-peer durability repair.

### 7.1 Genuine Termination of Peer B
On PC2 (Peer B):
Press `Ctrl+C` in the peer console (or disconnect its network cable).

### 7.2 Observe Failure Detection
On the CLI terminal:
```powershell
.\deploy\bin\rfe.exe peers
```
Within 1.5 seconds, Peer B transitions:
`ONLINE` $\rightarrow$ `SUSPECT` (after 1 missed heartbeat) $\rightarrow$ `OFFLINE` (after 2 missed heartbeats).

Coordinator output:
```
[Coordinator] HEALTH CHANGE: Peer PEER_B missed 1 heartbeat (520 ms) -> SUSPECT
[Coordinator] HEALTH CHANGE: Peer PEER_B missed 2 heartbeats (1040 ms) -> OFFLINE
```

### 7.3 Download with Surviving Peers (Degraded RS Reconstruction)
Run download while Peer B is completely dead:
```powershell
.\deploy\bin\rfe.exe download F001
```
Output:
```
=========================== DOWNLOAD COMPLETE ===========================
  File ID:         F001
  Output File:     output/presentation.mp4
  File Size:       14.20 MB
  Stripes:         8
  Shards Fetched:  16 (Retrieved directly from peers PEER_A and PEER_C)
  Integrity Check: MATCH VERIFIED
  Message:         Direct peer-to-client download and RS(3,2) reconstruction complete.
=========================================================================
```
*Result: Even though Peer B is offline, every stripe had 2 surviving shards ($D_0 + P_0$ or $D_1 + P_0$). The client reconstructed all data shards locally using systematic Galois Field GF(256) arithmetic!*

### 7.4 Start Replacement Peer D on PC4 (Coordinator Machine)
To demonstrate dynamic cluster rebalancing, start replacement Peer D on PC4:
```powershell
.\deploy\bin\peer.exe PEER_D <COORDINATOR_IP> 5004 <COORDINATOR_IP> 5000
```
On CLI:
```powershell
.\deploy\bin\rfe.exe peers
```
Observe:
- `PEER_A`: ONLINE
- `PEER_B`: OFFLINE
- `PEER_C`: ONLINE
- `PEER_D`: ONLINE

### 7.5 Trigger Durability Repair
From the CLI:
```powershell
.\deploy\bin\rfe.exe repair F001
```
Output:
```
[RFE] Requesting durability repair from Coordinator for F001...
Repair result: OK
[Peer-to-Peer Repair] F001-S000-D1 restored onto replacement Peer PEER_D via direct RS(3,2) reconstruction
[Peer-to-Peer Repair] F001-S001-D0 restored onto replacement Peer PEER_D via direct RS(3,2) reconstruction
...
```
*Notice:*
- Replacement Peer D connected **directly** to surviving Peer A and Peer C over TCP.
- Peer D retrieved the surviving shards, performed local RS(3,2) reconstruction, verified the SHA-256 hash, and saved the missing shards onto Peer D's local disk.
- **The Coordinator did NOT relay any chunk bytes.**

### 7.6 Verify File is Fully Protected Again
```powershell
.\deploy\bin\rfe.exe info F001
```
File status is restored to `AVAILABLE`, and shard locations now map to `PEER_A`, `PEER_C`, and `PEER_D`.

---

## 8. Summary of Validation Commands

| Command | Description |
|---|---|
| `rfe connect <IP>:<PORT> [--token <tok>]` | Authenticates session with Coordinator and displays Web Console URL |
| `rfe peers` | Shows live peer registry, storage, transfers, accurate health (`ONLINE`/`BUSY`/`SUSPECT`/`OFFLINE`) |
| `rfe upload <FILE> [--defer]` | Streams upload; chunks, SHA-256 hashes, RS-encodes & distributes across peers (`AVAILABLE`) |
| `rfe files` | Lists all files in Coordinator catalog and distribution statuses |
| `rfe search <QUERY>` | Searches remote Coordinator catalog by substring |
| `rfe info <FILE_ID>` | Displays detailed manifest, stripe policy, and per-shard states (`HEALTHY`/`UNAVAILABLE`/`CORRUPT`) |
| `rfe download <FILE_ID>` | Direct peer download with client RS reconstruction & corrupt-shard reporting |
| `rfe download <FILE_ID> --source central` | Explicit central fallback download over TCP |
| `rfe status` | Displays cluster health, peer counts, RS policy, and tracked file breakdown |
| `rfe transfers` | Displays active workloads and concurrency capacity |
| `rfe repair <FILE_ID>` | Triggers 9-step durability repair for missing/corrupt shards |
| `rfe events [N]` | Displays recent audit & recovery events from Coordinator |
