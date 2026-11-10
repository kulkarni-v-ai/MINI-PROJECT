# Resilient Multi-Peer File Storage & Recovery Engine

A native **C++17** distributed file storage, erasure-coding, integrity verification, and automated recovery system built for Windows (Winsock2 + OpenSSL `libcrypto`) and POSIX/Linux.

Every component runs genuinely over TCP/HTTP sockets and local disk storage—no mocked peers, no stubbed crypto, and zero external web servers or runtime frameworks required.

---

## System Architecture

```
CLIENT / FILE PROVIDER                            CENTRAL COORDINATION SERVER (Control Plane & Recovery Orchestrator)
  │                                               ┌──────────────────────────────────────────────────────────────────┐
  │ 1. Upload File (HTTP :8080 or TCP :5000)      │ 1. Upload API & Session Manager  (Auth tokens, HTTP/1.1 & TCP)   │
  └──────────────────────────────────────────────>│ 2. Chunking Engine               (Adaptive sizing: 64 KB – 8 MB) │
                                                  │ 3. SHA-256 Hashing & Index       (OpenSSL EVP per-shard & file)  │
  ┌──────────────────────────────────────────────<│ 4. Reed-Solomon Encoder          (Systematic RS(k+m, k) GF(256)) │
  │ 3. Distribute Encoded Shards (TCP Push)       │ 5. Placement Scheduler           (Health, load & free space)     │
  │                                               │ 6. Metadata Store                (Catalog, shard states, events) │
  │   ┌──────────┐  ┌──────────┐  ┌──────────┐    │ 7. Peer Health Monitor           (ONLINE / SUSPECT / OFFLINE)    │
  │   │  Peer A  │  │  Peer B  │  │  Peer C  │... │ 8. Recovery Manager              (9-step repair + exp. backoff)  │
  │   │  D0  P0  │  │  D1  P1  │  │  D2  D3  │    └──────────────────────────────┬───────────────────────────────────┘
  │   └────▲─────┘  └────▲─────┘  └────▲─────┘                                   │ 2. Request File (Manifest + Peers)
  │        │             │             │         4. Read Shards on Demand         │
  │        └─────────────┴─────────────┴─────────────────────────────────────────▼
  │                                               FILE RECEIVER / DOWNLOAD CLIENT (`rfe.exe`)
  └──────────────────────────────────────────────> • Fetch >= k valid shards directly from Peers (0 Coordinator relay)
                                                   • Report MISSING / CORRUPT shards -> Reassemble -> Verify SHA-256
```

### Control Plane Modules

| # | Module | Implementation | Responsibility |
|---|---|---|---|
| 1 | **Upload API & Session Manager** | [`engine/session/session_manager.hpp`](engine/session/session_manager.hpp), [`engine/coordinator/http_server.cpp`](engine/coordinator/http_server.cpp) | Constant-time auth token validation, Win32/POSIX CSPRNG session issuance, multi-part TCP upload (`UPLOAD_INIT`/`PART`/`COMPLETE`), and embedded HTTP/1.1 REST API + Live Web Console (`GET /`, `POST /api/auth`, `POST /api/upload`, `GET /api/files`, `GET /api/peers`, `GET /api/events`). |
| 2 | **Chunking Engine** | [`engine/coding/adaptive_chunker.cpp`](engine/coding/adaptive_chunker.cpp), [`engine/chunking/chunk_manager.cpp`](engine/chunking/chunk_manager.cpp) | Adaptive data-shard sizing (`<=1 MB -> 64 KB`, `1–10 MB -> 256 KB`, `10–100 MB -> 1 MB`, `100 MB–1 GB -> 4 MB`, `>1 GB -> 8 MB`). |
| 3 | **SHA-256 Hashing & Index** | [`engine/chunking/chunk_manager.cpp`](engine/chunking/chunk_manager.cpp) | Per-shard and full-file cryptographic SHA-256 digests computed via OpenSSL `EVP_sha256()`. |
| 4 | **Reed-Solomon Encoder** | [`engine/coding/gf256.cpp`](engine/coding/gf256.cpp), [`engine/coding/reed_solomon.cpp`](engine/coding/reed_solomon.cpp) | Systematic GF(256) Cauchy Reed-Solomon erasure coding (`RS(k+m, k)`, default `RS(3,2)`: `D0, D1, P0`), tolerating arbitrary shard/peer losses up to `m` per stripe. |
| 5 | **Placement Scheduler** | [`engine/distribution/dynamic_distributor.cpp`](engine/distribution/dynamic_distributor.cpp) | Places each shard of a stripe on distinct `ONLINE`, non-`BUSY` peers ordered by active transfers and available storage. |
| 6 | **Metadata Store** | [`engine/catalog/catalog_manager.cpp`](engine/catalog/catalog_manager.cpp), [`engine/metadata/metadata_manager.cpp`](engine/metadata/metadata_manager.cpp) | Thread-safe atomic JSON persistence of file manifests (`metadata/files/`), per-shard states (`HEALTHY`, `UNAVAILABLE`, `CORRUPT`), sessions (`metadata/sessions.json`), and cluster audit trail (`metadata/events.json`). |
| 7 | **Peer Health Monitor** | [`engine/registry/peer_registry.cpp`](engine/registry/peer_registry.cpp), [`engine/coordinator/coordinator_service.cpp`](engine/coordinator/coordinator_service.cpp) | Background monitor tracking peer heartbeats (`ONLINE` -> `SUSPECT` -> `OFFLINE` / `BUSY`) and triggering Step 1 & 2 recovery transitions. |
| 8 | **Recovery Manager** | [`engine/coordinator/coordinator_service.cpp`](engine/coordinator/coordinator_service.cpp), [`peer/peer.cpp`](peer/peer.cpp) | Executes the 9-step recovery pipeline directly peer-to-peer with exponential backoff (`2s -> 4s -> 8s -> 15s`) and immediate wakeup when peers return/join. |

### 9-Step Recovery Process

1. **Trigger:** Coordinator health monitor detects peer timeout (`OFFLINE`) **OR** client/peer reports missing or SHA-256 hash-mismatched shard (`REPORT_SHARD_ISSUE`).
2. **Mark Shard:** Affected shard state is updated to `UNAVAILABLE` or `CORRUPT` in the Metadata Store.
3. **Alternate Replica Check:** If an intact verified replica exists on another `ONLINE` peer -> **Step 4:** Target peer copies the replica directly via `COPY_SHARD`, verifies SHA-256, and writes it atomically -> **Step 8 & 9**.
4. **Stripe Survival Check:** Otherwise collect surviving `HEALTHY` shards in the same stripe from `ONLINE` peers.
5. **Enough Valid Shards (`>= k`)?**
   - **NO:** Transition file to **`RECOVERY_PENDING`** and retry asynchronously with exponential backoff (`2s -> 4s -> 8s -> 15s`), waking immediately when peers rejoin or recover.
   - **YES:** Target healthy peer connects directly to surviving peers (`GET_CHUNK`), performs **Step 7** GF(256) Reed-Solomon decoding + SHA-256 verification inside `REPAIR_SHARD`, writes the repaired shard atomically (**Step 8**), and updates metadata back to `HEALTHY` / `AVAILABLE` (**Step 9**).

---

## Prerequisites & Build

### Requirements
- **Windows 10 / 11 x64:** Visual Studio 2022 (or Build Tools 2022) with *Desktop development with C++* and CMake >= 3.16. OpenSSL 3 runtime & `libcrypto` headers/import library are bundled in `third_party/openssl/`.
- **Linux / macOS:** GCC 9+ or Clang 10+ (C++17), CMake >= 3.16, `libssl-dev` / `openssl`.

### Build Release Binaries (Windows PowerShell)

```powershell
cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release --parallel
```

> If `cmake` is not on your global `PATH`, invoke Visual Studio's CMake directly:
> `& "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe" -S . -B build -G "Visual Studio 17 2022" -A x64`

This builds:
- `build\Release\controller.exe` — Coordinator control plane, HTTP API/Web Console & test runner
- `build\Release\peer.exe` — Independent storage peer daemon
- `build\Release\rfe.exe` — Operator/Client CLI
- `build\Release\libcrypto-3-x64.dll` — Copied automatically next to all binaries

Updated portable binaries are also pre-populated under `deploy\bin\`.

### Build (Linux / macOS)

```bash
cmake -S . -B build
cmake --build build -j
```

---

## Verification & Automated Tests

Run the built-in 12-scenario multi-process integration suite (`T01` through `T12_AUTH_HTTP`):

```powershell
.\build\Release\controller.exe test
```

| Suite | Coverage |
|---|---|
| `T01`–`T07` | Gate-1 baseline engines: chunking, round-robin replication, single/multi-peer failure detection, interrupted transfer rejection, SHA-256 mismatch rejection, unrecoverable reporting, sequence reconstruction. |
| `T08_M2` | Dynamic peer registration (`PEER_REGISTER`), storage reporting, heartbeats, `ONLINE -> SUSPECT -> OFFLINE -> ONLINE`. |
| `T09_RS` | Adaptive sizing & Systematic GF(256) `RS(3,2)` reconstruction across all shard combinations (`D0+D1`, `D0+P0`, `D1+P0`) and `< K` refusal. |
| `T10_GATE2` | Gate-2 end-to-end distribution, direct peer retrieval, central fallback, and P2P RS repair onto replacement Peer `G2_D`. |
| `T11_RECOVERY_FLOW` | TCP authentication (`AUTH_ERROR` / `SESSION_AUTH`), streamed multi-part upload (`UPLOAD_INIT/PART/COMPLETE`), corrupt-shard detection & `REPORT_SHARD_ISSUE` auto-repair, and `< K` `RECOVERY_PENDING` exponential-backoff recovery upon peer restart. |
| `T12_AUTH_HTTP` | Embedded HTTP/1.1 Upload API (`POST /api/upload`), `401 Unauthorized` enforcement, Bearer token authentication (`POST /api/auth`), Web Console (`GET /`), and cross-protocol `ClientDownload` verification. |

Also run the PowerShell CLI & HTTP end-to-end validation script:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\test_gate2_cli.ps1
```

---

## Step-by-Step Run Guide (Tomorrow's Demo)

You can run the demo either on **one Windows PC** (multiple terminals on `127.0.0.1`) or across **4 Windows PCs over Wi-Fi / LAN**.

### Option A: Single-PC Quick Live Demo

Open **5 PowerShell terminals** in the repository root (or inside `deploy\`) using `.\build\Release\...` (or `.\bin\...`):

1. **Terminal 1 — Central Coordination Server (TCP :5000 + HTTP Web Console :8080)**
   ```powershell
   .\build\Release\controller.exe reset
   .\build\Release\controller.exe coordinator 5000 0.0.0.0 8080
   ```
   Open **`http://127.0.0.1:8080/`** in your browser to view live peers, file manifests, stripe shard states, and audit events (token: `rfe-demo-token`).

2. **Terminal 2, 3, 4 — Launch 3 Independent Storage Peers**
   ```powershell
   # Terminal 2 (Peer A)
   .\build\Release\peer.exe PEER_A 127.0.0.1 5001 127.0.0.1 5000

   # Terminal 3 (Peer B)
   .\build\Release\peer.exe PEER_B 127.0.0.1 5002 127.0.0.1 5000

   # Terminal 4 (Peer C)
   .\build\Release\peer.exe PEER_C 127.0.0.1 5003 127.0.0.1 5000
   ```

3. **Terminal 5 — Operate Client CLI (`rfe.exe`)**
   ```powershell
   # Authenticate session
   .\build\Release\rfe.exe connect 127.0.0.1:5000 --token rfe-demo-token

   # Inspect registered peers, free disk space & heartbeat health
   .\build\Release\rfe.exe peers

   # Create a sample 200 KB test file (or pick any real PDF/image/video/zip)
   New-Item -ItemType Directory -Force input | Out-Null
   $b = New-Object byte[] 200000; (New-Object System.Random 7).NextBytes($b)
   [System.IO.File]::WriteAllBytes("$PWD\input\demo.bin", $b)

   # Stream upload -> Adaptive Chunk -> SHA-256 -> RS(3,2) -> Dynamic Placement
   .\build\Release\rfe.exe upload .\input\demo.bin

   # View file catalog & exact shard-to-peer stripe mapping
   .\build\Release\rfe.exe files
   .\build\Release\rfe.exe info F001

   # Download directly from storage peers & verify SHA-256
   .\build\Release\rfe.exe download F001
   ```

4. **Simulate Live Failures & Watch 9-Step Auto-Recovery**
   - **Test 1 — Peer Crash + Degraded RS Read + Replacement Auto-Repair:**
     - Press `Ctrl+C` in **Terminal 3 (Peer B)**.
     - Within ~1.5 seconds, `.\build\Release\rfe.exe peers` shows `PEER_B` -> `OFFLINE`, and `.\build\Release\rfe.exe events` logs `PEER_OFFLINE`, `SHARD_UNAVAILABLE`, `REPAIR_PENDING`.
     - Run `.\build\Release\rfe.exe download F001` — succeeds directly from `PEER_A` + `PEER_C` via RS(3,2) GF(256) reconstruction!
     - Launch replacement Peer D in a new terminal:
       ```powershell
       .\build\Release\peer.exe PEER_D 127.0.0.1 5004 127.0.0.1 5000
       ```
     - Coordinator wakes immediately, Peer D fetches surviving shards from `PEER_A` & `PEER_C`, RS-decodes, verifies SHA-256, and restores `F001` to `AVAILABLE`! Verify with `.\build\Release\rfe.exe info F001` and `.\build\Release\rfe.exe events`.
   - **Test 2 — On-Disk Bit-Rot / Corrupt Shard Detection:**
     - Overwrite a few bytes of any `.chunk` file under `storage\peer_PEER_A\chunks\`.
     - Run `.\build\Release\rfe.exe download F001`.
     - Client detects the SHA-256 mismatch, reports `CORRUPT` to Coordinator (`SHARD_CORRUPT`), still reconstructs the clean file via RS parity, and Coordinator auto-repairs the damaged shard!
   - **Test 3 — Multi-Peer Outage (`< K` shards) -> `RECOVERY_PENDING` -> Automatic Comeback:**
     - Stop 2 of the 3 active peers (`Ctrl+C`) so only 1 peer survives.
     - `.\build\Release\rfe.exe files` shows `RECOVERY_PENDING` (cannot reconstruct with `< 2` shards).
     - Restart a peer -> backoff recovery wakes automatically and brings the file back to `AVAILABLE`!

---

### Option B: 4-PC Windows LAN Demo

Copy the self-contained **`deploy\`** folder to all 4 Windows laptops on the same Wi-Fi/LAN.

1. **On all 4 laptops (Administrator PowerShell — once):**
   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File .\deploy\scripts\setup_firewall.ps1
   ```
   This opens TCP `5000-5010` and HTTP `8080`.
2. **Find each PC's IPv4 address:**
   ```powershell
   (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notlike "*Loopback*" -and $_.IPAddress -notlike "169.254*" }).IPAddress
   ```
3. **Start the cluster (pass actual LAN IPs):**
   - **PC4 (Coordinator):** `cd deploy; .\bin\controller.exe coordinator 5000 0.0.0.0 8080` (or `powershell -ExecutionPolicy Bypass -File .\scripts\start_coordinator.ps1`)
   - **PC1 (Peer A):** `cd deploy; .\bin\peer.exe PEER_A <PC1_IP> 5001 <PC4_IP> 5000`
   - **PC2 (Peer B):** `cd deploy; .\bin\peer.exe PEER_B <PC2_IP> 5002 <PC4_IP> 5000`
   - **PC3 (Peer C):** `cd deploy; .\bin\peer.exe PEER_C <PC3_IP> 5003 <PC4_IP> 5000`
4. **Client / Demo Operator (any PC):**
   - Terminal CLI: `.\bin\rfe.exe connect <PC4_IP>:5000 --token rfe-demo-token`
   - Browser Web Console: open **`http://<PC4_IP>:8080/`**

See [`GATE2_WINDOWS_DEPLOYMENT.md`](GATE2_WINDOWS_DEPLOYMENT.md) for the complete 4-laptop step-by-step script.

---

## Configuration (`config.json` / `deploy/config.json`)

```json
{
  "coordinator": { "address": "127.0.0.1", "port": 5000 },
  "http_port": 8080,
  "auth_token": "rfe-demo-token",
  "require_auth": true,
  "auto_recover": true,
  "rs_k": 2,
  "rs_m": 1,
  "heartbeat_interval_ms": 500,
  "heartbeat_timeout_ms": 1000,
  "failure_threshold": 2
}
```

## HTTP REST API Reference (Port `8080`)

| Endpoint | Auth | Description |
|---|---|---|
| `GET /` | Public | Embedded live Web Console (upload, peer monitor, stripe map, audit log) |
| `POST /api/auth` | JSON `{"token": "..."}` | Issue CSPRNG Bearer `session_token` |
| `POST /api/upload` | `Authorization: Bearer <tok>` | Raw binary (`X-Filename`) or `multipart/form-data` upload -> chunk -> RS -> place |
| `GET /api/status` | Public read | Cluster health, peer counts, RS policy, active sessions |
| `GET /api/peers` | Public read | Live peer list with workload & heartbeat age |
| `GET /api/files` / `GET /api/files/<id>` | Public read | File catalog and per-shard status map |
| `GET /api/events` | Public read | Cluster audit & recovery event stream |
| `POST /api/repair/<id>` | `Authorization: Bearer <tok>` | Trigger synchronous 9-step repair pass |
| `GET /api/download/<id>` | `Authorization` or `?token=` | Download safety copy from Coordinator central staging |
