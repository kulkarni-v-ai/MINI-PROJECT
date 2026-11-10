# Antigravity Worklog — Resilient Multi-Peer File Distribution & Recovery Engine (Gate 2)

**Platform:** Windows 11 x64, MSVC 19.51 (Visual Studio 2022/18 Community), CMake 4.4.4, C++17

---

## 1. Milestone 1 — Baseline Validation & OpenSSL Resolution (Complete)

- **Build Toolchain:** MSVC `cl.exe` (19.51.36260.0), `lib.exe`, CMake 4.4.4.
- **Dependency Audit:** Bundled self-contained OpenSSL EVP headers and generated MSVC x64 import library (`libcrypto.lib` linked to `libcrypto-3-x64.dll`) inside `third_party/openssl/`.
- **Test Result:** All Gate-1 tests T01..T07 passed (`PASS=7, FAIL=0`).

---

## 2. Milestone 2 — Dynamic Peer Registration & Protocol Extension (Complete)

### Files Changed / Added
- `models/peer.hpp`:
  - Added `PeerHealth` enum: `ONLINE`, `SUSPECT`, `OFFLINE`, `RECOVERING`, `UNKNOWN`.
  - Added `PeerWorkload` struct: `available_storage_bytes`, `active_transfers`, `transfer_capacity`, `is_busy()` separating workload from health.
  - Added `PeerInfo` extensions: `workload`, `missed_heartbeats`, `last_heartbeat`, `to_json()`, `from_json()`, preserving Gate-1 compatibility alias `PeerState`.
- `engine/communication/protocol.hpp`:
  - Added protocol message types: `PEER_REGISTER`, `PEER_REGISTER_ACK`, `PEER_HEARTBEAT`, `PEER_HEARTBEAT_ACK`, `QUERY_PEERS`, `QUERY_PEERS_ACK`.
- `engine/registry/peer_registry.hpp` & `peer_registry.cpp`:
  - Thread-safe registry tracking dynamic registrations, health state progressions (`ONLINE -> SUSPECT -> OFFLINE`), heartbeat reconciliations, workload stats, and tabular terminal formatting.
- `engine/coordinator/coordinator_service.hpp` & `coordinator_service.cpp`:
  - Coordinator TCP listener accepting registrations, periodic heartbeats, tracking workloads, and health monitor loop.
- `engine/process/process_manager.hpp`, `process_manager_windows.cpp`, `process_manager_linux.cpp`:
  - Extended `start_peer` to optionally supply coordinator IPv4 and port as command-line arguments.
- `peer/peer.cpp`:
  - Added `PeerRegistrar` background worker to dynamically register with the Coordinator via TCP, push periodic `PEER_HEARTBEAT` messages reporting available storage (`std::filesystem::space`) and atomic active transfer counts.
- `controller/controller.cpp`:
  - Added `coordinator [port] [bind_ip]` CLI command.
  - Added `peers [coord_ip] [port]` CLI command to query and format the live cluster registry table.
  - Added `T08_M2` test validating dynamic registration, storage reporting, heartbeats, and state transitions.
- `CMakeLists.txt`:
  - Added `peer_registry.cpp` and `coordinator_service.cpp` to `engine` library.
- `config.json`:
  - Added `"coordinator": { "address": "127.0.0.1", "port": 5000 }`.

### Protocol Changes
1. `PEER_REGISTER`: Peer sends identity, reachable IPv4:port, free disk storage in bytes, transfer capacity, and active transfer count.
2. `PEER_REGISTER_ACK`: Coordinator acknowledges registration with status `OK` and recommended heartbeat interval.
3. `PEER_HEARTBEAT`: Peer periodically pushes live metrics (available storage, active transfers) to Coordinator.
4. `PEER_HEARTBEAT_ACK`: Coordinator acknowledges heartbeat receipt and updates peer health state.
5. `QUERY_PEERS`: Client queries live peer registry and receives `QUERY_PEERS_ACK` with full peer health and workload JSON array.

### Build Result
- MSVC x64 Release: **0 errors, 0 warnings**.
- Executables built: `controller.exe`, `peer.exe`, and static library `engine.lib`.

### Tests Executed
1. `controller.exe test T08_M2`:
   - 3 peers dynamically registered with Coordinator via TCP over separate ports (5501, 5502, 5503).
   - Real disk capacity verified (>0 bytes, reported `101.10 GB`).
   - Active transfers verified (0 active).
   - Simulated failure of Peer M2_B: transitioned `ONLINE -> SUSPECT -> OFFLINE` within timeout threshold.
   - Verified surviving peers M2_A and M2_C remained `ONLINE`.
   - Re-launched Peer M2_B: dynamically re-registered and transitioned back to `ONLINE`.
   - **Result: `PASS T08_M2` (`PASS=1, FAIL=0`)**.
2. Gate-1 Regression Suite (`controller.exe test T01 T02 T03 T04 T05 T06 T07`):
   - **Result: `PASS=7, FAIL=0`** (100% backward compatibility maintained).

---

## 3. Four-Windows-PC LAN Configuration & Commands

For physical demonstration across four Windows PCs:
- **PC4 (Coordinator):** `192.168.1.104:5000`
- **PC1 (Peer A):** `192.168.1.101:5001`
- **PC2 (Peer B):** `192.168.1.102:5002`
- **PC3 (Peer C):** `192.168.1.103:5003`

### Commands:
```powershell
# On PC4 (Coordinator):
.\build\Release\controller.exe coordinator 5000 0.0.0.0

# On PC1 (Peer A):
.\build\Release\peer.exe A 192.168.1.101 5001 192.168.1.104 5000

# On PC2 (Peer B):
.\build\Release\peer.exe B 192.168.1.102 5002 192.168.1.104 5000

# On PC3 (Peer C):
.\build\Release\peer.exe C 192.168.1.103 5003 192.168.1.104 5000

# On PC4 or Client (Inspect cluster registry):
.\build\Release\controller.exe peers 192.168.1.104 5000
```
*(Note: physical LAN validation will be reported only when executed on hardware).*

---

## 4. Remaining Work
- Milestone 3: Multi-file central staging and CLI file catalogue (`F001`, `F002`, ...).
- Milestone 4: Adaptive chunking and systematic Reed-Solomon RS(3,2) over GF(256).
- Milestone 5: Dynamic peer selection and verified shard distribution.
- Milestone 6: Direct peer-to-client shard downloads.
- Milestone 7: Durability repair orchestration (peer-to-peer shard regeneration).
- Milestone 8: Multi-user concurrency locks and download retry handling.
- Milestone 9: Final unified `rfe` CLI and four-PC LAN demonstration.

---

## 5. Exact Next Action
## Milestone 3-8 Completion: Gate-2 Full Implementation & E2E Validation

### 1. Components Implemented

1. **E1 — Adaptive Chunking & Systematic RS(3,2) over GF(256):**
   - [`gf256.hpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/coding/gf256.hpp), [`gf256.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/coding/gf256.cpp): Galois Field GF(256) arithmetic with primitive polynomial 0x11D, exp/log lookup tables, vector mul-add, and Gaussian elimination matrix inversion.
   - [`reed_solomon.hpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/coding/reed_solomon.hpp), [`reed_solomon.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/coding/reed_solomon.cpp): Systematic Cauchy RS(N,K) encoder and decoder. Solves submatrix inversion for any surviving subset of K shards out of N.
   - [`adaptive_chunker.hpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/coding/adaptive_chunker.hpp), [`adaptive_chunker.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/coding/adaptive_chunker.cpp): Dynamic shard sizing based on file size (<1MB: 64KB, 1-10MB: 256KB, 10-100MB: 1MB, 100MB-1GB: 4MB, >1GB: 8MB). Stripe splitting (K=2 data, M=1 parity), SHA-256 calculation, and incomplete stripe padding.

2. **Catalog & Multi-File Staging:**
   - [`catalog_manager.hpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/catalog/catalog_manager.hpp), [`catalog_manager.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/catalog/catalog_manager.cpp): Central safety staging (`storage/central/`), atomic per-file manifests (`metadata/files/<FILE_ID>.json`), catalog index (`metadata/catalog.json`), counter-based ID generation (`F001`, `F002`, ...), and per-file mutex locks preventing concurrent duplicate distribution.

3. **E3 — Dynamic Peer Selection & Distribution:**
   - [`dynamic_distributor.hpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/distribution/dynamic_distributor.hpp), [`dynamic_distributor.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/distribution/dynamic_distributor.cpp): Replaced Gate-1 round-robin with multi-attribute dynamic peer selection. Evaluates health (ONLINE), free storage capacity, and active transfer workload. Distributes each RS(3,2) stripe across 3 distinct physical peers over TCP with parallel worker pools and honest shortfall reporting.

4. **E5 — Direct Peer-to-Client Download & Local RS Reconstruction:**
   - [`client_download.hpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/recovery/client_download.hpp), [`client_download.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/recovery/client_download.cpp): Client queries Coordinator for file manifest and peer endpoints. Client directly connects to storage peers over TCP to retrieve any 2 surviving shards per stripe (Coordinator never relays file data bytes!). Client verifies shard SHA-256 integrity and reconstructs the original file locally using RS(3,2). Supports explicit `--source central` fallback.

5. **E4 & E5 — Durability Repair Orchestration:**
   - [`coordinator_service.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/engine/coordinator/coordinator_service.cpp) (`repair_file`): Identifies degraded stripes missing a shard. Finds replacement Peer D. Issues `REPAIR_SHARD` command.
   - [`peer.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/peer/peer.cpp) (`REPAIR_SHARD` handler): Replacement Peer D connects directly to surviving Peers A & C over TCP, downloads surviving shards, reconstructs missing shard via local RS(3,2) decoding, verifies SHA-256 hash, and commits to local disk.

6. **Native CLI (`rfe.exe`):**
   - [`rfe.cpp`](file:///C:/Users/ariha/Documents/resilient-file-engine/cli/rfe.cpp): Full Windows native terminal CLI implementing:
     - `rfe connect [ip:port]`
     - `rfe upload <file-path>`
     - `rfe files`
     - `rfe search <filename>`
     - `rfe info <file-id>`
     - `rfe download <file-id> [--source central]`
     - `rfe peers`
     - `rfe status`
     - `rfe transfers`
     - `rfe repair <file-id>`

---

### 2. Build Verification

Build command:
```powershell
cmake --build build --config Release
```
Output:
```
  engine.vcxproj -> build\Release\engine.lib
  controller.vcxproj -> build\Release\controller.exe
  peer.vcxproj -> build\Release\peer.exe
  rfe.vcxproj -> build\Release\rfe.exe
```
0 errors, 0 warnings. OpenSSL x64 DLLs copied next to all executables.

---

### 3. Automated Test Suite Results (10/10 PASS)

Command: `.\build\Release\controller.exe test`
```
================ Gate-1 & Gate-2 test suite ================
PASS T01 - round-robin placement A=3,B=4,C=3, replication factor 2
PASS T02 - single peer failure: detected, recovered, byte-identical
PASS T03 - interrupted transfer discarded, full retry succeeded
PASS T04 - SHA-256 mismatch rejected; recovered from second source
PASS T05 - no surviving source -> UNRECOVERABLE reported, reconstruction refused
PASS T06 - reconstruction in sequence order, byte-identical to original
PASS T07 - two-peer failure: surviving chunks recovered, rest UNRECOVERABLE
PASS T08_M2 - dynamic registration, storage reporting, heartbeats & state transitions verified
PASS T09_RS - RS(3,2) systematic encoding & reconstruction verified across D0+D1, D0+P0, D1+P0 pairs
PASS T10_GATE2 - Gate-2 E2E verified: On-demand RS(3,2) distribution, direct peer retrieval, central fallback, and peer-to-peer durability repair
===================================================
PASS=10 FAIL=0
```

---

### 4. Native `rfe.exe` End-to-End Test

Script: `.\tests\test_gate2_cli.ps1`
- `rfe connect 127.0.0.1:5700` -> Connected (latency: 19 ms)
- `rfe peers` -> Formatted table with ONLINE state, storage, transfers, busy status, heartbeats
- `rfe upload` -> Staged centrally with safety backup, File ID allocated
- `rfe files` -> Catalog table with File ID, filename, size, status, stripes, SHA-256
- `rfe search cli_demo` -> File match returned
- `rfe info F033` -> Full manifest and shard map
- `rfe download F033` -> First-demand distribution triggered; 2 shards fetched directly from peers; local RS(3,2) reconstruction complete; SHA-256 match verified
- `rfe download F033 --source central` -> Central fallback verified
- `rfe status` -> Cluster overview with 3 peers ONLINE, healthy, storage summary
- `rfe transfers` -> Transfer counts and capacity limits
- `rfe repair F033` -> Verified OK

---

### 5. Physical 4-PC Deployment Guide

Target Demonstration Architecture:
- **PC1:** Peer A (IP: `192.168.1.101`, Port: `5001`)
- **PC2:** Peer B (IP: `192.168.1.102`, Port: `5002`)
- **PC3:** Peer C (IP: `192.168.1.103`, Port: `5003`)
- **PC4:** Coordinator + Central Staging (IP: `192.168.1.100`, Port: `5000`)
- **Client CLI (`rfe.exe`):** Runs on any PC (e.g. PC4 or a separate laptop) connecting over LAN TCP to `192.168.1.100:5000`.

#### Step A: Windows Firewall Rules (Run in Administrator PowerShell on each PC)
```powershell
New-NetFirewallRule -DisplayName "RFE Engine TCP" -Direction Inbound -LocalPort 5000-5010 -Protocol TCP -Action Allow
```

#### Step B: Start Coordinator on PC4
```powershell
cd C:\resilient-file-engine
.\build\Release\controller.exe coordinator 5000 0.0.0.0
```

#### Step C: Start Peers on PC1, PC2, PC3
- On PC1 (Peer A):
  ```powershell
  .\build\Release\peer.exe PEER_A 192.168.1.101 5001 192.168.1.100 5000
  ```
- On PC2 (Peer B):
  ```powershell
  .\build\Release\peer.exe PEER_B 192.168.1.102 5002 192.168.1.100 5000
  ```
- On PC3 (Peer C):
  ```powershell
  .\build\Release\peer.exe PEER_C 192.168.1.103 5003 192.168.1.100 5000
  ```

#### Step D: Run Native Terminal CLI
On any demonstration laptop:
```powershell
.\build\Release\rfe.exe connect 192.168.1.100:5000
.\build\Release\rfe.exe peers
.\build\Release\rfe.exe upload "C:\Files\sample.bin"
.\build\Release\rfe.exe files
.\build\Release\rfe.exe download F001
.\build\Release\rfe.exe download F001 --source central
```

#### Step E: Live Fault & Durability Demonstration
1. Disconnect PC2 from network (or terminate `peer.exe` on PC2).
2. Run `.\build\Release\rfe.exe peers` -> Observe Peer B transition from `ONLINE` -> `SUSPECT` -> `OFFLINE`.
3. Run `.\build\Release\rfe.exe download F001` -> Succeeds using surviving Peer A and Peer C with RS(3,2) reconstruction!
4. Connect replacement Peer D (or restart Peer B).
5. Run `.\build\Release\rfe.exe repair F001` -> Durability repair directly reconstructs the missing shard onto replacement peer via direct peer-to-peer TCP transfer without Coordinator data relay.

