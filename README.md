# Resilient Multi-Peer File Storage & Recovery Engine
### *A Hybrid Adaptive Architecture for Resilient Multi-Peer File Distribution and Recovery*

A native **C++17** distributed file storage, adaptive chunking, systematic Reed-Solomon erasure coding, peer-aware placement, cryptographic integrity verification, and automated recovery system built for **Windows (Winsock2 + BCrypt + OpenSSL 3 `libcrypto`)** and **Linux / POSIX**.

Every component executes genuinely over real TCP/HTTP sockets, real multi-process daemons, and real disk I/O—**zero mocked peers, zero stubbed hashing, and zero external web servers or runtime dependencies required**.

---

## Table of Contents

1. [System Architecture Overview](#system-architecture-overview)
2. [Overall End-to-End Workflow](#overall-end-to-end-workflow)
3. [Algorithms & Detailed Workflows](#algorithms--detailed-workflows)
   - [4.1 DACA — Dynamic Adaptive Chunking Algorithm](#41-daca--dynamic-adaptive-chunking-algorithm)
   - [4.2 Systematic GF(256) Cauchy Reed-Solomon Coding](#42-systematic-gf256-cauchy-reed-solomon-coding)
   - [4.3 PASPA — Peer-Aware Shard Placement Algorithm](#43-paspa--peer-aware-shard-placement-algorithm)
   - [4.4 ARMA — Adaptive Redundancy Management Algorithm](#44-arma--adaptive-redundancy-management-algorithm)
   - [4.5 HPFD — Heartbeat-Based Peer Failure Detection](#45-hpfd--heartbeat-based-peer-failure-detection)
   - [4.6 HRERA — Hybrid Replica-and-Erasure Recovery Algorithm (9-Step Flowchart)](#46-hrera--hybrid-replica-and-erasure-recovery-algorithm-9-step-flowchart)
   - [4.7 SHA-256 Cryptographic Integrity Verification](#47-sha-256-cryptographic-integrity-verification)
   - [4.8 MGFRA — Metadata-Guided File Reconstruction Algorithm](#48-mgfra--metadata-guided-file-reconstruction-algorithm)
4. [Research-Based Comparison](#research-based-comparison)
5. [Control Plane & Storage Modules](#control-plane--storage-modules)
6. [Scratch System Setup, Dependencies & Build](#scratch-system-setup-dependencies--build)
7. [Automated Verification & Test Suite (`T01`–`T13`)](#automated-verification--test-suite-t01t13)
8. [How to Run the Live System (Step-by-Step Guide)](#how-to-run-the-live-system-step-by-step-guide)
   - [Fastest Launch: One-Command Demo (`run_demo.ps1` / `run_demo.sh`)](#fastest-launch-one-command-demo)
   - [Option A: Manual Single-PC Multi-Terminal Demo](#option-a-manual-single-pc-multi-terminal-demo)
   - [Option B: 4-PC Physical Wi-Fi / LAN Demo](#option-b-4-pc-physical-wi-fi--lan-demo)
9. [Failure, Corruption & Auto-Recovery Walkthrough](#failure-corruption--auto-recovery-walkthrough)
10. [Configuration & HTTP REST API Reference](#configuration--http-rest-api-reference)

---

## System Architecture Overview

```
+-----------------------+       1. Upload File (HTTP :8080 / Streamed TCP :5000)       +---------------------------------------------------------------------+
| CLIENT / FILE PROVIDER|------------------------------------------------------------->|                CENTRAL COORDINATION SERVER                          |
|  - Browser / CLI rfe  |                                                              |            (Control Plane & Recovery Orchestrator)                |
+-----------------------+                                                              |                                                                     |
                                                                                       |  [1] Upload API & Session Manager   [5] PASPA & ARMA Scheduler      |
                                                                                       |      (CSPRNG Tokens, HTTP/TCP)          (Suitability + Hot Replicas)|
       3. Distribute Encoded Shards + Hot Replicas (Verified TCP STORE_CHUNK)          |  [2] DACA Chunking Engine           [6] Metadata Store              |
  +------------------------------------------------------------------------------------+      (Adaptive Boundaries)              (Atomic Catalog & Events)   |
  |                                                                                    |  [3] SHA-256 Hashing & Index        [7] HPFD Peer Health Monitor    |
  |    +-------------+    +-------------+    +-------------+    +-------------+        |      (OpenSSL EVP_sha256)             (ONLINE/SUSPECT/OFFLINE)    |
  |    |   Peer A    |    |   Peer B    |    |   Peer C    |    |   Peer D    |  ...   |  [4] GF(256) Reed-Solomon Encoder   [8] HRERA Recovery Manager      |
  |    |  D0    P1   |    |  D1    P0   |    |  P0    D2   |    |  D0(repl)   |        |      (Systematic RS(k+m, k))          (Replica/RS + Backoff)    |
  |    +------^------+    +------^------+    +------^------+    +------^------+        +----------------------------------+----------------------------------+
  |           |                  |                  |                  |                                                  |
  |           +------------------+------------------+------------------+                                                  | 2. Request Manifest & Peer Map
  |                         4. Direct Peer-to-Client Shard Read (0 Coordinator Relay)                                     |
  |                                                                     \                                                 v
  |                                                                      +-----------> +----------------------------------+
  |                                                                                    |  FILE RECEIVER / DOWNLOAD CLIENT |
  |                                                                                    |  - Fetch >= k verified shards    |
  +---(HRERA Recovery Manager executes Step 1-9 peer-to-peer on timeout/corruption)--->|  - Report CORRUPT / MISSING      |
                                                                                       |  - MGFRA Reassemble & SHA-256    |
                                                                                       +----------------------------------+
```

---

## Overall End-to-End Workflow

```
INPUT FILE (Client / Web Console / rfe CLI)
    │
    ▼
[1] Upload API & Session Manager (`SESSION_AUTH` / Bearer token, streamed `UPLOAD_INIT/PART/COMPLETE` or HTTP `/api/upload`)
    │  Stages verified central backup -> allocates File ID (`F001`, `F002`, ...)
    ▼
[2] DACA: Dynamic Adaptive Chunking Algorithm (`AdaptiveChunker`)
    │  Analyses file size + online peer available storage -> computes shard boundary (`4 KB` – `8 MB`)
    ▼
[3] SHA-256 Hashing & Metadata Index (`ChunkManager` + `CatalogManager`)
    │  Computes per-shard & whole-file SHA-256 digests (`EVP_sha256`) + exact `original_data_length` / `padding_length`
    ▼
[4] Reed-Solomon Erasure Coding (`ReedSolomon` over `GF(256)`)
    │  Stripes k data shards (`D0..Dk-1`) -> Cauchy parity matrix -> encodes m parity shards (`P0..Pm-1`)
    ▼
[5] PASPA: Peer-Aware Shard Placement (`DynamicDistributor`)
    │  Weighted suitability scoring (health, free storage, transfer load, balance, host IP anti-affinity) -> TCP push
    ▼
[6] ARMA: Adaptive Redundancy Management (`DynamicDistributor`)
    │  When healthy disjoint spare peers exist, selectively pushes hot direct replica(s) of primary DATA shards
    ▼
DISTRIBUTED PEER NETWORK (`Peer A`, `Peer B`, `Peer C`, `Peer D`, ...)
    │
    ├──► [7] HPFD Failure Detection (`PeerRegistry::evaluate_health_detailed` + read-time `REPORT_SHARD_ISSUE`)
    │         ONLINE ──(1 timeout)──► SUSPECT ──(threshold timeouts)──► OFFLINE
    │
    └──► [8] HRERA 9-Step Recovery (`CoordinatorService::repair_file` + `recovery_worker_loop`)
              ├── Replica Available? YES ──► Direct P2P Copy (`COPY_SHARD`) + SHA-256 ──► `REPAIR_REPLICA_OK`
              └── Replica Available? NO  ──► >= k shards? YES ──► P2P RS Decode (`REPAIR_SHARD`) ──► `REPAIR_RS_OK`
                                                          NO  ──► `RECOVERY_PENDING` (Exponential backoff `2s->4s->8s->15s`)
    │
    ▼
[9] MGFRA File Reconstruction (`AdaptiveChunker::reconstruct_file` + `ClientDownload::download`)
    │  Groups by stripe ordered strictly by numeric `sequence` (safe at >=1000 stripes), verifies SHA-256, removes padding
    ▼
ORIGINAL FILE (Byte-identical & SHA-256 Verified)
```

---

## Algorithms & Detailed Workflows

### 4.1 DACA — Dynamic Adaptive Chunking Algorithm

- **Implementation:** [`engine/coding/adaptive_chunker.hpp`](engine/coding/adaptive_chunker.hpp), [`engine/coding/adaptive_chunker.cpp`](engine/coding/adaptive_chunker.cpp) (`AdaptiveChunker::calculate_shard_size`, `AdaptiveChunker::encode_file`)
- **Problem Solved:** Fixed global chunk sizes waste metadata/socket overhead on tiny chunks for huge files, or suffer poor parallelism and failed fits on huge chunks for small/constrained peers.

#### Workflow
```
INPUT FILE + ONLINE PEER WORKLOADS
    │
    ▼
[1] Analyse File Size Tier
      • File <= 1 MB       ──► 64 KB data shard
      • 1 MB – 10 MB       ──► 256 KB data shard
      • 10 MB – 100 MB     ──► 1 MB data shard
      • 100 MB – 1 GB      ──► 4 MB data shard
      • File > 1 GB        ──► 8 MB data shard
    │
    ▼
[2] Resource-Constraint Check (`min_peer_avail_bytes` from Coordinator PeerRegistry)
      • Compute storage headroom budget = max(min_size_bytes [4 KB], min_peer_avail_bytes / 4)
      • If chosen shard tier exceeds headroom budget, step down dynamically through
        supported tiers (`8 MB -> 4 MB -> 1 MB -> 256 KB -> 64 KB -> 16 KB -> 4 KB`)
    │
    ▼
[3] Stripe & Logical Normalization
      • Divide byte stream into stripes of `k` logical slices of `shard_size` bytes
      • Last incomplete slice(s) zero-padded internally to `shard_size`
      • Record exact `original_data_length` and `padding_length` per shard
    │
    ▼
[4] Metadata & Audit Emission
      • Generate `ShardMeta` (`file_id`, `chunk_id`, `stripe_id`, `sequence`, `sha256`)
      • Coordinator logs `DACA_CHUNKED` audit event
```

---

### 4.2 Systematic GF(256) Cauchy Reed-Solomon Coding

- **Implementation:** [`engine/coding/gf256.hpp`](engine/coding/gf256.hpp), [`engine/coding/gf256.cpp`](engine/coding/gf256.cpp), [`engine/coding/reed_solomon.hpp`](engine/coding/reed_solomon.hpp), [`engine/coding/reed_solomon.cpp`](engine/coding/reed_solomon.cpp)
- **Mathematical Design:**
  - Operates over Galois Field $\text{GF}(2^8)$ with primitive polynomial $x^8 + x^4 + x^3 + x^2 + 1$ (`0x11D`), precomputed 512-entry anti-log / log lookup tables for constant-time field multiplication & division, and zero-branch XOR addition (`gf.mul_add_row`).
  - Constructs a $(k + m) \times k$ systematic encoding matrix where:
    - Top $k$ rows form the **Identity matrix** $I_k$ (data shards `D0..Dk-1` stay unmodified and readable directly without parity math when all $k$ data shards are present).
    - Bottom $m$ rows form a **Cauchy matrix** $C_{i,j} = \frac{1}{(k + i) \oplus j}$ over $\text{GF}(256)$, guaranteeing **every $k \times k$ submatrix is invertible (non-singular)**.
  - Any $k$ surviving shards out of $k + m$ (data or parity in any mix) reconstruct all missing data & parity shards via Gauss-Jordan matrix inversion in $\text{GF}(256)$.

```
Data Shards: [ D0 | D1 | ... | D(k-1) ]
                  │
                  ▼   Multiply by Cauchy Matrix C[i,j] = 1 / ((k+i) ^ j) in GF(256)
                  │
Encoded Stripe: [ D0 | D1 | ... | D(k-1) | P0 | ... | P(m-1) ]   (RS(k+m, k), e.g. RS(3,2) or RS(6,4))
```

---

### 4.3 PASPA — Peer-Aware Shard Placement Algorithm

- **Implementation:** [`engine/distribution/dynamic_distributor.hpp`](engine/distribution/dynamic_distributor.hpp), [`engine/distribution/dynamic_distributor.cpp`](engine/distribution/dynamic_distributor.cpp) (`DynamicDistributor::distribute_file`)
- **Problem Solved:** Naive round-robin placement ignores peer congestion, low disk space, missed heartbeats, cumulative load skew, and correlated host failures.

#### Suitability Formula & Workflow
```
NEW SHARD IN STRIPE S
    │
    ▼
[1] Identify Active Peers (`state == ONLINE`)
    │
    ▼
[2] Filter Ineligible Candidates
      • Exclude BUSY peers (`active_transfers >= transfer_capacity`) unless cluster saturated
      • Exclude peers with insufficient storage (`virtual_avail < shard.size`)
      • Exclude peers already holding a shard or replica in the SAME stripe (anti-correlation)
    │
    ▼
[3] Compute Multi-Factor PASPA Suitability Score
      Score(p) = Health(0..30)               = max(0, 30 - 10 * missed_heartbeats)
               + StorageHeadroom(0..25)       = 25 * (virtual_avail[p] / max_avail)
               + TransferHeadroom(0..25)      = 25 * (1 - active_transfers / transfer_capacity)
               + CumulativeBalance(0..20)     = 20 / (1 + assigned_shard_count[p])
               + FailureDomainBonus(0 or +15) = +15 if p.address not yet used in stripe S
    │
    ▼
[4] Select Highest-Scoring Peer -> Transfer via TCP `STORE_CHUNK`
    │
    ▼
[5] Receiver Verifies Payload SHA-256 -> Returns `STORE_ACK`
      • On failure, automatically falls through to next highest-ranked eligible candidate
    │
    ▼
[6] Update Virtual Headroom & Commit Metadata (`shard.peers`, `status = HEALTHY`)
```

---

### 4.4 ARMA — Adaptive Redundancy Management Algorithm

- **Implementation:** [`engine/distribution/dynamic_distributor.cpp`](engine/distribution/dynamic_distributor.cpp), [`engine/catalog/catalog_manager.cpp`](engine/catalog/catalog_manager.cpp), [`engine/coordinator/coordinator_service.cpp`](engine/coordinator/coordinator_service.cpp)
- **Configurable Policy (`config.json`):** `"adaptive_replicas": true`, `"hot_replica_count": 1`
- **Problem Solved:** Pure replication costs $2\times$–$3\times$ storage overhead; pure Reed-Solomon has minimal storage overhead ($1.5\times$ for `RS(3,2)`) but forces $k$-peer reads & matrix decoding even on single-shard failures or bit-rot.

#### Workflow
1. **Primary RS Placement:** PASPA places the $k + m$ erasure shards across $k + m$ distinct peers per stripe.
2. **Disjoint Spare Capacity Check:** After placing a stripe, ARMA inspects whether healthy peers exist that hold **no shard in the current stripe** (for example, in a 4-peer or 5-peer cluster running `RS(3,2)` where 3 peers hold `D0, D1, P0` and 1–2 spare peers exist).
3. **Selective Hot Replication:** If disjoint spare capacity is available, ARMA uses PASPA scoring to push verified direct hot replicas of up to `hot_replica_count` primary `DATA` shards onto disjoint peer(s), recording `shard.peers = {primary, replica}` (displayed with **`ARMA REPL`** badge in the Web Console).
4. **Zero Penalty on Minimal Clusters:** In a 3-peer `RS(3,2)` deployment, all 3 peers are occupied per stripe, so ARMA places 0 extra replicas and maintains exact $1.5\times$ RS storage efficiency.
5. **Degradation & Auto-Protection:** When HPFD detects any peer in `shard.peers` going `OFFLINE`, or `ClientDownload` detects hash corruption, ARMA + HRERA prioritize restoring lost replica redundancy or scheduling recovery.

---

### 4.5 HPFD — Heartbeat-Based Peer Failure Detection

- **Implementation:** [`peer/peer.cpp`](peer/peer.cpp) (`PeerRegistrar`), [`engine/registry/peer_registry.hpp`](engine/registry/peer_registry.hpp), [`engine/registry/peer_registry.cpp`](engine/registry/peer_registry.cpp), [`engine/coordinator/coordinator_service.cpp`](engine/coordinator/coordinator_service.cpp) (`health_monitor_loop`)
- **Timing Rule:**
  - Peers push `PEER_HEARTBEAT` over TCP every `heartbeat_interval_ms` (default `500 ms`), including live `available_storage_bytes`, `active_transfers`, and `transfer_capacity`.
  - Background `health_monitor_loop()` evaluates elapsed time since last heartbeat:
    - $\text{elapsed} < \text{timeout\_ms}$: **`ONLINE`** (or **`BUSY`** if `active_transfers >= transfer_capacity`)
    - $\text{elapsed} \ge \text{timeout\_ms}$: **`SUSPECT`** (`PEER_SUSPECT` event logged)
    - $\text{elapsed} \ge \text{timeout\_ms} \times \text{failure\_threshold}$: **`OFFLINE`** (`PEER_OFFLINE` event logged -> marks affected shards & schedules HRERA)
    - Returning peer whose heartbeat/register resumes: **`RECOVERED -> ONLINE`** -> Coordinator immediately runs `reconcile_returning_peer()` using `VERIFY_CHUNK` + SHA-256 to validate surviving on-disk chunks and wakes `RECOVERY_PENDING` workers.

```
[PEER ONLINE] ──(1 missed HB >= timeout_ms)──► [SUSPECT] ──(failure_threshold missed HBs)──► [OFFLINE]
     ▲                                              │                                             │
     │                                              ▼                                             ▼
     └──────────(Heartbeat / Register Resumes)◄── [Reconcile Disk Chunks (VERIFY_CHUNK + SHA-256)] ◄── Trigger HRERA
```

---

### 4.6 HRERA — Hybrid Replica-and-Erasure Recovery Algorithm (9-Step Flowchart)

- **Implementation:** [`engine/coordinator/coordinator_service.cpp`](engine/coordinator/coordinator_service.cpp) (`CoordinatorService::repair_file`, `recovery_worker_loop`), [`peer/peer.cpp`](peer/peer.cpp) (`COPY_SHARD`, `REPAIR_SHARD`, `VERIFY_CHUNK`), [`engine/recovery/client_download.cpp`](engine/recovery/client_download.cpp) (`report_shard_issue`)

#### Complete 9-Step Decision Flowchart (Zero Coordinator Data Relay)
```
  [1. Trigger: HPFD Peer Timeout (OFFLINE)  OR  Client/Peer Missing/SHA-256 Hash-Mismatch (REPORT_SHARD_ISSUE)]
          │
          ▼
  [2. Mark Shard Unavailable / Corrupt in Metadata Store (`UNAVAILABLE` / `CORRUPT`)]
          │
          ▼
  [Missing Shards Priority Check] ──(For shards with 0 surviving online copies first, then degraded replicas)──┐
          │                                                                                                  │
          ▼                                                                                                  │
  [3. Valid Verified Replica Available on an Alternate ONLINE Peer?]                                         │
      ├── YES ──► [4. Fetch Replica Directly Peer-to-Peer (`COPY_SHARD`) + Verify SHA-256]                  │
      │                 │                                                                                    │
      │                 ├── Free disjoint target peer exists (or corrupt online peer healed in-place)        │
      │                 │         └──► [8. Atomic Write `.tmp -> .chunk`] ──► [9. Update Metadata & Log `REPAIR_REPLICA_OK`]
      │                 └── No extra target peer (but verified copy survives)
      │                           └──► Prune dead holder from `sh.peers`, keep healthy copy (`AVAILABLE`)
      │
      └── NO  ──► [5. Collect Valid Surviving Shards from Same Stripe across ONLINE Peers]
                        │
                        ▼
                  [6. Enough Valid Shards (>= k)?]
                      ├── YES ──► Target Healthy Peer fetches >=k shards directly (`GET_CHUNK`)
                      │             │
                      │             ▼
                      │           [7. Decode Missing Shard(s) via GF(256) RS(`REPAIR_SHARD`) + Verify SHA-256]
                      │             │
                      │             ▼
                      │           [8. Write Repaired Shard Atomically] ──► [9. Update Metadata & Log `REPAIR_RS_OK`]
                      │
                      └── NO  ──► [`RECOVERY_PENDING`] Retry asynchronously with Exponential Backoff
                                  (`2s -> 4s -> 8s -> 15s`, immediate wakeup on `PEER_REGISTER` / `PEER_HEARTBEAT`)
```

---

### 4.7 SHA-256 Cryptographic Integrity Verification

- **Implementation:** [`engine/chunking/chunk_manager.cpp`](engine/chunking/chunk_manager.cpp) (`ChunkManager::sha256_of_bytes`, `sha256_of_file` via OpenSSL `EVP_sha256()`)
- **Enforced at Every Boundary:**
  1. **Encoding:** Computes SHA-256 of every staged `DATA` and `PARITY` shard plus the complete original file.
  2. **Storage (`STORE_CHUNK`):** Target peer computes payload digest before atomic rename and rejects mismatched or truncated transfers.
  3. **Direct Replica Copy (`COPY_SHARD`):** Receiving peer checks exact expected byte length and SHA-256 before committing `.chunk`.
  4. **P2P Repair (`REPAIR_SHARD`):** Target peer verifies SHA-256 of every incoming source shard **and** recomputes/verifies SHA-256 of the RS-reconstructed shard before saving.
  5. **Peer Reconciliation (`VERIFY_CHUNK`):** Returning peers hash local `.chunk` files before re-joining active shard maps.
  6. **Client Download (`ClientDownload`):** Verifies each downloaded shard against `sm.sha256` (immediately reporting bit-rot to Coordinator via `REPORT_SHARD_ISSUE`), verifies reconstructed data shards inside MGFRA, and validates the assembled output file against the original file SHA-256.

---

### 4.8 MGFRA — Metadata-Guided File Reconstruction Algorithm

- **Implementation:** [`engine/coding/adaptive_chunker.cpp`](engine/coding/adaptive_chunker.cpp) (`AdaptiveChunker::reconstruct_file`), [`engine/recovery/client_download.cpp`](engine/recovery/client_download.cpp) (`ClientDownload::download`)

#### Workflow
```
COLLECT VERIFIED SHARDS (Directly from >= k Peers per Stripe via TCP GET_CHUNK)
    │
    ▼
[1] Group by Stripe ID & Sort Numerically by Minimum `sequence` Number
    │  (Prevents lexicographical string-sort bugs like `"S1000" < "S200"` on >=1000-stripe files)
    ▼
[2] Validate Sequence Slot Completeness (`D0..Dk-1` present in manifest) & Check SHA-256 per Loaded Shard
    │
    ▼
[3] If Any Data Shard Missing: Systematic GF(256) RS Decode (`rs_.decode()`)
    │
    ▼
[4] Re-Verify SHA-256 of Every Reconstructed Data Shard against `sm.sha256`
    │
    ▼
[5] Write Sequentially in Strict Sequence Order & Remove RS Alignment Padding (`original_data_length`)
    │
    ▼
[6] Verify Total Bytes Written == `original_size` & Final Full-File SHA-256 Match ──► ORIGINAL FILE
```

---

## Research-Based Comparison

| Baseline Approach | Strength | Limitation | Hybrid Adaptive Architecture Response |
|---|---|---|---|
| **Replication-Only** | Fast direct recovery | $2\times$–$3\times$ storage overhead | **ARMA** uses hot direct replicas *selectively* when spare capacity exists |
| **Reed-Solomon Only** | Low storage overhead (e.g., $1.5\times$) | Higher repair & degraded-read cost | **HRERA** treats RS decoding as the storage-efficient fallback |
| **Fixed Chunking** | Simple offset indexing | Suboptimal across KB–GB file scales | **DACA** adapts shard boundaries by file size and peer storage headroom |
| **Basic Placement** | Uniform hash/round-robin | Blind to congestion, health & domains | **PASPA** scores health, storage, load, balance, and IP domain separation |
| **Ad-Hoc P2P Repair** | Decentralized bandwidth | No priority policy or backoff queue | **HRERA** prioritizes replica copy $\rightarrow$ P2P RS decode $\rightarrow$ backoff queue |

---

## Control Plane & Storage Modules

| # | Module | Source Files | Responsibility |
|---|---|---|---|
| **1** | **Upload API & Session Manager** | [`engine/session/session_manager.hpp`](engine/session/session_manager.hpp), [`engine/session/session_manager.cpp`](engine/session/session_manager.cpp), [`engine/coordinator/http_server.cpp`](engine/coordinator/http_server.cpp) | CSPRNG session tokens (`BCryptGenRandom` / `/dev/urandom`), constant-time token check, streamed multi-part TCP upload (`UPLOAD_INIT/PART/COMPLETE`), HTTP/1.1 REST API & live Web Console (`:8080`). |
| **2** | **DACA Chunking Engine** | [`engine/coding/adaptive_chunker.hpp`](engine/coding/adaptive_chunker.hpp), [`engine/coding/adaptive_chunker.cpp`](engine/coding/adaptive_chunker.cpp) | Adaptive boundary selection, zero-padding normalization, and sequence-ordered MGFRA reconstruction. |
| **3** | **SHA-256 Hashing & Index** | [`engine/chunking/chunk_manager.cpp`](engine/chunking/chunk_manager.cpp) | OpenSSL `EVP_sha256()` digests for shards and full files. |
| **4** | **Reed-Solomon Encoder** | [`engine/coding/gf256.cpp`](engine/coding/gf256.cpp), [`engine/coding/reed_solomon.cpp`](engine/coding/reed_solomon.cpp) | Arbitrary `RS(k+m, k)` systematic Cauchy erasure coding over `GF(256)`. |
| **5** | **PASPA & ARMA Scheduler** | [`engine/distribution/dynamic_distributor.cpp`](engine/distribution/dynamic_distributor.cpp) | Multi-factor suitability scoring, failure-domain separation, and selective direct hot replication. |
| **6** | **Metadata Store** | [`engine/catalog/catalog_manager.cpp`](engine/catalog/catalog_manager.cpp), [`engine/metadata/metadata_manager.cpp`](engine/metadata/metadata_manager.cpp) | Atomic `.tmp` JSON persistence (`catalog.json`, `files/*.json`, `sessions.json`, `events.json`). |
| **7** | **HPFD Peer Health Monitor** | [`engine/registry/peer_registry.cpp`](engine/registry/peer_registry.cpp), [`engine/coordinator/coordinator_service.cpp`](engine/coordinator/coordinator_service.cpp) | Real-time heartbeat state machine (`ONLINE`, `BUSY`, `SUSPECT`, `OFFLINE`) & returning-peer disk reconciliation. |
| **8** | **HRERA Recovery Manager** | [`engine/coordinator/coordinator_service.cpp`](engine/coordinator/coordinator_service.cpp), [`peer/peer.cpp`](peer/peer.cpp) | 9-step peer-to-peer repair pipeline (`COPY_SHARD` & `REPAIR_SHARD`) + `RECOVERY_PENDING` exponential backoff worker. |

---

## Scratch System Setup, Dependencies & Build

Choose the path matching your machine:

### Path A — Fresh Windows 10 / 11 PC with Zero Pre-Installed Tools (Portable Run)

If you just want to **run the full system immediately without installing Visual Studio, CMake, or OpenSSL**:
1. Download the repository ZIP from GitHub (click **Code $\rightarrow$ Download ZIP** and extract) **or** `git clone https://github.com/kulkarni-v-ai/MINI-PROJECT.git`.
2. Prebuilt standalone x64 binaries inside [`deploy/bin/`](deploy/bin/) (`controller.exe`, `peer.exe`, `rfe.exe`) are compiled with a **static MSVC C/C++ runtime (`/MT`)** and ship with the bundled OpenSSL 3 runtime (`libcrypto-3-x64.dll`), so **zero VC++ Redistributable or external SDK installations are required** on Windows 10/11.
3. Launch immediately using [`run_demo.ps1`](run_demo.ps1) (see Section 8 below).

---

### Path B — Fresh Windows 10 / 11 PC Building from Source

Install the build prerequisites on a brand-new Windows PC using `winget` (Windows Package Manager, built into Windows 10/11):

```powershell
# 1. Install Git (to clone repository)
winget install -e --id Git.Git

# 2. Install CMake >= 3.16
winget install -e --id Kitware.CMake

# 3. Install Visual Studio 2022 Build Tools with C++ (MSVC v143 + Windows 10/11 SDK)
winget install -e --id Microsoft.VisualStudio.2022.BuildTools `
  --override "--quiet --wait --norestart --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
```

> **Note:** **OpenSSL 3 is already bundled** in [`third_party/openssl/`](third_party/openssl/) (headers, import `libcrypto.lib`, and `libcrypto-3-x64.dll`), so no external OpenSSL download is needed on Windows.

Clone and build (open a fresh PowerShell terminal after installing):

```powershell
git clone https://github.com/kulkarni-v-ai/MINI-PROJECT.git
cd MINI-PROJECT

cmake -S . -B build -G "Visual Studio 17 2022" -A x64
cmake --build build --config Release --parallel
```

> **Tip:** If `cmake` is not on your global `PATH`, invoke Visual Studio's bundled CMake directly:
> ```powershell
> $cmake = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe"
> & $cmake -S . -B build -G "Visual Studio 17 2022" -A x64
> & $cmake --build build --config Release --parallel
> ```

Generated executables in `build\Release\`:
- `controller.exe` — Central Coordination Server, HTTP PowerShell Web Monitor & test suite runner
- `peer.exe` — Independent storage peer daemon
- `rfe.exe` — Client / Operator CLI
- `libcrypto-3-x64.dll` — OpenSSL 3 crypto runtime (copied automatically next to binaries)

---

### Path C — Fresh Linux / WSL2 / macOS System

Install compiler, CMake, Git, and OpenSSL development headers:

- **Ubuntu / Debian / WSL2:**
  ```bash
  sudo apt update && sudo apt install -y git build-essential cmake libssl-dev
  ```
- **Fedora / RHEL / Rocky Linux:**
  ```bash
  sudo dnf install -y git gcc-c++ cmake openssl-devel
  ```
- **macOS (Homebrew):**
  ```bash
  xcode-select --install
  brew install git cmake openssl@3
  ```

Clone & build:

```bash
git clone https://github.com/kulkarni-v-ai/MINI-PROJECT.git
cd MINI-PROJECT
cmake -S . -B build
cmake --build build -j
```

*(On macOS with Homebrew OpenSSL, pass `-DOPENSSL_ROOT_DIR=$(brew --prefix openssl@3)` to `cmake -S . -B build` if needed.)*

---

## Automated Verification & Test Suite (`T01`–`T13`)

Run all **13 multi-process integration tests**:

```powershell
.\build\Release\controller.exe test
```

| Test | What It Verifies (Zero Mocks) |
|---|---|
| `T01`–`T07` | Gate-1 baseline engines: fixed chunking, round-robin replication, single/multi-peer failure detection, interrupted transfer discard & retry, SHA-256 corruption rejection, unrecoverable loss refusal, sequence-ordered reconstruction. |
| `T08_M2` | Dynamic peer registration (`PEER_REGISTER`), free storage & active transfer reporting, heartbeats, `ONLINE -> SUSPECT -> OFFLINE -> ONLINE`. |
| `T09_RS` | DACA sizing & systematic `GF(256)` `RS(3,2)` reconstruction across all pair combinations (`D0+D1`, `D0+P0`, `D1+P0`) and `< k` refusal. |
| `T10_GATE2` | Gate-2 end-to-end on-demand RS distribution, direct peer retrieval, central fallback, and P2P RS repair onto replacement Peer `G2_D`. |
| `T11_RECOVERY_FLOW` | TCP authentication (`AUTH_ERROR` / `SESSION_AUTH`), streamed multi-part upload (`UPLOAD_INIT/PART/COMPLETE`), corrupt-shard detection & `REPORT_SHARD_ISSUE` auto-repair, and `< k` `RECOVERY_PENDING` exponential-backoff resume. |
| `T12_AUTH_HTTP` | Embedded HTTP/1.1 Upload API (`POST /api/upload`), `401 Unauthorized` enforcement, Bearer token authentication (`POST /api/auth`), Web Console (`GET /`), and cross-protocol `ClientDownload` verification. |
| `T13_HYBRID_ALGORITHMS` | Full **Hybrid Adaptive** suite: DACA resource-aware step-down sizing, `RS(6,4)` double-shard loss recovery, MGFRA sequence ordering across **1,005 stripes (`S000`–`S1004`)**, **PASPA + ARMA** selective hot replication across 5 peers, **HRERA Path A (`REPAIR_REPLICA_OK` via `COPY_SHARD`)** without RS math, and **HRERA Path B (`REPAIR_RS_OK` via `REPAIR_SHARD`)** P2P RS fallback. |

Also run the PowerShell CLI & HTTP end-to-end validation script:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\tests\test_gate2_cli.ps1
```

---

## How to Run the Live System (Step-by-Step Guide)

### Fastest Launch: One-Command Demo

- **Windows (works with either compiled `build\Release\` OR zero-install `deploy\bin\`):**
  ```powershell
  powershell -NoProfile -ExecutionPolicy Bypass -File .\run_demo.ps1 -Reset
  ```
  *Starts Coordinator (`:5000` / Web Monitor `:8080`) + 4 independent peers (`PEER_A..PEER_D`, use `-Peers 3` for RS-only or `-Peers 5` for full failover) and opens **`http://127.0.0.1:8080/`**.*

- **Linux / macOS:**
  ```bash
  chmod +x ./run_demo.sh
  ./run_demo.sh 4
  ```
  *Then open **`http://127.0.0.1:8080/`** in your browser.*

---

### Option A: Manual Single-PC Multi-Terminal Demo

Run inside the repository root (using `.\build\Release\...`, or inside `deploy\` using `.\bin\...`):

1. **Terminal 1 — Start Coordinator Server (TCP `:5000` + PowerShell/CMD HTTP Live Visualizer `:8080`)**
   ```powershell
   .\build\Release\controller.exe reset
   .\build\Release\controller.exe coordinator 5000 0.0.0.0 8080
   ```
   Open **`http://127.0.0.1:8080/`** in your browser to launch the **PowerShell / CMD-Styled Live Monitor** (default auth token: `rfe-demo-token`, with a **PowerShell Blue / CMD Black** theme switch). Upload any file or click **Visualize Internals** to watch in real time:
   - The **8-stage End-to-End File Flow**,
   - **DACA chunk byte offsets & zero-padding**,
   - **GF(256) Cauchy Parity Matrix & live peer hex/ASCII byte proof**,
   - **Interactive Peer-Drop / Live Peer-Outage RS Recovery math**, and
   - The **HRERA 9-Step Recovery Flowchart** & live Peer Chassis Rack.

2. **Terminals 2, 3, 4 (+ Optional Peers 4 & 5 for ARMA Selective Hot Replicas)**
   - **3-Peer `RS(3,2)` Mode ($1.5\times$ minimal storage overhead):**
     ```powershell
     # Terminal 2
     .\build\Release\peer.exe PEER_A 127.0.0.1 5001 127.0.0.1 5000
     # Terminal 3
     .\build\Release\peer.exe PEER_B 127.0.0.1 5002 127.0.0.1 5000
     # Terminal 4
     .\build\Release\peer.exe PEER_C 127.0.0.1 5003 127.0.0.1 5000
     ```
   - **5-Peer Full Hybrid Adaptive Mode (Triggers ARMA Hot Replica + Direct `COPY_SHARD` Failover):**
     ```powershell
     # Terminal 5 (Disjoint Peer D receives ARMA hot replica of primary DATA shard)
     .\build\Release\peer.exe PEER_D 127.0.0.1 5004 127.0.0.1 5000
     # Terminal 6 (Standby healthy Peer E ready for immediate failover target)
     .\build\Release\peer.exe PEER_E 127.0.0.1 5005 127.0.0.1 5000
     ```

3. **Client Terminal — Operate `rfe.exe` CLI (or Upload Directly from the Browser Web Console)**
   ```powershell
   # Authenticate CLI session with Coordinator
   .\build\Release\rfe.exe connect 127.0.0.1:5000 --token rfe-demo-token

   # Inspect HPFD live peer health, available disk storage & transfer load
   .\build\Release\rfe.exe peers

   # Generate a sample file (or upload any PDF, ZIP, video, image)
   New-Item -ItemType Directory -Force input | Out-Null
   $b = New-Object byte[] 120000; (New-Object System.Random 7).NextBytes($b)
   [System.IO.File]::WriteAllBytes("$PWD\input\demo.bin", $b)

   # Stream upload -> DACA Chunk -> SHA-256 -> RS(3,2) -> PASPA Placement -> ARMA Replicas
   .\build\Release\rfe.exe upload .\input\demo.bin

   # View catalog, exact shard locations & peer-to-client MGFRA download
   .\build\Release\rfe.exe files
   .\build\Release\rfe.exe info F001
   .\build\Release\rfe.exe download F001
   .\build\Release\rfe.exe events 15
   ```

---

### Option B: 4-PC Physical Wi-Fi / LAN Demo

Copy **`deploy\`** to all 4 Windows laptops on the same Wi-Fi/LAN.

1. **Open Firewall Ports once per PC (Administrator PowerShell):**
   ```powershell
   powershell -NoProfile -ExecutionPolicy Bypass -File .\deploy\scripts\setup_firewall.ps1
   ```
2. **Find each laptop's LAN IPv4:**
   ```powershell
   (Get-NetIPAddress -AddressFamily IPv4 | Where-Object { $_.InterfaceAlias -notlike "*Loopback*" -and $_.IPAddress -notlike "169.254*" }).IPAddress
   ```
3. **Start Roles with Actual LAN IPs (`<PC4_IP>` = Coordinator IP):**
   - **PC4 (Coordinator + HTTP Web Console):** `.\deploy\bin\controller.exe coordinator 5000 0.0.0.0 8080`
   - **PC1 (Peer A):** `.\deploy\bin\peer.exe PEER_A <PC1_IP> 5001 <PC4_IP> 5000`
   - **PC2 (Peer B):** `.\deploy\bin\peer.exe PEER_B <PC2_IP> 5002 <PC4_IP> 5000`
   - **PC3 (Peer C):** `.\deploy\bin\peer.exe PEER_C <PC3_IP> 5003 <PC4_IP> 5000`
4. **Connect Client / Browser from any PC:**
   - **Web Console:** `http://<PC4_IP>:8080/`
   - **CLI:** `.\deploy\bin\rfe.exe connect <PC4_IP>:5000 --token rfe-demo-token`

See [`GATE2_WINDOWS_DEPLOYMENT.md`](GATE2_WINDOWS_DEPLOYMENT.md) for full multi-laptop output examples.

---

## Failure, Corruption & Auto-Recovery Walkthrough

1. **Test 1 — Replica Failover (`REPAIR_REPLICA_OK` via `COPY_SHARD`, 4–5 Peers):**
   - Check `rfe info F001` to see which peer is primary for `F001-S000-D0` (e.g. `PEER_A,PEER_D`).
   - Stop `PEER_A` (`Ctrl+C`).
   - HPFD marks `PEER_A` `OFFLINE`; HRERA sees `D0`'s alternate replica on `PEER_D` and copies `D0` directly from `PEER_D` to healthy `PEER_E` (`REPAIR_REPLICA_OK`) without needing RS decoding.
2. **Test 2 — Erasure Recovery (`REPAIR_RS_OK` via `REPAIR_SHARD`, 3+ Peers):**
   - Stop a peer holding an unreplicated shard (e.g. `PEER_B` holding `D1`).
   - Run `.\build\Release\rfe.exe download F001` while offline: client seamlessly reconstructs `D1` locally using surviving `D0 + P0` via GF(256) Reed-Solomon.
   - Ensure a healthy replacement/spare peer (`PEER_D` or restarted `PEER_A`) is online -> HRERA Step 3 NO -> Step 5/7 restores `D1` directly peer-to-peer (`REPAIR_RS_OK`) and returns `F001` to `AVAILABLE`.
3. **Test 3 — Silent Bit-Rot / Shard Corruption Detection:**
   - Overwrite bytes in `storage\peer_PEER_A\chunks\F001-S000-D0.chunk` and run `.\build\Release\rfe.exe download F001`.
   - `ClientDownload` detects the SHA-256 mismatch, logs `SHARD_CORRUPT` via `REPORT_SHARD_ISSUE`, reconstructs a verified file via alternate replica or parity, and Coordinator auto-repairs the damaged shard.
4. **Test 4 — Multi-Peer Outage (`< k` Surviving Shards -> `RECOVERY_PENDING`):**
   - Stop 2 peers simultaneously so only 1 shard survives per stripe (`< k=2`).
   - `rfe files` displays **`RECOVERY_PENDING`**, and `rfe download` refuses incomplete reconstruction cleanly.
   - Restart peers -> background backoff worker wakes immediately and restores all shards to `AVAILABLE`.

---

## Configuration & HTTP REST API Reference

### [`config.json`](config.json)

```json
{
  "coordinator": { "address": "127.0.0.1", "port": 5000 },
  "http_port": 8080,
  "auth_token": "rfe-demo-token",
  "require_auth": true,
  "auto_recover": true,
  "rs_k": 2,
  "rs_m": 1,
  "adaptive_replicas": true,
  "hot_replica_count": 1,
  "heartbeat_interval_ms": 500,
  "heartbeat_timeout_ms": 1000,
  "failure_threshold": 2
}
```

### HTTP REST API (`:8080`)

| Endpoint | Auth | Description |
|---|---|---|
| `GET /` | Public | PowerShell / CMD Live Cluster Monitor (8-stage pipeline, DACA chunk map, GF(256) parity/recovery proof, peer chassis rack & HRERA 9-step flowchart) |
| `POST /api/auth` | JSON `{"token": "..."}` | Issues CSPRNG Bearer `session_token` |
| `POST /api/upload` | `Authorization: Bearer <tok>` | Raw binary (`X-Filename`) or `multipart/form-data` -> DACA -> SHA-256 -> RS -> PASPA -> ARMA |
| `GET /api/status` | Public read | Cluster health, peer counts, RS/ARMA policy, active sessions |
| `GET /api/peers` | Public read | Live peer registry with workloads and heartbeat ages |
| `GET /api/files` / `GET /api/files/<id>` | Public read | Catalog index and per-stripe shard health map |
| `GET /api/inspect/<id>` | Public (`?probe=1` requires Auth) | DACA tier/offset breakdown, GF(256) encoding matrix, live peer TCP `GET_CHUNK` probe (`?probe=1`), parity verification & live C++ RS reconstruction proof |
| `GET /api/events` | Public read | Cluster integrity, placement & recovery audit trail |
| `POST /api/repair/<id>` | `Authorization: Bearer <tok>` | Synchronous 9-step HRERA recovery trigger |
| `GET /api/download/<id>` | `Authorization` or `?token=` | Downloads safety copy from Coordinator central staging |
