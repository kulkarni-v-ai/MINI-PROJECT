# GATE 2 ARCHITECTURE SPECIFICATION
## Resilient Multi-Peer File Distribution & Recovery Engine

**Target Platform:** Windows 10/11 x64  
**Implementation Stack:** C++17, MSVC, CMake, Winsock2 TCP, OpenSSL SHA-256 (EVP), Native Win32 Console

---

## 1. System Topology & Physical Roles

Four Windows PCs connected over a local area network (LAN):

- **PC1 (Peer A):** Shard storage, direct shard transfers, peer-to-peer durability repair.
- **PC2 (Peer B):** Shard storage, direct shard transfers, peer-to-peer durability repair.
- **PC3 (Peer C):** Shard storage, direct shard transfers, peer-to-peer durability repair.
- **PC4 (Coordinator + Central Staging):** File catalogue, peer registry, metadata manifests, job scheduler, recovery orchestrator. (Optional: Can also run a local peer agent).
- **Client (CLI `rfe`):** Interactive terminal application executing on user machine, contacting Coordinator for metadata and Peers directly for shard transfer.

```
USER CLI (rfe)
    |
    | TCP (Catalogue & Metadata queries only)
    v
COORDINATOR (PC4)
  +-- File Catalogue & Multi-File Metadata (Atomic JSON)
  +-- Central Staging (Safety backup of originals)
  +-- Dynamic Peer Registry (ONLINE / BUSY / SUSPECT / OFFLINE)
  +-- Distribution & Durability Repair Scheduler
    |
    +-----------+-----------+
    | TCP       | TCP       | TCP (Control & manifests)
    v           v           v
  Peer A      Peer B      Peer C (PC1 - PC3)
  Storage     Storage     Storage
    ^           ^           ^
    |           |           |
    +===========+===========+ (Direct Peer-to-Peer TCP for Durability Repair)
    ^
    | (Direct Peer-to-Client TCP for Shard Downloads)
USER CLI
```

---

## 2. Engine Subsystem Responsibilities (E1–E6)

### E1 — Adaptive Chunking and Reed-Solomon RS(3,2)
- **Adaptive Sizing Policy:**
  - Up to 1 MB: 64 KB data shard
  - 1–10 MB: 256 KB data shard
  - 10–100 MB: 1 MB data shard
  - 100 MB–1 GB: 4 MB data shard
  - Above 1 GB: 8 MB data shard
  - Configurable via `config.json`.
- **Systematic Reed-Solomon over GF(256):**
  - Coding rate: $K=2$ (data shards $D_0, D_1$), $M=1$ (parity shard $P_0$), $N=3$ total shards per stripe.
  - Reconstruction property: Any 2 valid shards out of 3 reconstruct the original stripe:
    - $D_0 + D_1 \rightarrow$ Identity assembly
    - $D_0 + P_0 \rightarrow$ Decoded $D_1$ via Galois field arithmetic
    - $D_1 + P_0 \rightarrow$ Decoded $D_0$ via Galois field arithmetic
  - Final incomplete stripe padded deterministically; padding length recorded in metadata.

### E2 — Communication & Dynamic Peer Registry
- Application wire protocol: 4-byte big-endian header length + UTF-8 JSON header + binary payload.
- Configurable LAN IPv4 and TCP port per node.
- Dynamic registration: Peers advertise their ID, IP, port, free disk space, and transfer capacity upon startup.
- Direct peer-to-peer shard transfers without routing binary payloads through Coordinator.

### E3 — Dynamic Peer Selection & Distribution
- Elimination of static round-robin.
- Peer placement algorithm selects 3 distinct healthy peers per stripe based on:
  1. `ONLINE` health state
  2. Free storage capacity
  3. Active transfer count (avoiding overloaded peers)
  4. Network reachability
- First-demand distribution: Files are staged centrally on upload (`STORED`); RS chunking and peer distribution occur on first download request.

### E4 — Failure Detection & State Progression
- Real heartbeat polling at regular intervals (default 500 ms) with ACK timeout (1000 ms).
- Multi-tier state transitions:
  - `ONLINE` $\rightarrow$ responsive and available.
  - `BUSY` $\rightarrow$ concurrent transfer capacity reached; queue requests; never falsely treat as failed.
  - `SUSPECT` $\rightarrow$ 1 missed heartbeat.
  - `OFFLINE` $\rightarrow$ threshold (default 2) missed heartbeats.
  - `RECOVERING` $\rightarrow$ peer returning and reconciling shard manifests.

### E5 — Download Recovery vs Durability Repair
- **Download Recovery (Client-driven):**
  - Client contacts Coordinator to get shard locations.
  - Client connects directly to 2 surviving peers holding shards for each stripe.
  - Client validates SHA-256 for each received shard and decodes stripe locally.
- **Durability Repair (Peer-driven):**
  - Triggered when a peer is confirmed `OFFLINE`.
  - Coordinator designates a healthy replacement peer (e.g., Peer D or surviving peer).
  - Target peer contacts surviving shard holders directly over TCP, reconstructs missing shard, verifies hash, and commits to local disk.
  - Manifest updated only after target peer confirms successful storage.
  - Central-assisted repair used only as an explicit fallback when surviving peer count $< 2$.

### E6 — Multi-File Metadata & Staging
- Atomic JSON file manifests stored per File ID: `metadata/files/<FILE_ID>.json`.
- State tracking per file: `STORED`, `DISTRIBUTING`, `AVAILABLE`, `DEGRADED`, `RECOVERING`, `UNRECOVERABLE`.
- Per-file mutex/job locking to prevent redundant distribution when concurrent downloads hit an un-distributed file.
- Event audit log persisted to `metadata/events.json`.

---

## 3. Terminal Interface (`rfe` CLI)

| Command | Purpose |
|---|---|
| `rfe connect <ip>:<port>` | Connect client session to Coordinator |
| `rfe upload <file-path>` | Stage original file to Coordinator; assign File ID |
| `rfe files` | List all tracked files and distribution states |
| `rfe search <query>` | Search file catalog by filename |
| `rfe info <file-id>` | Detailed manifest, stripe count, shard map, peer locations |
| `rfe download <file-id>` | Retrieve shards from peers and reconstruct file locally |
| `rfe peers` | List registered peers, health states, storage, and workloads |
| `rfe status` | Coordinator status, active transfers, cluster durability |
| `rfe transfers` | View live active and pending chunk/shard transfers |
