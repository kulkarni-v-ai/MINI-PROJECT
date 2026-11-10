// ============================================================================
// client_download.hpp - Direct Peer-to-Client Download & Local RS Reconstruction (E5)
//
// Protocol flow:
//   1. Client authenticates session (if auth enabled) and queries Coordinator for
//      file manifest & live peer endpoints.
//   2. Coordinator triggers RS(k+m, k) encoding & placement if file is STORED.
//   3. Client connects DIRECTLY to storage peers over TCP (Coordinator does NOT
//      relay shard bytes).
//   4. Client prioritizes HEALTHY shards on ONLINE peers, retrieves at least K
//      valid shards per stripe, verifies each shard's SHA-256, and reports any
//      missing or hash-mismatched (CORRUPT) shard back to Coordinator.
//   5. Client performs local Reed-Solomon reconstruction in sequence order and
//      verifies the final file SHA-256.
//   6. Supports central staging download over local path OR remote TCP
//      (--source central).
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../catalog/catalog_manager.hpp"
#include "../coordinator/coordinator_service.hpp"

enum class DownloadSource {
    PEERS,   // Normal multi-peer direct retrieval with RS(k+m, k) local decode
    CENTRAL  // Explicit central staging fallback (--source central)
};

struct DownloadResult {
    bool success = false;
    std::string file_id;
    std::string filename;
    uint64_t file_size = 0;
    std::string expected_sha256;
    std::string actual_sha256;
    std::string output_path;
    DownloadSource source_used = DownloadSource::PEERS;
    size_t stripes_total = 0;
    size_t shards_retrieved = 0;
    size_t corrupted_shards_detected = 0;
    std::string message;
};

class ClientDownload {
public:
    ClientDownload(std::string coordinator_address = "127.0.0.1",
                   uint16_t coordinator_port = 5000,
                   std::string output_dir = "output",
                   std::string session_token = "",
                   std::string auth_token = "rfe-demo-token");

    // Execute download workflow
    DownloadResult download(const std::string& file_id,
                            DownloadSource source = DownloadSource::PEERS,
                            CoordinatorService* in_proc_coordinator = nullptr);

private:
    std::string coord_addr_;
    uint16_t coord_port_;
    std::string output_dir_;
    std::string session_token_;
    std::string auth_token_;

    bool ensure_session();
    void report_shard_issue(const std::string& file_id,
                            const std::string& shard_id,
                            const std::string& peer_id,
                            const std::string& issue,
                            CoordinatorService* in_proc_coordinator);

    bool fetch_shard_from_peer(const std::string& peer_addr,
                               uint16_t peer_port,
                               const std::string& shard_id,
                               const std::string& expected_sha256,
                               const std::string& save_path,
                               std::string& error_out,
                               bool& integrity_mismatch_out);
};
