// ============================================================================
// recovery_manager.hpp - E5: Recovery & Reconstruction Engine (core engine)
//
// Implements the spec §8 flow:
//   1. receive failed peer ID        2. read metadata
//   3. find chunks whose replica locations include the failed peer
//   4. mark them affected            5. pick surviving eligible sources
//   6. request chunk over TCP        7. receive the COMPLETE chunk
//   8. calculate SHA-256             9. compare with expected SHA-256
//  10. valid -> accept              11. invalid -> reject, log, try next source
//  12. interrupted -> discard temp data, retry the COMPLETE transfer
//  13. no surviving source -> report UNRECOVERABLE
//  14. sort by sequence             15. concatenate in sequence order
//  16. write recovered file         17. byte-compare with the original
//  18. log the final result
//
// Resumable byte-range recovery is intentionally NOT implemented (spec §8).
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../models/peer.hpp"
#include "../metadata/metadata_manager.hpp"

struct RecoveryResult {
    bool success = false;
    std::string failed_peer;
    std::vector<std::string> affected_chunks;       // step 4
    std::vector<std::string> recovered_chunks;      // accepted chunks
    std::vector<std::string> unrecoverable_chunks;  // step 13
    int retry_count = 0;                            // steps 11-12
    int integrity_failures = 0;                     // step 11
    uint64_t bytes_transferred = 0;
};

class RecoveryManager {
public:
    RecoveryManager(std::vector<PeerInfo> peers, MetadataManager& meta,
                    std::string output_dir = "output");

    // Steps 1-13: pull every required chunk from surviving replicas into
    // the recovery staging area (output/recovered_chunks/).
    //
    // offline_peers: peers known to be down (usually just the failed one;
    // T05/T07 pass several). simulate_interrupt_once: T03 test hook —
    // the first GET_CHUNK asks the source peer to abort mid-transfer.
    RecoveryResult recover(const std::string& failed_peer,
                           const std::vector<std::string>& offline_peers,
                           bool simulate_interrupt_once = false);

    // Steps 14-18: order staged chunks by sequence, concatenate, write the
    // recovered file and byte-compare it against the original.
    // Never reconstructs if required chunks are missing (spec §19).
    bool reconstruct(const std::string& original_path,
                     std::string& recovered_path_out,
                     std::string& error_out);

    std::string staging_dir() const { return output_dir_ + "/recovered_chunks"; }

private:
    std::vector<PeerInfo> peers_;
    MetadataManager& meta_;
    std::string output_dir_;

    bool is_offline(const std::string& peer_id,
                    const std::vector<std::string>& offline) const;

    // Steps 6-12 for a single chunk. Returns true once a SHA-256-valid
    // copy is staged. `simulate_interrupt` arms the T03 hook.
    bool fetch_chunk(const ChunkMeta& chunk,
                     const std::vector<PeerInfo>& eligible_sources,
                     bool simulate_interrupt,
                     RecoveryResult& result);
};
