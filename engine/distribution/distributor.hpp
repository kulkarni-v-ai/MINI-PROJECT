// ============================================================================
// distributor.hpp - E3: Distribution & Replication Engine
//
// Round-robin placement with replication factor 2 (spec §6):
//
//   primary_index = chunk_index % number_of_peers
//   replica_index = (primary_index + 1) % number_of_peers
//
//   C1 -> A + B, C2 -> B + C, C3 -> C + A, C4 -> A + B, ...
//
// E3 places chunks and creates replicas. It does NOT detect failures
// and does NOT recover chunks.
// ============================================================================
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../models/peer.hpp"

class Distributor {
public:
    // peers: the configured peer list (A, B, C); replication factor = 2.
    explicit Distributor(std::vector<PeerInfo> peers);

    // Compute the two replica locations for chunk `index` (0-based).
    std::pair<std::string, std::string> placement_for(size_t index) const;

    // Push every staged chunk to its two target peers over TCP.
    // `staging_dir` holds <chunk_id>.chunk files produced by E1.
    // Returns true only if every replica of every chunk was stored;
    // on success, chunk peers lists are filled in `chunks`.
    bool distribute(const std::string& staging_dir,
                    std::vector<ChunkMeta>& chunks,
                    std::string& error_out);

    const std::vector<PeerInfo>& peers() const { return peers_; }

private:
    std::vector<PeerInfo> peers_;

    bool store_chunk(const PeerInfo& peer, const ChunkMeta& chunk,
                     const std::string& staged_path, std::string& error_out);
};
