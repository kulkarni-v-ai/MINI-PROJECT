// ============================================================================
// dynamic_distributor.hpp - PASPA & ARMA Placement / Redundancy Engine (E3)
//
// Implements:
//   - PASPA (Peer-Aware Shard Placement Algorithm):
//       Weighted suitability scoring across ONLINE peers combining health stability,
//       available storage headroom, active-transfer load, cumulative balance, and
//       failure-domain separation (unique peer ID & IP host anti-affinity per stripe).
//   - ARMA (Adaptive Redundancy Management Algorithm):
//       Combines RS(k+m, k) systematic erasure shards with selective direct hot
//       replicas for primary data shards when healthy disjoint spare capacity exists.
//   - Genuine TCP shard push with SHA-256 verification before committing metadata.
// ============================================================================
#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../models/peer.hpp"
#include "../catalog/catalog_manager.hpp"
#include "../registry/peer_registry.hpp"

struct ArmaPolicy {
    bool adaptive_replicas = true;
    size_t hot_replica_count = 1; // selective hot direct replicas per stripe when disjoint peers exist
};

class DynamicDistributor {
public:
    explicit DynamicDistributor(PeerRegistry& registry);

    // Execute PASPA primary placement + ARMA selective direct replication across peers.
    // Updates shard.peers and shard.status on verified SHA-256 commit.
    FileDistributionStatus distribute_file(const std::string& file_id,
                                          std::vector<ShardMeta>& shards,
                                          const std::string& staging_dir,
                                          std::string& log_out,
                                          size_t min_k = 2,
                                          ArmaPolicy arma = ArmaPolicy());

private:
    PeerRegistry& registry_;

    bool push_shard(const PeerInfo& peer,
                    const ShardMeta& shard,
                    const std::string& shard_file_path,
                    std::string& error_out);
};
