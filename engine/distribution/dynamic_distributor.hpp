// ============================================================================
// dynamic_distributor.hpp - Dynamic Peer Selection & Shard Distribution (E3)
//
// Dynamically selects distinct, healthy, capable peers per RS(3,2) stripe:
//   - Filter by ONLINE health state
//   - Filter by available disk capacity
//   - Filter by transfer workload (concurrent active transfers vs capacity)
//   - Rotates placement across stripes to avoid hot spots
//   - Sends real shard bytes over TCP
//   - Verifies SHA-256 acknowledgements before committing locations
//   - Retries missing/failed placements onto alternative eligible peers
//   - Reports AVAILABLE (3/3), DEGRADED (2/3), or shortfall honestly
// ============================================================================
#pragma once

#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../models/peer.hpp"
#include "../catalog/catalog_manager.hpp"
#include "../registry/peer_registry.hpp"

class DynamicDistributor {
public:
    explicit DynamicDistributor(PeerRegistry& registry);

    // Distribute shards to dynamically chosen distinct peers.
    // Updates shard.peers on successful placement.
    FileDistributionStatus distribute_file(const std::string& file_id,
                                          std::vector<ShardMeta>& shards,
                                          const std::string& staging_dir,
                                          std::string& log_out,
                                          size_t min_k = 2);

private:
    PeerRegistry& registry_;

    bool push_shard(const PeerInfo& peer,
                    const ShardMeta& shard,
                    const std::string& shard_file_path,
                    std::string& error_out);
};
