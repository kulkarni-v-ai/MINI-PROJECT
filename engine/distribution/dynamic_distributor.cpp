// ============================================================================
// dynamic_distributor.cpp - Dynamic Peer Selection & Shard Distribution implementation
// ============================================================================
#include "dynamic_distributor.hpp"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>

#include "../chunking/chunk_manager.hpp"
#include "../communication/protocol.hpp"
#include "../communication/tcp_client.hpp"

DynamicDistributor::DynamicDistributor(PeerRegistry& registry) : registry_(registry) {}

bool DynamicDistributor::push_shard(const PeerInfo& peer,
                                   const ShardMeta& shard,
                                   const std::string& shard_file_path,
                                   std::string& error_out) {
    std::ifstream in(shard_file_path, std::ios::binary);
    if (!in) {
        error_out = "staged shard file not found: " + shard_file_path;
        return false;
    }

    std::vector<uint8_t> payload((std::istreambuf_iterator<char>(in)),
                                 std::istreambuf_iterator<char>());

    if (payload.size() != shard.size) {
        error_out = "shard size mismatch on disk: " + std::to_string(payload.size()) +
                    " vs expected " + std::to_string(shard.size);
        return false;
    }

    TCPClient client(3000, 3000);
    if (!client.connect_to(peer.address, peer.port)) {
        error_out = "failed to connect to peer " + peer.id + " (" + peer.address + ":" +
                    std::to_string(peer.port) + ")";
        return false;
    }

    json::Json req = json::Json::object();
    req["type"] = proto::STORE_CHUNK;
    req["chunk_id"] = shard.chunk_id;
    req["sha256"] = shard.sha256;

    json::Json resp;
    std::vector<uint8_t> resp_payload;
    auto status = client.request(req, payload, resp, resp_payload);
    if (status != proto::RecvStatus::OK) {
        error_out = "transfer error to peer " + peer.id;
        return false;
    }

    if (resp["type"].as_string() != proto::STORE_ACK) {
        error_out = "peer " + peer.id + " rejected shard store: " +
                    (resp.contains("reason") ? resp["reason"].as_string() : "unknown reason");
        return false;
    }

    return true;
}

FileDistributionStatus DynamicDistributor::distribute_file(const std::string& /*file_id*/,
                                                          std::vector<ShardMeta>& shards,
                                                          const std::string& staging_dir,
                                                          std::string& log_out,
                                                          size_t min_k) {
    std::ostringstream ss_log;
    auto online_peers = registry_.get_online_peers();
    if (online_peers.empty()) {
        log_out = "cannot distribute: 0 peers are online";
        return FileDistributionStatus::STORED;
    }

    // Group shards by stripe
    std::map<std::string, std::vector<size_t>> stripes;
    for (size_t i = 0; i < shards.size(); ++i) {
        stripes[shards[i].stripe_id].push_back(i);
    }

    size_t stripe_index = 0;
    bool all_stripes_fully_placed = true;
    bool all_stripes_recoverable = true; // at least min_k shards per stripe

    for (const auto& kv : stripes) {
        (void)kv.first; // stripe_id
        const auto& shard_indices = kv.second;

        // Filter and rank eligible peers
        std::vector<PeerInfo> eligible;
        for (const auto& p : online_peers) {
            if (p.workload.is_busy()) continue;
            // Verify peer has enough storage
            uint64_t req_space = shards[shard_indices[0]].size;
            if (p.workload.available_storage_bytes > 0 &&
                p.workload.available_storage_bytes < req_space) {
                continue;
            }
            eligible.push_back(p);
        }

        if (eligible.empty()) {
            eligible = online_peers; // Fallback to all online peers if all are reported busy
        }

        // Sort eligible peers: lowest active transfers first, then highest available storage
        std::sort(eligible.begin(), eligible.end(), [](const PeerInfo& a, const PeerInfo& b) {
            if (a.workload.active_transfers != b.workload.active_transfers)
                return a.workload.active_transfers < b.workload.active_transfers;
            return a.workload.available_storage_bytes > b.workload.available_storage_bytes;
        });

        // Rotate offset based on stripe index to spread combinations
        if (eligible.size() > 1) {
            size_t rot = stripe_index % eligible.size();
            std::rotate(eligible.begin(), eligible.begin() + rot, eligible.end());
        }

        std::vector<std::string> used_peer_ids;
        size_t stored_in_stripe = 0;

        for (size_t s_idx : shard_indices) {
            ShardMeta& shard = shards[s_idx];
            const std::string shard_path = staging_dir + "/" + shard.chunk_id + ".chunk";

            bool placed = false;
            // Find distinct peer not yet storing a shard in this stripe
            for (const auto& candidate : eligible) {
                if (std::find(used_peer_ids.begin(), used_peer_ids.end(), candidate.id) != used_peer_ids.end()) {
                    continue;
                }

                std::string err;
                if (push_shard(candidate, shard, shard_path, err)) {
                    shard.peers = {candidate.id};
                    shard.status = "HEALTHY";
                    used_peer_ids.push_back(candidate.id);
                    placed = true;
                    ++stored_in_stripe;
                    ss_log << "  [E3] Placed " << shard.chunk_id << " -> Peer " << candidate.id << "\n";
                    break;
                } else {
                    ss_log << "  [E3] Failed to store " << shard.chunk_id << " on Peer " << candidate.id
                           << ": " << err << " (retrying alternative)\n";
                }
            }

            if (!placed) {
                shard.status = "UNAVAILABLE";
                ss_log << "  [E3] Shortfall: could not place shard " << shard.chunk_id << " on any peer\n";
            }
        }

        if (stored_in_stripe < shard_indices.size()) all_stripes_fully_placed = false;
        if (stored_in_stripe < min_k) all_stripes_recoverable = false;

        ++stripe_index;
    }

    log_out = ss_log.str();

    if (all_stripes_fully_placed) return FileDistributionStatus::AVAILABLE;
    if (all_stripes_recoverable)  return FileDistributionStatus::DEGRADED;
    return FileDistributionStatus::STORED;
}
