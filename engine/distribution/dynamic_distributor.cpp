// ============================================================================
// dynamic_distributor.cpp - PASPA & ARMA Peer Selection / Distribution Engine
// ============================================================================
#include "dynamic_distributor.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <set>
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
                                                          size_t min_k,
                                                          ArmaPolicy arma) {
    std::ostringstream ss_log;
    auto online_peers = registry_.get_online_peers();
    if (online_peers.empty()) {
        log_out = "cannot distribute: 0 peers are online";
        return FileDistributionStatus::STORED;
    }

    // Group shards by stripe and sort strictly by numeric min sequence
    struct StripeBucket {
        std::string stripe_id;
        uint64_t min_seq = std::numeric_limits<uint64_t>::max();
        std::vector<size_t> shard_indices;
    };
    std::map<std::string, size_t> stripe_pos;
    std::vector<StripeBucket> ordered_stripes;
    for (size_t i = 0; i < shards.size(); ++i) {
        auto it = stripe_pos.find(shards[i].stripe_id);
        if (it == stripe_pos.end()) {
            stripe_pos[shards[i].stripe_id] = ordered_stripes.size();
            ordered_stripes.push_back({shards[i].stripe_id, shards[i].sequence, {i}});
        } else {
            auto& b = ordered_stripes[it->second];
            b.min_seq = std::min(b.min_seq, shards[i].sequence);
            b.shard_indices.push_back(i);
        }
    }
    std::sort(ordered_stripes.begin(), ordered_stripes.end(),
              [](const StripeBucket& a, const StripeBucket& b) {
                  if (a.min_seq != b.min_seq) return a.min_seq < b.min_seq;
                  return a.stripe_id < b.stripe_id;
              });

    // Dynamic PASPA state across stripes
    std::map<std::string, uint64_t> virtual_avail;
    std::map<std::string, uint32_t> virtual_transfers;
    std::map<std::string, size_t> cumulative_assigned;
    for (const auto& p : online_peers) {
        virtual_avail[p.id] = p.workload.available_storage_bytes;
        virtual_transfers[p.id] = p.workload.active_transfers;
        cumulative_assigned[p.id] = 0;
    }

    size_t stripe_index = 0;
    bool all_stripes_fully_placed = true;
    bool all_stripes_recoverable = true; // at least min_k shards per stripe

    for (auto& bucket : ordered_stripes) {
        auto& shard_indices = bucket.shard_indices;
        std::sort(shard_indices.begin(), shard_indices.end(), [&](size_t a, size_t b) {
            return shards[a].sequence < shards[b].sequence;
        });

        const uint64_t req_space = shards[shard_indices[0]].size;

        // PASPA Step 1 & 2: Identify active peers & filter ineligible candidates
        std::vector<PeerInfo> eligible;
        for (const auto& p : online_peers) {
            if (p.workload.is_busy()) continue;
            const uint64_t avail = virtual_avail[p.id];
            if (p.workload.available_storage_bytes > 0 && avail < req_space) {
                continue;
            }
            eligible.push_back(p);
        }
        if (eligible.empty()) {
            eligible = online_peers; // Fallback to busy online peers if capacity saturated
        }

        // Compute max available storage across eligible peers for normalized scoring
        uint64_t max_avail = 1;
        for (const auto& p : eligible) {
            max_avail = std::max(max_avail, virtual_avail[p.id]);
        }

        std::vector<std::string> used_peer_ids;
        std::set<std::string> used_ips;

        // PASPA Step 3 & 4: Evaluate Storage + Health + Load + Locations -> Suitability Score
        auto rank_candidates = [&](size_t step_offset) {
            struct ScoredPeer {
                PeerInfo peer;
                double score = 0.0;
                size_t tie_order = 0;
            };
            std::vector<ScoredPeer> scored;
            scored.reserve(eligible.size());
            for (size_t idx = 0; idx < eligible.size(); ++idx) {
                const auto& p = eligible[idx];
                if (std::find(used_peer_ids.begin(), used_peer_ids.end(), p.id) != used_peer_ids.end()) {
                    continue;
                }

                // 1) Health stability score (0..30)
                double health_score = std::max(0.0, 30.0 - 10.0 * static_cast<double>(p.missed_heartbeats));

                // 2) Storage headroom score (0..25)
                double storage_score = (virtual_avail[p.id] > 0)
                    ? 25.0 * (static_cast<double>(virtual_avail[p.id]) / static_cast<double>(max_avail))
                    : 12.5;

                // 3) Transfer load headroom score (0..25)
                double cap = std::max(1.0, static_cast<double>(p.workload.transfer_capacity));
                double load_ratio = std::min(1.0, static_cast<double>(virtual_transfers[p.id]) / cap);
                double load_score = 25.0 * (1.0 - load_ratio);

                // 4) Cumulative cluster balance across stripes (0..20)
                double balance_score = 20.0 / (1.0 + static_cast<double>(cumulative_assigned[p.id]));

                // 5) Failure-domain IP anti-affinity bonus (+15 when distinct host/IP available)
                double domain_bonus = (used_ips.find(p.address) == used_ips.end()) ? 15.0 : 0.0;

                double total = health_score + storage_score + load_score + balance_score + domain_bonus;
                size_t tie_order = (idx + stripe_index + step_offset) % eligible.size();
                scored.push_back({p, total, tie_order});
            }

            std::sort(scored.begin(), scored.end(), [](const ScoredPeer& a, const ScoredPeer& b) {
                if (std::abs(a.score - b.score) > 1e-4) return a.score > b.score;
                return a.tie_order < b.tie_order;
            });
            return scored;
        };

        size_t stored_in_stripe = 0;

        for (size_t pos_in_stripe = 0; pos_in_stripe < shard_indices.size(); ++pos_in_stripe) {
            const size_t s_idx = shard_indices[pos_in_stripe];
            ShardMeta& shard = shards[s_idx];
            const std::string shard_path = staging_dir + "/" + shard.chunk_id + ".chunk";

            auto candidates = rank_candidates(pos_in_stripe);
            bool placed = false;

            for (const auto& sc : candidates) {
                std::string err;
                if (push_shard(sc.peer, shard, shard_path, err)) {
                    shard.peers = {sc.peer.id};
                    shard.status = "HEALTHY";
                    used_peer_ids.push_back(sc.peer.id);
                    used_ips.insert(sc.peer.address);
                    if (virtual_avail[sc.peer.id] >= shard.size) {
                        virtual_avail[sc.peer.id] -= shard.size;
                    }
                    virtual_transfers[sc.peer.id] += 1;
                    cumulative_assigned[sc.peer.id] += 1;
                    placed = true;
                    ++stored_in_stripe;
                    ss_log << "  [PASPA] Placed " << shard.chunk_id << " -> Peer " << sc.peer.id
                           << " (score=" << std::fixed << std::setprecision(1) << sc.score << ")\n";
                    break;
                } else {
                    ss_log << "  [PASPA] Failed " << shard.chunk_id << " on Peer " << sc.peer.id
                           << ": " << err << " (trying next suitability rank)\n";
                }
            }

            if (!placed) {
                shard.status = "UNAVAILABLE";
                ss_log << "  [PASPA] Shortfall: could not place shard " << shard.chunk_id << " on any peer\n";
            }
        }

        // ARMA: Adaptive Redundancy Management
        // When healthy disjoint spare peers exist beyond the RS(k+m, k) stripe width, place
        // selective direct replicas for primary DATA shards so single-peer failures or bit-rot
        // can recover immediately via low-cost replica copy (HRERA Step 3 YES -> Step 4).
        if (arma.adaptive_replicas && arma.hot_replica_count > 0 &&
            stored_in_stripe == shard_indices.size()) {
            size_t replicas_placed = 0;
            for (size_t s_idx : shard_indices) {
                if (replicas_placed >= arma.hot_replica_count) break;
                ShardMeta& shard = shards[s_idx];
                if (shard.shard_type != "DATA" || shard.status != "HEALTHY") continue;

                auto candidates = rank_candidates(shard_indices.size() + replicas_placed);
                if (candidates.empty()) break; // no disjoint spare peer in cluster

                const std::string shard_path = staging_dir + "/" + shard.chunk_id + ".chunk";
                for (const auto& sc : candidates) {
                    std::string err;
                    if (push_shard(sc.peer, shard, shard_path, err)) {
                        shard.peers.push_back(sc.peer.id);
                        used_peer_ids.push_back(sc.peer.id);
                        used_ips.insert(sc.peer.address);
                        if (virtual_avail[sc.peer.id] >= shard.size) {
                            virtual_avail[sc.peer.id] -= shard.size;
                        }
                        virtual_transfers[sc.peer.id] += 1;
                        cumulative_assigned[sc.peer.id] += 1;
                        ++replicas_placed;
                        ss_log << "  [ARMA] Hot replica " << shard.chunk_id << " -> Peer " << sc.peer.id
                               << " (score=" << std::fixed << std::setprecision(1) << sc.score << ")\n";
                        break;
                    }
                }
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
