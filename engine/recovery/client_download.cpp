// ============================================================================
// client_download.cpp - Direct Peer-to-Client Download & Local RS Reconstruction
// ============================================================================
#include "client_download.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>

#include "../chunking/chunk_manager.hpp"
#include "../coding/adaptive_chunker.hpp"
#include "../communication/protocol.hpp"
#include "../communication/tcp_client.hpp"

namespace fs = std::filesystem;

ClientDownload::ClientDownload(std::string coordinator_address,
                               uint16_t coordinator_port,
                               std::string output_dir,
                               std::string session_token,
                               std::string auth_token)
    : coord_addr_(std::move(coordinator_address)),
      coord_port_(coordinator_port),
      output_dir_(std::move(output_dir)),
      session_token_(std::move(session_token)),
      auth_token_(std::move(auth_token)) {}

bool ClientDownload::ensure_session() {
    if (!session_token_.empty()) return true;
    if (auth_token_.empty()) return false;

    TCPClient client(3000, 3000);
    if (!client.connect_to(coord_addr_, coord_port_)) return false;

    json::Json req = json::Json::object();
    req["type"] = proto::SESSION_AUTH;
    req["client_id"] = "download-client";
    req["auth_token"] = auth_token_;

    json::Json resp;
    std::vector<uint8_t> pl;
    if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
        resp.contains("type") && resp["type"].as_string() == proto::SESSION_AUTH_ACK &&
        resp.contains("session_token")) {
        session_token_ = resp["session_token"].as_string();
        return true;
    }
    return false;
}

void ClientDownload::report_shard_issue(const std::string& file_id,
                                        const std::string& shard_id,
                                        const std::string& peer_id,
                                        const std::string& issue,
                                        CoordinatorService* in_proc_coordinator) {
    if (in_proc_coordinator) {
        in_proc_coordinator->catalog().mark_shard_status(
            file_id, shard_id, issue == "CORRUPT" ? "CORRUPT" : "UNAVAILABLE");
        in_proc_coordinator->schedule_recovery(file_id, true);
        return;
    }

    ensure_session();
    TCPClient client(2000, 2000);
    if (!client.connect_to(coord_addr_, coord_port_)) return;

    json::Json req = json::Json::object();
    req["type"] = proto::REPORT_SHARD_ISSUE;
    if (!session_token_.empty()) req["session_token"] = session_token_;
    req["file_id"] = file_id;
    req["shard_id"] = shard_id;
    req["peer_id"] = peer_id;
    req["issue"] = issue;

    json::Json resp;
    std::vector<uint8_t> pl;
    client.request(req, {}, resp, pl);
}

bool ClientDownload::fetch_shard_from_peer(const std::string& peer_addr,
                                           uint16_t peer_port,
                                           const std::string& shard_id,
                                           const std::string& expected_sha256,
                                           const std::string& save_path,
                                           std::string& error_out,
                                           bool& integrity_mismatch_out) {
    integrity_mismatch_out = false;
    TCPClient client(3000, 5000);
    if (!client.connect_to(peer_addr, peer_port)) {
        error_out = "connection refused to peer at " + peer_addr + ":" + std::to_string(peer_port);
        return false;
    }

    json::Json req = json::Json::object();
    req["type"] = proto::GET_CHUNK;
    req["chunk_id"] = shard_id;

    json::Json resp;
    std::vector<uint8_t> payload;
    if (client.request(req, {}, resp, payload) != proto::RecvStatus::OK) {
        error_out = "network failure retrieving shard " + shard_id;
        return false;
    }

    if (resp["type"].as_string() != proto::CHUNK_RESPONSE) {
        error_out = "peer rejected shard request: " +
                    (resp.contains("reason") ? resp["reason"].as_string() : "unknown error");
        return false;
    }

    // Verify SHA-256 before accepting
    const std::string actual_sha = ChunkManager::sha256_of_bytes(payload);
    if (!expected_sha256.empty() && actual_sha != expected_sha256) {
        integrity_mismatch_out = true;
        error_out = "integrity error: SHA-256 mismatch for shard " + shard_id;
        return false;
    }

    // Write to staging
    std::ofstream out(save_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        error_out = "failed to create local file: " + save_path;
        return false;
    }
    out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    return true;
}

DownloadResult ClientDownload::download(const std::string& file_id,
                                        DownloadSource source,
                                        CoordinatorService* in_proc_coordinator) {
    DownloadResult res;
    res.file_id = file_id;
    res.source_used = source;

    FileRecord rec;
    std::vector<PeerInfo> peers;
    size_t default_k = 2;
    size_t default_m = 1;

    // Retrieve file manifest & cluster peers
    if (in_proc_coordinator) {
        auto& catalog = in_proc_coordinator->catalog();
        if (!catalog.get_file(file_id, rec)) {
            res.message = "file ID not found in catalogue: " + file_id;
            return res;
        }

        // On-demand distribution if still STORED
        if (rec.status == FileDistributionStatus::STORED) {
            in_proc_coordinator->process_uploaded_file(file_id, rec);
            catalog.get_file(file_id, rec);
        }
        peers = in_proc_coordinator->registry().get_all_peers();
        default_k = in_proc_coordinator->rs_k();
        default_m = in_proc_coordinator->rs_m();
    } else {
        if (session_token_.empty()) ensure_session();

        bool need_retry = false;
        auto query_manifest = [&](bool allow_reauth) -> bool {
            need_retry = false;
            TCPClient client(3000, 10000);
            if (!client.connect_to(coord_addr_, coord_port_)) {
                res.message = "cannot connect to Coordinator at " + coord_addr_ + ":" + std::to_string(coord_port_);
                return false;
            }

            json::Json req = json::Json::object();
            req["type"] = "DOWNLOAD_MANIFEST";
            req["file_id"] = file_id;
            if (!session_token_.empty()) req["session_token"] = session_token_;
            else if (!auth_token_.empty()) req["auth_token"] = auth_token_;

            json::Json resp;
            std::vector<uint8_t> pl;
            if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK) {
                res.message = "failed to obtain file manifest from Coordinator";
                return false;
            }
            if (resp.contains("type") && resp["type"].as_string() == proto::AUTH_ERROR) {
                if (allow_reauth && !auth_token_.empty()) {
                    session_token_.clear();
                    if (ensure_session()) {
                        need_retry = true;
                        return true;
                    }
                }
                res.message = resp.contains("reason") ? resp["reason"].as_string() : "authentication failed";
                return false;
            }
            if (!resp.contains("manifest")) {
                res.message = resp.contains("reason")
                    ? resp["reason"].as_string()
                    : "failed to obtain file manifest from Coordinator";
                return false;
            }

            rec = FileRecord::from_json(resp["manifest"]);
            if (resp.contains("peers") && resp["peers"].is_array()) {
                for (const auto& pi : resp["peers"].items()) {
                    peers.push_back(PeerInfo::from_json(pi));
                }
            }
            return true;
        };

        if (!query_manifest(true)) return res;
        if (need_retry && !query_manifest(false)) return res;
    }

    res.filename = rec.filename;
    res.file_size = rec.size;
    res.expected_sha256 = rec.sha256;
    res.stripes_total = rec.stripes_count;

    std::error_code ec;
    fs::create_directories(output_dir_, ec);
    const std::string final_out_path = output_dir_ + "/" + rec.filename;
    res.output_path = final_out_path;

    // Path A: Explicit Central Fallback (--source central)
    if (source == DownloadSource::CENTRAL) {
        if (!rec.central_path.empty() && fs::exists(rec.central_path, ec)) {
            fs::copy_file(rec.central_path, final_out_path, fs::copy_options::overwrite_existing, ec);
            if (ec) {
                res.message = "failed to copy central file: " + ec.message();
                return res;
            }
        } else if (!in_proc_coordinator) {
            // Stream over TCP from remote Coordinator
            ensure_session();
            std::ofstream out(final_out_path, std::ios::binary | std::ios::trunc);
            if (!out) {
                res.message = "failed to open output path: " + final_out_path;
                return res;
            }
            uint64_t offset = 0;
            bool done = false;
            while (!done) {
                TCPClient client(3000, 10000);
                if (!client.connect_to(coord_addr_, coord_port_)) {
                    res.message = "cannot connect to Coordinator for central download";
                    return res;
                }
                json::Json req = json::Json::object();
                req["type"] = proto::DOWNLOAD_CENTRAL;
                req["file_id"] = file_id;
                req["offset"] = static_cast<long long>(offset);
                req["length"] = static_cast<long long>(4ULL * 1024ULL * 1024ULL);
                if (!session_token_.empty()) req["session_token"] = session_token_;

                json::Json resp;
                std::vector<uint8_t> chunk;
                if (client.request(req, {}, resp, chunk) != proto::RecvStatus::OK ||
                    resp["type"].as_string() != proto::DOWNLOAD_CENTRAL_ACK) {
                    res.message = resp.contains("reason")
                        ? resp["reason"].as_string()
                        : "central download transfer error";
                    return res;
                }
                if (!chunk.empty()) {
                    out.write(reinterpret_cast<const char*>(chunk.data()),
                              static_cast<std::streamsize>(chunk.size()));
                    offset += chunk.size();
                }
                done = (resp.contains("eof") && resp["eof"].as_bool()) || chunk.empty();
            }
        } else {
            res.message = "central original file is missing at: " + rec.central_path;
            return res;
        }

        res.actual_sha256 = ChunkManager::sha256_of_file(final_out_path);
        res.success = (res.actual_sha256 == res.expected_sha256);
        res.message = res.success
            ? "File retrieved from Coordinator central staging (verified SHA-256)"
            : "Central fallback SHA-256 verification failed";
        return res;
    }

    // Path B: Multi-Peer Direct Shard Retrieval with Local RS(k+m, k) Reconstruction
    const size_t need_k = rec.k > 0 ? rec.k : default_k;
    const size_t total_m = rec.m > 0 ? rec.m : default_m;

    std::map<std::string, PeerInfo> peer_map;
    for (const auto& p : peers) peer_map[p.id] = p;

    const std::string local_shards_dir = output_dir_ + "/shards_" + file_id;
    fs::remove_all(local_shards_dir, ec);
    fs::create_directories(local_shards_dir, ec);

    // MGFRA: Group shards by stripe ordered numerically by minimum sequence number
    struct StripeGroup {
        std::string stripe_id;
        uint64_t min_seq = std::numeric_limits<uint64_t>::max();
        std::vector<ShardMeta> shards;
    };
    std::map<std::string, size_t> stripe_idx_map;
    std::vector<StripeGroup> stripes;
    for (const auto& sh : rec.shards) {
        auto it = stripe_idx_map.find(sh.stripe_id);
        if (it == stripe_idx_map.end()) {
            stripe_idx_map[sh.stripe_id] = stripes.size();
            stripes.push_back({sh.stripe_id, sh.sequence, {sh}});
        } else {
            auto& sg = stripes[it->second];
            sg.min_seq = std::min(sg.min_seq, sh.sequence);
            sg.shards.push_back(sh);
        }
    }
    std::sort(stripes.begin(), stripes.end(),
              [](const StripeGroup& a, const StripeGroup& b) {
                  if (a.min_seq != b.min_seq) return a.min_seq < b.min_seq;
                  return a.stripe_id < b.stripe_id;
              });

    if (stripes.empty()) {
        res.message = "manifest contains no distributed stripes (check cluster peers)";
        return res;
    }

    size_t total_retrieved = 0;

    for (auto& sg : stripes) {
        const std::string& stripe_id = sg.stripe_id;
        auto stripe_shards = sg.shards;

        // Try HEALTHY shards on ONLINE peers first
        auto shard_rank = [&](const ShardMeta& sm) -> int {
            if (sm.peers.empty()) return 4;
            bool any_online = false;
            for (const auto& pid : sm.peers) {
                auto it = peer_map.find(pid);
                if (it != peer_map.end() && it->second.state == PeerHealth::ONLINE) {
                    any_online = true;
                    break;
                }
            }
            if (sm.status == "HEALTHY" && any_online) return 0;
            if (any_online) return 1;
            if (sm.status == "HEALTHY") return 2;
            return 3;
        };
        std::stable_sort(stripe_shards.begin(), stripe_shards.end(),
                         [&](const ShardMeta& a, const ShardMeta& b) {
                             return shard_rank(a) < shard_rank(b);
                         });

        size_t valid_in_stripe = 0;

        for (const auto& sm : stripe_shards) {
            if (valid_in_stripe >= need_k) break;
            if (sm.peers.empty()) continue;

            for (const auto& peer_id : sm.peers) {
                auto it = peer_map.find(peer_id);
                if (it == peer_map.end()) continue;

                const std::string save_file = local_shards_dir + "/" + sm.chunk_id + ".chunk";
                std::string err;
                bool hash_mismatch = false;

                // DIRECT TCP retrieval from storage peer (Coordinator never relays shard bytes)
                if (fetch_shard_from_peer(it->second.address, it->second.port,
                                          sm.chunk_id, sm.sha256, save_file, err, hash_mismatch)) {
                    ++valid_in_stripe;
                    ++total_retrieved;
                    break;
                } else if (hash_mismatch) {
                    ++res.corrupted_shards_detected;
                    std::cout << "[Client] Hash mismatch on " << sm.chunk_id
                              << " from Peer " << peer_id << " -> reporting CORRUPT to Coordinator\n";
                    report_shard_issue(file_id, sm.chunk_id, peer_id, "CORRUPT", in_proc_coordinator);
                } else if (it->second.state == PeerHealth::ONLINE && sm.status == "HEALTHY") {
                    std::cout << "[Client] Shard " << sm.chunk_id
                              << " unreachable on ONLINE Peer " << peer_id << " -> reporting UNAVAILABLE\n";
                    report_shard_issue(file_id, sm.chunk_id, peer_id, "MISSING", in_proc_coordinator);
                }
            }
        }

        if (valid_in_stripe < need_k) {
            fs::remove_all(local_shards_dir, ec);
            res.message = "insufficient surviving peer shards for stripe " + stripe_id +
                          " (retrieved " + std::to_string(valid_in_stripe) + "/" +
                          std::to_string(need_k) + " required)";
            return res;
        }
    }

    res.shards_retrieved = total_retrieved;

    // Local Reed-Solomon Reconstruction directly on Client
    AdaptiveChunker chunker(AdaptivePolicy(), need_k, total_m);
    std::string rec_err;
    if (!chunker.reconstruct_file(file_id, rec.size, rec.shards, local_shards_dir, final_out_path, rec_err)) {
        fs::remove_all(local_shards_dir, ec);
        res.message = "Reed-Solomon client reconstruction failed: " + rec_err;
        return res;
    }

    res.actual_sha256 = ChunkManager::sha256_of_file(final_out_path);
    if (res.actual_sha256 != res.expected_sha256) {
        fs::remove_all(local_shards_dir, ec);
        res.message = "reconstructed file SHA-256 mismatch (actual=" + res.actual_sha256 +
                      " expected=" + res.expected_sha256 + ")";
        return res;
    }

    fs::remove_all(local_shards_dir, ec);

    res.success = true;
    res.message = "Direct peer-to-client download and RS(" +
                  std::to_string(need_k + total_m) + "," + std::to_string(need_k) +
                  ") reconstruction complete. Integrity verified.";
    return res;
}
