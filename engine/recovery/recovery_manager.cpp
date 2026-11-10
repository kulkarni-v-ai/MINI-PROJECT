// ============================================================================
// recovery_manager.cpp - E5: Recovery & Reconstruction Engine (implementation)
// ============================================================================
#include "recovery_manager.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "../chunking/chunk_manager.hpp"
#include "../communication/tcp_client.hpp"

namespace fs = std::filesystem;

RecoveryManager::RecoveryManager(std::vector<PeerInfo> peers,
                                 MetadataManager& meta, std::string output_dir)
    : peers_(std::move(peers)), meta_(meta), output_dir_(std::move(output_dir)) {}

bool RecoveryManager::is_offline(const std::string& peer_id,
                                 const std::vector<std::string>& offline) const {
    return std::find(offline.begin(), offline.end(), peer_id) != offline.end();
}

bool RecoveryManager::fetch_chunk(const ChunkMeta& chunk,
                                  const std::vector<PeerInfo>& eligible_sources,
                                  bool simulate_interrupt,
                                  RecoveryResult& result) {
    const std::string final_path = staging_dir() + "/" + chunk.chunk_id + ".chunk";
    const std::string tmp_path = final_path + ".tmp";

    for (const auto& src : eligible_sources) {
        bool armed = simulate_interrupt;
        simulate_interrupt = false;  // the T03 hook fires at most once
        // A source may need two attempts if the first transfer is interrupted.
        for (int attempt = 0; attempt < 2; ++attempt) {
            TCPClient client;
            if (!client.connect_to(src.address, src.port)) break;  // next source

            json::Json req = json::Json::object();
            req["type"] = proto::GET_CHUNK;
            req["chunk_id"] = chunk.chunk_id;
            if (armed) req["simulate"] = "interrupt";  // T03 test hook

            json::Json resp;
            std::vector<uint8_t> payload;
            proto::RecvStatus st = client.request(req, {}, resp, payload);

            // Step 12: interrupted transfer -> discard incomplete temporary
            // data and retry the COMPLETE chunk transfer (no byte ranges).
            if (st != proto::RecvStatus::OK) {
                std::error_code ec;
                fs::remove(tmp_path, ec);  // discard partial data
                payload.clear();
                ++result.retry_count;
                json::Json d = json::Json::object();
                d["chunk_id"] = chunk.chunk_id;
                d["source_peer"] = src.id;
                d["recv_status"] = static_cast<long long>(st);
                meta_.log_event("TRANSFER_INTERRUPTED",
                                "transfer interrupted; discarding partial data "
                                "and retrying the complete chunk", d);
                if (armed) { armed = false; continue; }  // retry same source
                break;  // genuine outage: move to the next source
            }

            if (resp["type"].as_string() == proto::TRANSFER_ERROR) {
                ++result.retry_count;
                break;  // source does not hold the chunk: next source
            }
            if (resp["type"].as_string() != proto::CHUNK_RESPONSE) break;

            // Step 7: only a COMPLETE chunk may proceed — write it to a
            // temporary file first (spec §19).
            result.bytes_transferred += payload.size();
            {
                std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(payload.data()),
                          static_cast<std::streamsize>(payload.size()));
            }

            // Steps 8-9: calculate SHA-256 and compare with the expected one.
            const std::string got = ChunkManager::sha256_of_file(tmp_path);
            if (got == chunk.sha256 && payload.size() == chunk.size) {
                // Step 10: valid -> accept (publish atomically).
                std::error_code ec;
                fs::rename(tmp_path, final_path, ec);
                if (ec) {  // rename across devices fallback
                    fs::copy_file(tmp_path, final_path,
                                  fs::copy_options::overwrite_existing, ec);
                    fs::remove(tmp_path, ec);
                }
                json::Json d = json::Json::object();
                d["chunk_id"] = chunk.chunk_id;
                d["source_peer"] = src.id;
                d["sha256"] = got;
                d["verified"] = true;
                meta_.log_event("CHUNK_RECOVERED",
                                "chunk recovered and SHA-256 verified", d);
                return true;
            }

            // Step 11: invalid -> reject, log the integrity failure,
            // try another eligible surviving source.
            ++result.integrity_failures;
            ++result.retry_count;
            std::error_code ec;
            fs::remove(tmp_path, ec);  // reject corrupt data
            json::Json d = json::Json::object();
            d["chunk_id"] = chunk.chunk_id;
            d["source_peer"] = src.id;
            d["expected_sha256"] = chunk.sha256;
            d["actual_sha256"] = got;
            meta_.log_event("INTEGRITY_FAILURE",
                            "SHA-256 mismatch: chunk rejected; trying another "
                            "surviving source", d);
            break;  // try the next source
        }
    }
    return false;
}

RecoveryResult RecoveryManager::recover(
    const std::string& failed_peer,
    const std::vector<std::string>& offline_peers,
    bool simulate_interrupt_once) {
    // Step 1-2: failed peer ID received; read metadata.
    RecoveryResult result;
    result.failed_peer = failed_peer;
    const std::vector<ChunkMeta> chunks = meta_.chunks();

    fs::create_directories(staging_dir());

    // Steps 3-4: find and mark the affected chunks.
    for (const auto& c : chunks)
        if (is_offline(failed_peer, {failed_peer}) &&
            std::find(c.peers.begin(), c.peers.end(), failed_peer) != c.peers.end())
            result.affected_chunks.push_back(c.chunk_id);

    {
        json::Json d = json::Json::object();
        d["failed_peer"] = failed_peer;
        json::Json aff = json::Json::array();
        for (const auto& a : result.affected_chunks) aff.push_back(a);
        d["affected_chunks"] = aff;
        d["affected_count"] = static_cast<long long>(result.affected_chunks.size());
        meta_.log_event("RECOVERY_STARTED",
                        "peer failure received; affected chunks identified", d);
    }

    // Reconstruction needs every chunk: affected chunks must be re-fetched
    // from surviving replicas; unaffected chunks are fetched from any
    // online holder.
    for (const auto& c : chunks) {
        // Step 5: inspect the replica list, remove failed/offline peers,
        // identify surviving eligible sources (order: primary first).
        std::vector<PeerInfo> eligible;
        for (const auto& pid : c.peers) {
            if (is_offline(pid, offline_peers)) continue;
            for (const auto& p : peers_)
                if (p.id == pid) eligible.push_back(p);
        }

        if (eligible.empty()) {
            // Step 13: no surviving source -> UNRECOVERABLE.
            result.unrecoverable_chunks.push_back(c.chunk_id);
            json::Json d = json::Json::object();
            d["chunk_id"] = c.chunk_id;
            d["replica_locations"] = json::Json::array();
            for (const auto& pid : c.peers) d["replica_locations"].push_back(pid);
            meta_.log_event("UNRECOVERABLE",
                            "no surviving source for chunk", d);
            continue;
        }

        const bool arm_interrupt =
            simulate_interrupt_once && result.recovered_chunks.empty();
        if (fetch_chunk(c, eligible, arm_interrupt, result))
            result.recovered_chunks.push_back(c.chunk_id);
        else
            result.unrecoverable_chunks.push_back(c.chunk_id);
    }

    result.success = result.unrecoverable_chunks.empty();
    {
        json::Json d = json::Json::object();
        d["success"] = result.success;
        d["recovered"] = static_cast<long long>(result.recovered_chunks.size());
        d["unrecoverable"] =
            static_cast<long long>(result.unrecoverable_chunks.size());
        d["retry_count"] = result.retry_count;
        d["integrity_failures"] = result.integrity_failures;
        d["bytes_transferred"] = static_cast<long long>(result.bytes_transferred);
        meta_.log_event("RECOVERY_FINISHED",
                        result.success ? "all chunks recovered"
                                       : "recovery incomplete",
                        d);
    }
    return result;
}

bool RecoveryManager::reconstruct(const std::string& original_path,
                                  std::string& recovered_path_out,
                                  std::string& error_out) {
    const std::vector<ChunkMeta> chunks = meta_.chunks();  // already sequence-sorted
    if (chunks.empty()) {
        error_out = "no metadata";
        return false;
    }

    // Never reconstruct if required chunks are missing (spec §19).
    for (const auto& c : chunks) {
        const std::string p = staging_dir() + "/" + c.chunk_id + ".chunk";
        if (!fs::exists(p)) {
            error_out = "missing chunk " + c.chunk_id +
                        " - refusing to reconstruct";
            meta_.log_event("RECONSTRUCTION_REFUSED", error_out);
            return false;
        }
    }

    // Steps 14-16: sort by sequence (metadata is already ordered; the sort
    // below makes the ordering explicit), concatenate, write recovered file.
    std::vector<ChunkMeta> ordered = chunks;
    std::sort(ordered.begin(), ordered.end(),
              [](const ChunkMeta& a, const ChunkMeta& b) {
                  return a.sequence < b.sequence;
              });

    recovered_path_out = output_dir_ + "/recovered_" + meta_.filename();
    std::ofstream out(fs::u8path(recovered_path_out), std::ios::binary | std::ios::trunc);
    if (!out) {
        error_out = "cannot write recovered file";
        return false;
    }
    for (const auto& c : ordered) {
        // Final integrity gate: re-verify every chunk before it joins the
        // reconstructed file.
        const std::string p = staging_dir() + "/" + c.chunk_id + ".chunk";
        if (ChunkManager::sha256_of_file(p) != c.sha256) {
            error_out = "staged chunk failed final SHA-256 check: " + c.chunk_id;
            return false;
        }
        std::ifstream in(p, std::ios::binary);
        out << in.rdbuf();
    }
    out.close();

    // Step 17: byte-compare recovered file against the original.
    std::ifstream a(fs::u8path(original_path), std::ios::binary);
    std::ifstream b(fs::u8path(recovered_path_out), std::ios::binary);
    if (!a || !b) {
        error_out = "cannot open files for comparison";
        return false;
    }
    const std::vector<char> ba((std::istreambuf_iterator<char>(a)),
                               std::istreambuf_iterator<char>());
    const std::vector<char> bb((std::istreambuf_iterator<char>(b)),
                               std::istreambuf_iterator<char>());
    const bool identical = (ba == bb);

    // Step 18: log the final reconstruction result.
    json::Json d = json::Json::object();
    d["original"] = original_path;
    d["recovered"] = recovered_path_out;
    d["original_sha256"] = ChunkManager::sha256_of_file(original_path);
    d["recovered_sha256"] = ChunkManager::sha256_of_file(recovered_path_out);
    d["byte_identical"] = identical;
    meta_.log_event("RECONSTRUCTION",
                    identical ? "recovered file matches the original"
                              : "recovered file DIFFERS from the original", d);

    if (!identical) {
        error_out = "recovered file differs from original";
        return false;
    }
    return true;
}
