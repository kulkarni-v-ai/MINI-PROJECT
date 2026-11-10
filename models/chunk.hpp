// ============================================================================
// chunk.hpp - Core data model: Shard & Chunk metadata (E1/E6)
//
// In Gate-2:
//   - Systematic Reed-Solomon RS(3,2) shards
//   - Records File ID, Stripe ID, Shard ID, Shard Type (DATA/PARITY), Sequence,
//     Expected size, Original data length, Padding length, SHA-256, Peer location
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../third_party/mini_json.hpp"

struct ChunkMeta {
    std::string chunk_id;              // e.g. "F001-C001" or "F001-S001-D0"
    std::string file_id;               // e.g. "F001"
    std::string filename;              // original file name
    std::string stripe_id;             // e.g. "S001"
    std::string shard_type = "DATA";   // "DATA" or "PARITY"
    uint64_t    sequence = 0;          // 1-based original position
    uint64_t    size = 0;              // expected shard size in bytes
    uint64_t    original_data_length = 0; // bytes of original payload before zero-pad
    uint64_t    padding_length = 0;       // padding bytes added for stripe alignment
    std::string sha256;                // hex digest of the shard payload
    std::string status = "HEALTHY";    // "HEALTHY", "UNAVAILABLE", "CORRUPT", "RECOVERING"
    std::vector<std::string> peers;    // replica or shard peer locations, e.g. ["A"]

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["chunk_id"]             = chunk_id;
        j["sequence"]             = static_cast<long long>(sequence);
        j["size"]                 = static_cast<long long>(size);
        j["original_data_length"] = static_cast<long long>(original_data_length);
        j["padding_length"]       = static_cast<long long>(padding_length);
        j["sha256"]               = sha256;
        j["stripe_id"]            = stripe_id;
        j["shard_type"]           = shard_type;
        j["status"]               = status;
        json::Json locs = json::Json::array();
        for (const auto& p : peers) locs.push_back(p);
        j["peers"] = locs;
        return j;
    }

    static ChunkMeta from_json(const std::string& chunk_id,
                               const std::string& file_id,
                               const std::string& filename,
                               const json::Json& j) {
        ChunkMeta c;
        c.chunk_id = chunk_id;
        c.file_id  = file_id;
        c.filename = filename;
        c.sequence = static_cast<uint64_t>(j["sequence"].as_int());
        c.size     = static_cast<uint64_t>(j["size"].as_int());
        c.sha256   = j["sha256"].as_string();
        if (j.contains("stripe_id")) c.stripe_id = j["stripe_id"].as_string();
        if (j.contains("shard_type")) c.shard_type = j["shard_type"].as_string();
        if (j.contains("status")) c.status = j["status"].as_string();
        if (j.contains("original_data_length"))
            c.original_data_length = static_cast<uint64_t>(j["original_data_length"].as_int());
        else
            c.original_data_length = c.size;
        if (j.contains("padding_length"))
            c.padding_length = static_cast<uint64_t>(j["padding_length"].as_int());
        for (const auto& p : j["peers"].items()) c.peers.push_back(p.as_string());
        return c;
    }
};

using ShardMeta = ChunkMeta;
