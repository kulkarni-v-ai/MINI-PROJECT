// ============================================================================
// metadata_manager.hpp - E6: Metadata & Monitoring Engine
//
// Stores structured JSON metadata and monitoring information (spec §9).
// It stores metadata only — never the file payload.
//
// Files (under metadata/):
//   file_metadata.json  - file ID, filename, sizes, per-chunk metadata,
//                         replica locations
//   events.json         - monitoring events (peer status, recovery events,
//                         detection time, integrity results, retries,
//                         bytes transferred, success/failure)
//   peer_status.json    - last known status of every peer
// ============================================================================
#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "../../models/peer.hpp"
#include "../../third_party/mini_json.hpp"

class MetadataManager {
public:
    explicit MetadataManager(std::string metadata_dir = "metadata");

    // ---- file metadata ------------------------------------------------------
    void init_file_metadata(const std::string& file_id,
                            const std::string& filename,
                            uint64_t original_size,
                            uint64_t chunk_size,
                            const std::vector<ChunkMeta>& chunks);

    void set_chunk_peers(const std::string& chunk_id,
                         const std::vector<std::string>& peers);

    bool load_file_metadata();                 // false if none exists
    bool has_file_metadata() const;

    std::string file_id() const;
    std::string filename() const;
    uint64_t original_size() const;
    uint64_t chunk_size() const;
    std::vector<ChunkMeta> chunks() const;     // ordered by sequence
    bool get_chunk(const std::string& chunk_id, ChunkMeta& out) const;

    // ---- file-id allocation (F001, F002, ...) --------------------------------
    std::string next_file_id();

    // ---- monitoring events -----------------------------------------------------
    void log_event(const std::string& category, const std::string& message,
                   const json::Json& details = json::Json::object());
    json::Json read_events() const;            // {"events":[...]}

    // ---- peer status -----------------------------------------------------------
    void set_peer_status(const std::string& peer_id, PeerState state);
    std::vector<PeerInfo> read_peer_status(
        const std::vector<PeerInfo>& configured) const;

    void reset();  // clear metadata + events for a fresh run

private:
    std::string dir_;
    mutable std::mutex mu_;  // protects shared state across threads

    std::string file_meta_path() const { return dir_ + "/file_metadata.json"; }
    std::string events_path() const    { return dir_ + "/events.json"; }
    std::string status_path() const    { return dir_ + "/peer_status.json"; }
    std::string counter_path() const   { return dir_ + "/file_counter.txt"; }

    static json::Json read_json_file(const std::string& path,
                                     const json::Json& fallback);
    static void write_json_file(const std::string& path, const json::Json& j);
};
