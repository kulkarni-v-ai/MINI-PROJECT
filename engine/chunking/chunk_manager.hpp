// ============================================================================
// chunk_manager.hpp - E1: File & Chunking Engine
//
// Responsibilities (spec §4):
//   - read input file as binary
//   - split into fixed-size chunks (default 64 KB)
//   - generate file ID, chunk IDs, sequence numbers
//   - calculate SHA-256 of every chunk
//   - produce chunk metadata
//
// E1 must NOT decide peer placement and must NOT perform recovery.
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"

class ChunkManager {
public:
    explicit ChunkManager(uint64_t chunk_size = 65536);

    // Split `file_path` (read as binary) into fixed-size chunks.
    // Returns chunk metadata in sequence order. Chunk payloads are written
    // to `staging_dir` as <chunk_id>.chunk so E3 can push them to peers.
    std::vector<ChunkMeta> chunk_file(const std::string& file_id,
                                      const std::string& file_path,
                                      const std::string& staging_dir);

    uint64_t chunk_size() const { return chunk_size_; }

    // SHA-256 helpers (OpenSSL primitive only).
    static std::string sha256_of_file(const std::string& path);
    static std::string sha256_of_bytes(const std::vector<uint8_t>& data);

private:
    uint64_t chunk_size_;
};
