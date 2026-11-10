// ============================================================================
// adaptive_chunker.hpp - Adaptive Chunking & Systematic RS(3,2) Orchestrator (E1)
//
// Interprets adaptive sizing policy based on original file size:
//   Up to 1 MB:     64 KB data shards
//   1 - 10 MB:     256 KB data shards
//   10 - 100 MB:     1 MB data shards
//   100 MB - 1 GB:   4 MB data shards
//   Above 1 GB:      8 MB data shards
//
// Stripes data across RS(3,2): K=2 data shards (D0, D1), M=1 parity shard (P0).
// Any 2 valid shards per stripe reconstruct original file data.
// Handles final incomplete stripe zero-padding and metadata bookkeeping.
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../models/chunk.hpp"
#include "reed_solomon.hpp"

struct AdaptivePolicy {
    uint64_t threshold_1mb   = 1ULL * 1024ULL * 1024ULL;
    uint64_t threshold_10mb  = 10ULL * 1024ULL * 1024ULL;
    uint64_t threshold_100mb = 100ULL * 1024ULL * 1024ULL;
    uint64_t threshold_1gb   = 1024ULL * 1024ULL * 1024ULL;

    uint64_t min_size_bytes     = 4ULL * 1024ULL;
    uint64_t size_under_1mb     = 64ULL * 1024ULL;
    uint64_t size_1_to_10mb     = 256ULL * 1024ULL;
    uint64_t size_10_to_100mb   = 1024ULL * 1024ULL;
    uint64_t size_100mb_to_1gb  = 4ULL * 1024ULL * 1024ULL;
    uint64_t size_above_1gb     = 8ULL * 1024ULL * 1024ULL;
};

class AdaptiveChunker {
public:
    explicit AdaptiveChunker(AdaptivePolicy policy = AdaptivePolicy(),
                             size_t data_shards = 2,
                             size_t parity_shards = 1);

    // DACA: Determine data shard size from input file size and optional peer storage constraint
    uint64_t calculate_shard_size(uint64_t file_size,
                                  uint64_t min_peer_avail_bytes = 0) const;

    // Split input file into RS(k+m, k) stripes, generate parity shards, compute SHA-256,
    // write shards to staging_dir as <shard_id>.chunk, and return shard metadata list.
    std::vector<ShardMeta> encode_file(const std::string& file_id,
                                       const std::string& file_path,
                                       const std::string& staging_dir,
                                       uint64_t& chosen_shard_size_out,
                                       uint64_t min_peer_avail_bytes = 0);

    // Reconstruct file from available shards in shards_dir using RS decoding.
    // Requires at least K valid shards per stripe.
    bool reconstruct_file(const std::string& file_id,
                          uint64_t original_size,
                          const std::vector<ShardMeta>& manifest_shards,
                          const std::string& shards_dir,
                          const std::string& output_file_path,
                          std::string& error_out) const;

    size_t data_shards() const { return k_; }
    size_t parity_shards() const { return m_; }

private:
    AdaptivePolicy policy_;
    size_t k_;
    size_t m_;
    ReedSolomon rs_;
};
