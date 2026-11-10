// ============================================================================
// reed_solomon.hpp - Systematic Reed-Solomon Erasure Coding over GF(256)
//
// Supports systematic (N, K) erasure coding:
//   - K data shards (D0, D1, ...)
//   - M parity shards (P0, ...)
//   - N = K + M total shards
//
// For RS(3,2): K=2, M=1, N=3. Any 2 valid shards reconstruct the original stripe.
// Architecture is generalized so K and M can be extended.
// ============================================================================
#pragma once

#include <cstdint>
#include <vector>

#include "gf256.hpp"

class ReedSolomon {
public:
    // data_shards (K), parity_shards (M)
    ReedSolomon(size_t data_shards = 2, size_t parity_shards = 1);

    size_t data_shards() const { return k_; }
    size_t parity_shards() const { return m_; }
    size_t total_shards() const { return n_; }
    const std::vector<uint8_t>& encoding_matrix() const { return matrix_; }

    // Encode K data shards into M parity shards.
    // Each data shard and parity shard must have length `shard_len`.
    void encode(const std::vector<const uint8_t*>& data_ptrs,
                const std::vector<uint8_t*>& parity_ptrs,
                size_t shard_len) const;

    // Decode: Reconstruct any missing shards.
    // `shards`: vector of pointers to N shard buffers (each of length `shard_len`).
    // `shard_present`: boolean mask of size N (true if shard is available and valid).
    // Missing buffers with `shard_present[i] == false` must point to allocated memory
    // where reconstructed bytes will be written.
    // Returns true if reconstruction succeeds (requires at least K valid shards).
    bool decode(const std::vector<uint8_t*>& shards,
                const std::vector<bool>& shard_present,
                size_t shard_len) const;

private:
    size_t k_; // data shards
    size_t m_; // parity shards
    size_t n_; // total shards = k + m
    std::vector<uint8_t> matrix_; // N x K encoding matrix

    void build_matrix();
};
