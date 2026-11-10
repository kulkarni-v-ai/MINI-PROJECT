// ============================================================================
// reed_solomon.cpp - Systematic Reed-Solomon Erasure Coding implementation
// ============================================================================
#include "reed_solomon.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>

ReedSolomon::ReedSolomon(size_t data_shards, size_t parity_shards)
    : k_(data_shards), m_(parity_shards), n_(data_shards + parity_shards) {
    if (k_ == 0 || m_ == 0 || n_ > 256) {
        throw std::invalid_argument("invalid Reed-Solomon parameters (K, M)");
    }
    build_matrix();
}

void ReedSolomon::build_matrix() {
    matrix_.assign(n_ * k_, 0);

    // Systematic top K rows form the Identity Matrix
    for (size_t r = 0; r < k_; ++r) {
        matrix_[r * k_ + r] = 1;
    }

    // Parity rows (M rows) generated using Cauchy matrix:
    // C[i, j] = 1 / (x_i ^ y_j) where x_i and y_j are disjoint subsets of GF(256)
    // For Cauchy matrix, every square submatrix is non-singular.
    const auto& gf = GF256::instance();
    for (size_t i = 0; i < m_; ++i) {
        const uint8_t xi = static_cast<uint8_t>(k_ + i); // x_i = K + i
        for (size_t j = 0; j < k_; ++j) {
            const uint8_t yj = static_cast<uint8_t>(j);      // y_j = j
            matrix_[(k_ + i) * k_ + j] = gf.div(1, xi ^ yj);
        }
    }
}

void ReedSolomon::encode(const std::vector<const uint8_t*>& data_ptrs,
                         const std::vector<uint8_t*>& parity_ptrs,
                         size_t shard_len) const {
    if (data_ptrs.size() != k_ || parity_ptrs.size() != m_) {
        throw std::invalid_argument("invalid pointer array count for RS encode");
    }

    const auto& gf = GF256::instance();

    for (size_t p = 0; p < m_; ++p) {
        uint8_t* out = parity_ptrs[p];
        std::memset(out, 0, shard_len);

        const size_t row = k_ + p;
        for (size_t c = 0; c < k_; ++c) {
            const uint8_t coeff = matrix_[row * k_ + c];
            gf.mul_add_row(out, data_ptrs[c], coeff, shard_len);
        }
    }
}

bool ReedSolomon::decode(const std::vector<uint8_t*>& shards,
                         const std::vector<bool>& shard_present,
                         size_t shard_len) const {
    if (shards.size() != n_ || shard_present.size() != n_) {
        return false;
    }

    // Check count of available shards
    size_t available_count = 0;
    for (size_t i = 0; i < n_; ++i) {
        if (shard_present[i]) ++available_count;
    }
    if (available_count < k_) return false; // Insufficient surviving shards

    // Check if all data shards are already present
    bool all_data_present = true;
    for (size_t i = 0; i < k_; ++i) {
        if (!shard_present[i]) {
            all_data_present = false;
            break;
        }
    }
    if (all_data_present) {
        // Data shards are complete; if any parity is missing, re-encode it
        std::vector<const uint8_t*> data_ptrs(k_);
        for (size_t i = 0; i < k_; ++i) data_ptrs[i] = shards[i];

        std::vector<uint8_t*> par_ptrs(m_);
        for (size_t p = 0; p < m_; ++p) par_ptrs[p] = shards[k_ + p];

        encode(data_ptrs, par_ptrs, shard_len);
        return true;
    }

    // Select K available rows to construct the submatrix
    std::vector<size_t> chosen_rows;
    chosen_rows.reserve(k_);
    for (size_t i = 0; i < n_ && chosen_rows.size() < k_; ++i) {
        if (shard_present[i]) chosen_rows.push_back(i);
    }

    std::vector<uint8_t> submatrix(k_ * k_);
    for (size_t r = 0; r < k_; ++r) {
        const size_t orig_row = chosen_rows[r];
        for (size_t c = 0; c < k_; ++c) {
            submatrix[r * k_ + c] = matrix_[orig_row * k_ + c];
        }
    }

    // Invert the submatrix over GF(256)
    const auto& gf = GF256::instance();
    std::vector<uint8_t> inv_matrix;
    if (!gf.invert_matrix(submatrix, k_, inv_matrix)) {
        return false;
    }

    // Recover the K original data shards:
    // data[c] = sum_{r=0}^{k-1} (inv_matrix[c, r] * chosen_shards[r])
    std::vector<std::vector<uint8_t>> recovered_data(k_, std::vector<uint8_t>(shard_len, 0));
    for (size_t c = 0; c < k_; ++c) {
        for (size_t r = 0; r < k_; ++r) {
            const uint8_t coeff = inv_matrix[c * k_ + r];
            gf.mul_add_row(recovered_data[c].data(), shards[chosen_rows[r]], coeff, shard_len);
        }
    }

    // Copy recovered data shards into output buffers where missing
    for (size_t c = 0; c < k_; ++c) {
        if (!shard_present[c]) {
            std::memcpy(shards[c], recovered_data[c].data(), shard_len);
        }
    }

    // If any parity shard was also missing, re-encode it from recovered data shards
    std::vector<const uint8_t*> data_ptrs(k_);
    for (size_t i = 0; i < k_; ++i) {
        data_ptrs[i] = shards[i];
    }
    for (size_t p = 0; p < m_; ++p) {
        if (!shard_present[k_ + p]) {
            std::memset(shards[k_ + p], 0, shard_len);
            const size_t row = k_ + p;
            for (size_t c = 0; c < k_; ++c) {
                const uint8_t coeff = matrix_[row * k_ + c];
                gf.mul_add_row(shards[k_ + p], data_ptrs[c], coeff, shard_len);
            }
        }
    }

    return true;
}
