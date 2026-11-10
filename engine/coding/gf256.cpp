// ============================================================================
// gf256.cpp - Galois Field GF(2^8) Arithmetic implementation
// ============================================================================
#include "gf256.hpp"

#include <cstring>

const GF256& GF256::instance() {
    static const GF256 gf;
    return gf;
}

GF256::GF256() {
    std::memset(exp_tbl_, 0, sizeof(exp_tbl_));
    std::memset(log_tbl_, 0, sizeof(log_tbl_));

    // Generate exp and log tables using generator polynomial 0x11D
    uint32_t val = 1;
    for (int i = 0; i < 255; ++i) {
        exp_tbl_[i] = static_cast<uint8_t>(val);
        exp_tbl_[i + 255] = static_cast<uint8_t>(val);
        log_tbl_[val] = static_cast<uint8_t>(i);

        val <<= 1;
        if (val & 0x100) {
            val ^= 0x11D; // primitive poly x^8 + x^4 + x^3 + x^2 + 1
        }
    }
    log_tbl_[0] = 0; // undefined in pure math, but safe default
}

void GF256::mul_add_row(uint8_t* dst, const uint8_t* src, uint8_t scalar, size_t len) const {
    if (scalar == 0) return;
    if (scalar == 1) {
        for (size_t i = 0; i < len; ++i) dst[i] ^= src[i];
        return;
    }
    const uint8_t* exp_scalar = &exp_tbl_[log_tbl_[scalar]];
    for (size_t i = 0; i < len; ++i) {
        if (src[i] != 0) {
            dst[i] ^= exp_scalar[log_tbl_[src[i]]];
        }
    }
}

void GF256::mul_row(uint8_t* dst, const uint8_t* src, uint8_t scalar, size_t len) const {
    if (scalar == 0) {
        std::memset(dst, 0, len);
        return;
    }
    if (scalar == 1) {
        if (dst != src) std::memcpy(dst, src, len);
        return;
    }
    const uint8_t* exp_scalar = &exp_tbl_[log_tbl_[scalar]];
    for (size_t i = 0; i < len; ++i) {
        dst[i] = (src[i] != 0) ? exp_scalar[log_tbl_[src[i]]] : 0;
    }
}

bool GF256::invert_matrix(const std::vector<uint8_t>& mat, size_t k, std::vector<uint8_t>& inv_out) const {
    if (mat.size() != k * k) return false;

    // Augmented matrix [A | I] of dimension k x 2k
    std::vector<uint8_t> aug(k * 2 * k, 0);
    for (size_t r = 0; r < k; ++r) {
        for (size_t c = 0; c < k; ++c) {
            aug[r * (2 * k) + c] = mat[r * k + c];
        }
        aug[r * (2 * k) + (k + r)] = 1; // Identity
    }

    // Forward and back substitution
    for (size_t col = 0; col < k; ++col) {
        // Find pivot
        size_t pivot = col;
        while (pivot < k && aug[pivot * (2 * k) + col] == 0) ++pivot;
        if (pivot == k) return false; // Singular matrix

        // Swap pivot row with current row
        if (pivot != col) {
            for (size_t c = 0; c < 2 * k; ++c) {
                std::swap(aug[col * (2 * k) + c], aug[pivot * (2 * k) + c]);
            }
        }

        // Scale row so pivot is 1
        const uint8_t pivot_val = aug[col * (2 * k) + col];
        if (pivot_val != 1) {
            const uint8_t pivot_inv = inv(pivot_val);
            for (size_t c = 0; c < 2 * k; ++c) {
                aug[col * (2 * k) + c] = mul(aug[col * (2 * k) + c], pivot_inv);
            }
        }

        // Eliminate column in all other rows
        for (size_t r = 0; r < k; ++r) {
            if (r != col) {
                const uint8_t factor = aug[r * (2 * k) + col];
                if (factor != 0) {
                    for (size_t c = 0; c < 2 * k; ++c) {
                        aug[r * (2 * k) + c] ^= mul(aug[col * (2 * k) + c], factor);
                    }
                }
            }
        }
    }

    // Extract right half into inv_out
    inv_out.resize(k * k);
    for (size_t r = 0; r < k; ++r) {
        for (size_t c = 0; c < k; ++c) {
            inv_out[r * k + c] = aug[r * (2 * k) + (k + c)];
        }
    }
    return true;
}
