// ============================================================================
// gf256.hpp - Galois Field GF(2^8) Arithmetic for Reed-Solomon Erasure Coding
//
// Primitive polynomial: x^8 + x^4 + x^3 + x^2 + 1 (0x11D).
// Fast table-driven multiplication and division using exp/log lookup tables.
// Supports Gaussian elimination matrix inversion over GF(256).
// ============================================================================
#pragma once

#include <cstdint>
#include <vector>
#include <stdexcept>

class GF256 {
public:
    static const GF256& instance();

    uint8_t add(uint8_t a, uint8_t b) const { return a ^ b; }
    uint8_t sub(uint8_t a, uint8_t b) const { return a ^ b; }

    uint8_t mul(uint8_t a, uint8_t b) const {
        if (a == 0 || b == 0) return 0;
        return exp_tbl_[log_tbl_[a] + log_tbl_[b]];
    }

    uint8_t div(uint8_t a, uint8_t b) const {
        if (b == 0) throw std::domain_error("GF(256) division by zero");
        if (a == 0) return 0;
        int diff = static_cast<int>(log_tbl_[a]) - static_cast<int>(log_tbl_[b]);
        if (diff < 0) diff += 255;
        return exp_tbl_[diff];
    }

    uint8_t inv(uint8_t a) const {
        if (a == 0) throw std::domain_error("GF(256) inverse of zero");
        return exp_tbl_[255 - log_tbl_[a]];
    }

    // Multiply an entire buffer by a scalar: dst[i] ^= scalar * src[i]
    void mul_add_row(uint8_t* dst, const uint8_t* src, uint8_t scalar, size_t len) const;

    // Multiply an entire buffer by a scalar: dst[i] = scalar * src[i]
    void mul_row(uint8_t* dst, const uint8_t* src, uint8_t scalar, size_t len) const;

    // Invert a square matrix of size k x k over GF(256) using Gaussian elimination.
    // Matrix is stored in row-major order: matrix[row * k + col].
    bool invert_matrix(const std::vector<uint8_t>& mat, size_t k, std::vector<uint8_t>& inv_out) const;

private:
    GF256();
    uint8_t exp_tbl_[512]; // duplicated to avoid modulo
    uint8_t log_tbl_[256];
};
