// ============================================================================
// chunk_manager.cpp - E1: File & Chunking Engine (implementation)
// ============================================================================
#include "chunk_manager.hpp"

#include <openssl/evp.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>

namespace fs = std::filesystem;

ChunkManager::ChunkManager(uint64_t chunk_size) : chunk_size_(chunk_size) {
    if (chunk_size_ == 0) throw std::runtime_error("chunk size must be > 0");
}

// ---- SHA-256 helpers -------------------------------------------------------
// OpenSSL is used ONLY for the cryptographic primitive (spec §2).

static std::string hex_of(const unsigned char* digest, unsigned int len) {
    std::ostringstream oss;
    oss << std::hex << std::setfill('0');
    for (unsigned int i = 0; i < len; ++i)
        oss << std::setw(2) << static_cast<int>(digest[i]);
    return oss.str();
}

std::string ChunkManager::sha256_of_bytes(const std::vector<uint8_t>& data) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) throw std::runtime_error("EVP_MD_CTX_new failed");
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    if (!data.empty())
        EVP_DigestUpdate(ctx, data.data(), data.size());
    EVP_DigestFinal_ex(ctx, digest, &digest_len);
    EVP_MD_CTX_free(ctx);
    return hex_of(digest, digest_len);
}

std::string ChunkManager::sha256_of_file(const std::string& path) {
    std::ifstream in(fs::u8path(path), std::ios::binary);  // UTF-8 path (Windows-safe)
    if (!in) throw std::runtime_error("cannot open file for sha256: " + path);
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) throw std::runtime_error("EVP_MD_CTX_new failed");
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    std::vector<char> buf(1 << 16);
    while (in) {
        in.read(buf.data(), static_cast<std::streamsize>(buf.size()));
        std::streamsize got = in.gcount();
        if (got > 0) EVP_DigestUpdate(ctx, buf.data(), static_cast<size_t>(got));
    }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;
    EVP_DigestFinal_ex(ctx, digest, &digest_len);
    EVP_MD_CTX_free(ctx);
    return hex_of(digest, digest_len);
}

// ---- chunking --------------------------------------------------------------

std::vector<ChunkMeta> ChunkManager::chunk_file(const std::string& file_id,
                                                const std::string& file_path,
                                                const std::string& staging_dir) {
    // Binary files must be handled as binary (spec §19).
    std::ifstream in(fs::u8path(file_path), std::ios::binary);  // UTF-8 path (Windows-safe)
    if (!in) throw std::runtime_error("cannot open input file: " + file_path);

    fs::create_directories(staging_dir);

    const std::string filename = fs::u8path(file_path).filename().u8string();
    std::vector<ChunkMeta> chunks;
    std::vector<char> buffer(chunk_size_);

    uint64_t sequence = 0;
    while (in) {
        in.read(buffer.data(), static_cast<std::streamsize>(chunk_size_));
        std::streamsize got = in.gcount();
        if (got <= 0) break;

        ++sequence;
        ChunkMeta meta;
        meta.file_id  = file_id;
        meta.filename = filename;
        meta.sequence = sequence;
        meta.size     = static_cast<uint64_t>(got);

        // Chunk identity: <file_id>-C<seq:03d>, e.g. F001-C001 (spec §4).
        std::ostringstream cid;
        cid << file_id << "-C" << std::setw(3) << std::setfill('0') << sequence;
        meta.chunk_id = cid.str();

        // SHA-256 of the raw chunk bytes = integrity anchor.
        std::vector<uint8_t> payload(buffer.begin(), buffer.begin() + got);
        meta.sha256 = sha256_of_bytes(payload);

        // Stage the payload so E3 can transmit it. E1 does NOT place chunks.
        const std::string stage_path = staging_dir + "/" + meta.chunk_id + ".chunk";
        std::ofstream out(stage_path, std::ios::binary);
        if (!out) throw std::runtime_error("cannot write staging chunk: " + stage_path);
        out.write(buffer.data(), got);
        out.close();

        chunks.push_back(std::move(meta));
    }

    if (chunks.empty())
        throw std::runtime_error("input file is empty: " + file_path);

    return chunks;
}
