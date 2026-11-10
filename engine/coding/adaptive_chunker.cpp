// ============================================================================
// adaptive_chunker.cpp - Adaptive Chunking & Systematic RS(3,2) implementation
// ============================================================================
#include "adaptive_chunker.hpp"

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>

#include "../chunking/chunk_manager.hpp"

namespace fs = std::filesystem;

AdaptiveChunker::AdaptiveChunker(AdaptivePolicy policy, size_t data_shards, size_t parity_shards)
    : policy_(policy), k_(data_shards), m_(parity_shards), rs_(data_shards, parity_shards) {}

uint64_t AdaptiveChunker::calculate_shard_size(uint64_t file_size) const {
    if (file_size <= policy_.threshold_1mb) return policy_.size_under_1mb;
    if (file_size <= policy_.threshold_10mb) return policy_.size_1_to_10mb;
    if (file_size <= policy_.threshold_100mb) return policy_.size_10_to_100mb;
    if (file_size <= policy_.threshold_1gb) return policy_.size_100mb_to_1gb;
    return policy_.size_above_1gb;
}

std::vector<ShardMeta> AdaptiveChunker::encode_file(const std::string& file_id,
                                                   const std::string& file_path,
                                                   const std::string& staging_dir,
                                                   uint64_t& chosen_shard_size_out) {
    std::ifstream in(file_path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open file for encoding: " + file_path);

    in.seekg(0, std::ios::end);
    const uint64_t file_size = static_cast<uint64_t>(in.tellg());
    in.seekg(0, std::ios::beg);

    fs::create_directories(staging_dir);

    const uint64_t shard_size = calculate_shard_size(file_size);
    chosen_shard_size_out = shard_size;

    std::vector<ShardMeta> all_shards;
    uint64_t seq = 1;
    uint64_t stripe_idx = 0;
    uint64_t bytes_remaining = file_size;

    const std::string filename = fs::path(file_path).filename().string();

    while (bytes_remaining > 0 || (file_size == 0 && stripe_idx == 0)) {
        std::ostringstream ss_stripe;
        ss_stripe << "S" << std::setfill('0') << std::setw(3) << stripe_idx;
        const std::string stripe_id = ss_stripe.str();

        // Prepare data shards buffers
        std::vector<std::vector<uint8_t>> data_bufs(k_, std::vector<uint8_t>(shard_size, 0));
        std::vector<uint64_t> orig_data_lengths(k_, 0);
        std::vector<uint64_t> padding_lengths(k_, 0);

        for (size_t d = 0; d < k_; ++d) {
            if (bytes_remaining > 0) {
                const uint64_t to_read = std::min(bytes_remaining, shard_size);
                in.read(reinterpret_cast<char*>(data_bufs[d].data()), static_cast<std::streamsize>(to_read));
                orig_data_lengths[d] = to_read;
                padding_lengths[d] = shard_size - to_read;
                bytes_remaining -= to_read;
            } else {
                orig_data_lengths[d] = 0;
                padding_lengths[d] = shard_size;
            }
        }

        // Prepare parity shards buffers
        std::vector<std::vector<uint8_t>> parity_bufs(m_, std::vector<uint8_t>(shard_size, 0));

        std::vector<const uint8_t*> data_ptrs(k_);
        for (size_t d = 0; d < k_; ++d) data_ptrs[d] = data_bufs[d].data();

        std::vector<uint8_t*> parity_ptrs(m_);
        for (size_t p = 0; p < m_; ++p) parity_ptrs[p] = parity_bufs[p].data();

        // Reed-Solomon systematic encoding
        rs_.encode(data_ptrs, parity_ptrs, shard_size);

        // Save data shards to staging
        for (size_t d = 0; d < k_; ++d) {
            const std::string shard_id = file_id + "-" + stripe_id + "-D" + std::to_string(d);
            const std::string out_path = staging_dir + "/" + shard_id + ".chunk";
            {
                std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(data_bufs[d].data()), static_cast<std::streamsize>(shard_size));
            }

            ShardMeta sm;
            sm.chunk_id = shard_id;
            sm.file_id = file_id;
            sm.filename = filename;
            sm.stripe_id = stripe_id;
            sm.shard_type = "DATA";
            sm.sequence = seq++;
            sm.size = shard_size;
            sm.original_data_length = orig_data_lengths[d];
            sm.padding_length = padding_lengths[d];
            sm.sha256 = ChunkManager::sha256_of_bytes(data_bufs[d]);
            all_shards.push_back(sm);
        }

        // Save parity shards to staging
        for (size_t p = 0; p < m_; ++p) {
            const std::string shard_id = file_id + "-" + stripe_id + "-P" + std::to_string(p);
            const std::string out_path = staging_dir + "/" + shard_id + ".chunk";
            {
                std::ofstream out(out_path, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(parity_bufs[p].data()), static_cast<std::streamsize>(shard_size));
            }

            ShardMeta sm;
            sm.chunk_id = shard_id;
            sm.file_id = file_id;
            sm.filename = filename;
            sm.stripe_id = stripe_id;
            sm.shard_type = "PARITY";
            sm.sequence = seq++;
            sm.size = shard_size;
            sm.original_data_length = 0;
            sm.padding_length = 0;
            sm.sha256 = ChunkManager::sha256_of_bytes(parity_bufs[p]);
            all_shards.push_back(sm);
        }

        ++stripe_idx;
        if (file_size == 0) break;
    }

    return all_shards;
}

bool AdaptiveChunker::reconstruct_file(const std::string& file_id,
                                      uint64_t original_size,
                                      const std::vector<ShardMeta>& manifest_shards,
                                      const std::string& shards_dir,
                                      const std::string& output_file_path,
                                      std::string& error_out) const {
    if (manifest_shards.empty()) {
        error_out = "manifest contains no shards";
        return false;
    }

    // Group shards by stripe
    std::map<std::string, std::vector<ShardMeta>> stripes;
    for (const auto& sm : manifest_shards) {
        if (sm.file_id == file_id) {
            stripes[sm.stripe_id].push_back(sm);
        }
    }

    fs::path out_p(output_file_path);
    if (out_p.has_parent_path()) {
        fs::create_directories(out_p.parent_path());
    }

    std::ofstream out(output_file_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        error_out = "failed to open output destination: " + output_file_path;
        return false;
    }

    uint64_t total_written = 0;

    for (const auto& kv : stripes) {
        const std::string& stripe_id = kv.first;
        const auto& stripe_shards = kv.second;

        const size_t total_n = k_ + m_;
        std::vector<std::vector<uint8_t>> shard_bufs(total_n);
        std::vector<bool> shard_present(total_n, false);
        uint64_t shard_len = 0;
        std::vector<uint64_t> orig_lens(k_, 0);

        for (const auto& sm : stripe_shards) {
            size_t idx = 0;
            if (sm.shard_type == "DATA") {
                // Parse DX
                size_t d_pos = sm.chunk_id.rfind("-D");
                if (d_pos != std::string::npos) {
                    idx = std::stoul(sm.chunk_id.substr(d_pos + 2));
                }
                if (idx < k_) orig_lens[idx] = sm.original_data_length;
            } else {
                // Parse PX
                size_t p_pos = sm.chunk_id.rfind("-P");
                if (p_pos != std::string::npos) {
                    idx = k_ + std::stoul(sm.chunk_id.substr(p_pos + 2));
                }
            }
            if (idx >= total_n) continue;

            shard_len = sm.size;
            shard_bufs[idx].assign(shard_len, 0);

            const std::string path = shards_dir + "/" + sm.chunk_id + ".chunk";
            if (fs::exists(path) && fs::file_size(path) == shard_len) {
                std::ifstream sh_in(path, std::ios::binary);
                sh_in.read(reinterpret_cast<char*>(shard_bufs[idx].data()), static_cast<std::streamsize>(shard_len));
                if (ChunkManager::sha256_of_bytes(shard_bufs[idx]) == sm.sha256) {
                    shard_present[idx] = true;
                }
            }
        }

        // Count available shards
        size_t available = 0;
        for (bool b : shard_present) if (b) ++available;

        if (available < k_) {
            error_out = "insufficient surviving shards for stripe " + stripe_id +
                        " (have " + std::to_string(available) + ", need " + std::to_string(k_) + ")";
            return false;
        }

        // Decode / reconstruct missing shards
        std::vector<uint8_t*> shard_ptrs(total_n);
        for (size_t i = 0; i < total_n; ++i) shard_ptrs[i] = shard_bufs[i].data();

        if (!rs_.decode(shard_ptrs, shard_present, shard_len)) {
            error_out = "Reed-Solomon decoding failed on stripe " + stripe_id;
            return false;
        }

        // Write original data bytes from reconstructed data shards
        for (size_t d = 0; d < k_; ++d) {
            const uint64_t valid_bytes = std::min(orig_lens[d], original_size - total_written);
            if (valid_bytes > 0) {
                out.write(reinterpret_cast<const char*>(shard_bufs[d].data()), static_cast<std::streamsize>(valid_bytes));
                total_written += valid_bytes;
            }
        }
    }

    out.close();

    if (total_written != original_size) {
        error_out = "reconstructed file size mismatch (wrote " + std::to_string(total_written) +
                    ", expected " + std::to_string(original_size) + ")";
        return false;
    }

    return true;
}
