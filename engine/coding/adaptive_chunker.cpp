// ============================================================================
// adaptive_chunker.cpp - Adaptive Chunking & Systematic RS(3,2) implementation
// ============================================================================
#include "adaptive_chunker.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <sstream>

#include "../chunking/chunk_manager.hpp"

namespace fs = std::filesystem;

AdaptiveChunker::AdaptiveChunker(AdaptivePolicy policy, size_t data_shards, size_t parity_shards)
    : policy_(policy), k_(data_shards), m_(parity_shards), rs_(data_shards, parity_shards) {}

uint64_t AdaptiveChunker::calculate_shard_size(uint64_t file_size,
                                               uint64_t min_peer_avail_bytes) const {
    uint64_t chosen = policy_.size_above_1gb;
    if (file_size <= policy_.threshold_1mb)        chosen = policy_.size_under_1mb;
    else if (file_size <= policy_.threshold_10mb)  chosen = policy_.size_1_to_10mb;
    else if (file_size <= policy_.threshold_100mb) chosen = policy_.size_10_to_100mb;
    else if (file_size <= policy_.threshold_1gb)   chosen = policy_.size_100mb_to_1gb;

    // DACA resource-awareness: adapt shard boundary downward if tightest peer free storage requires smaller stripes
    if (min_peer_avail_bytes > 0) {
        const uint64_t headroom_budget = std::max<uint64_t>(policy_.min_size_bytes, min_peer_avail_bytes / 4ULL);
        if (chosen > headroom_budget) {
            const std::array<uint64_t, 7> tiers = {
                policy_.size_above_1gb,
                policy_.size_100mb_to_1gb,
                policy_.size_10_to_100mb,
                policy_.size_1_to_10mb,
                policy_.size_under_1mb,
                16ULL * 1024ULL,
                policy_.min_size_bytes
            };
            uint64_t adapted = policy_.min_size_bytes;
            for (uint64_t t : tiers) {
                if (t <= chosen && t <= headroom_budget) {
                    adapted = t;
                    break;
                }
            }
            chosen = std::max(policy_.min_size_bytes, adapted);
        }
    }
    return chosen;
}

std::vector<ShardMeta> AdaptiveChunker::encode_file(const std::string& file_id,
                                                   const std::string& file_path,
                                                   const std::string& staging_dir,
                                                   uint64_t& chosen_shard_size_out,
                                                   uint64_t min_peer_avail_bytes) {
    std::ifstream in(file_path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open file for encoding: " + file_path);

    in.seekg(0, std::ios::end);
    const uint64_t file_size = static_cast<uint64_t>(in.tellg());
    in.seekg(0, std::ios::beg);

    fs::create_directories(staging_dir);

    const uint64_t shard_size = calculate_shard_size(file_size, min_peer_avail_bytes);
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

    // MGFRA Step 1: Group shards by stripe and order strictly by numeric sequence number
    // (avoids lexicographical "S1000" < "S200" ordering bug on large multi-stripe files).
    struct StripeGroup {
        std::string stripe_id;
        uint64_t min_seq = std::numeric_limits<uint64_t>::max();
        std::vector<ShardMeta> shards;
    };
    std::map<std::string, size_t> group_index;
    std::vector<StripeGroup> ordered_stripes;
    for (const auto& sm : manifest_shards) {
        if (sm.file_id != file_id) continue;
        auto it = group_index.find(sm.stripe_id);
        size_t idx = 0;
        if (it == group_index.end()) {
            idx = ordered_stripes.size();
            group_index[sm.stripe_id] = idx;
            ordered_stripes.push_back({sm.stripe_id, sm.sequence, {}});
        } else {
            idx = it->second;
            ordered_stripes[idx].min_seq = std::min(ordered_stripes[idx].min_seq, sm.sequence);
        }
        ordered_stripes[idx].shards.push_back(sm);
    }

    if (ordered_stripes.empty()) {
        error_out = "no shards matched file_id " + file_id;
        return false;
    }

    std::sort(ordered_stripes.begin(), ordered_stripes.end(),
              [](const StripeGroup& a, const StripeGroup& b) {
                  if (a.min_seq != b.min_seq) return a.min_seq < b.min_seq;
                  return a.stripe_id < b.stripe_id;
              });

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

    for (auto& sg : ordered_stripes) {
        const std::string& stripe_id = sg.stripe_id;
        std::sort(sg.shards.begin(), sg.shards.end(), [](const ShardMeta& a, const ShardMeta& b) {
            return a.sequence < b.sequence;
        });

        const size_t total_n = k_ + m_;
        std::vector<std::vector<uint8_t>> shard_bufs(total_n);
        std::vector<bool> shard_present(total_n, false);
        std::vector<std::string> expected_hashes(total_n);
        uint64_t shard_len = 0;
        std::vector<uint64_t> orig_lens(k_, 0);
        std::vector<bool> data_slot_expected(k_, false);

        for (const auto& sm : sg.shards) {
            size_t idx = 0;
            if (sm.shard_type == "DATA") {
                size_t d_pos = sm.chunk_id.rfind("-D");
                if (d_pos == std::string::npos) continue;
                try { idx = std::stoul(sm.chunk_id.substr(d_pos + 2)); } catch (...) { continue; }
                if (idx >= k_) continue;
                orig_lens[idx] = sm.original_data_length;
                data_slot_expected[idx] = true;
            } else {
                size_t p_pos = sm.chunk_id.rfind("-P");
                if (p_pos == std::string::npos) continue;
                try { idx = k_ + std::stoul(sm.chunk_id.substr(p_pos + 2)); } catch (...) { continue; }
                if (idx >= total_n) continue;
            }

            shard_len = sm.size;
            expected_hashes[idx] = sm.sha256;
            shard_bufs[idx].assign(shard_len, 0);

            const std::string path = shards_dir + "/" + sm.chunk_id + ".chunk";
            if (fs::exists(path) && fs::file_size(path) == shard_len) {
                std::ifstream sh_in(path, std::ios::binary);
                sh_in.read(reinterpret_cast<char*>(shard_bufs[idx].data()), static_cast<std::streamsize>(shard_len));
                if (sm.sha256.empty() || ChunkManager::sha256_of_bytes(shard_bufs[idx]) == sm.sha256) {
                    shard_present[idx] = true;
                }
            }
        }

        // Validate sequence completeness on metadata slots
        for (size_t d = 0; d < k_; ++d) {
            if (!data_slot_expected[d]) {
                error_out = "missing sequence metadata for data slot D" + std::to_string(d) +
                            " in stripe " + stripe_id;
                return false;
            }
        }

        // Count available verified shards
        size_t available = 0;
        for (bool b : shard_present) if (b) ++available;

        if (available < k_) {
            error_out = "insufficient surviving shards for stripe " + stripe_id +
                        " (have " + std::to_string(available) + ", need " + std::to_string(k_) + ")";
            return false;
        }

        // Decode / reconstruct missing shards using RS(k+m, k)
        std::vector<uint8_t*> shard_ptrs(total_n);
        for (size_t i = 0; i < total_n; ++i) shard_ptrs[i] = shard_bufs[i].data();

        if (!rs_.decode(shard_ptrs, shard_present, shard_len)) {
            error_out = "Reed-Solomon decoding failed on stripe " + stripe_id;
            return false;
        }

        // MGFRA Verification & Ordered Unpadded Sequential Write
        for (size_t d = 0; d < k_; ++d) {
            if (!expected_hashes[d].empty() &&
                ChunkManager::sha256_of_bytes(shard_bufs[d]) != expected_hashes[d]) {
                error_out = "reconstructed data shard SHA-256 verification failed on stripe " +
                            stripe_id + " slot D" + std::to_string(d);
                return false;
            }
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
