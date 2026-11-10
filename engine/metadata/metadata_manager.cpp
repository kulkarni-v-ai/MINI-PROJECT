// ============================================================================
// metadata_manager.cpp - E6: Metadata & Monitoring Engine (implementation)
// ============================================================================
#include "metadata_manager.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <system_error>
#include <thread>

namespace fs = std::filesystem;

MetadataManager::MetadataManager(std::string metadata_dir)
    : dir_(std::move(metadata_dir)) {
    fs::create_directories(dir_);
}

// ---- helpers -----------------------------------------------------------------

json::Json MetadataManager::read_json_file(const std::string& path,
                                           const json::Json& fallback) {
    std::ifstream in(path);
    if (!in) return fallback;
    std::stringstream ss;
    ss << in.rdbuf();
    try {
        return json::Json::parse(ss.str());
    } catch (...) {
        return fallback;
    }
}

void MetadataManager::write_json_file(const std::string& path,
                                      const json::Json& j) {
    // Write-then-rename so readers never see a half-written file.
    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        if (!out) throw std::runtime_error("cannot write metadata: " + tmp);
        out << j.dump(2) << "\n";
    }
    // On Windows a rename can fail transiently while a reader (dashboard,
    // antivirus, indexer) holds the destination open, so retry briefly and
    // finally fall back to copy-over.
    std::error_code ec;
    for (int attempt = 0; attempt < 20; ++attempt) {
        fs::rename(tmp, path, ec);
        if (!ec) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    fs::copy_file(tmp, path, fs::copy_options::overwrite_existing, ec);
    std::error_code ignore;
    fs::remove(tmp, ignore);
    if (ec) throw std::runtime_error("cannot update metadata " + path + ": " + ec.message());
}

static std::string now_iso8601() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return buf;
}

// ---- file metadata -------------------------------------------------------------

void MetadataManager::init_file_metadata(const std::string& file_id,
                                         const std::string& filename,
                                         uint64_t original_size,
                                         uint64_t chunk_size,
                                         const std::vector<ChunkMeta>& chunks) {
    std::lock_guard<std::mutex> lk(mu_);
    json::Json root = json::Json::object();
    root["file_id"] = file_id;
    root["filename"] = filename;
    root["original_size"] = static_cast<long long>(original_size);
    root["chunk_size"] = static_cast<long long>(chunk_size);
    root["total_chunks"] = static_cast<long long>(chunks.size());
    json::Json cmap = json::Json::object();
    for (const auto& c : chunks) cmap.members()[c.chunk_id] = c.to_json();
    root["chunks"] = cmap;
    write_json_file(file_meta_path(), root);
}

void MetadataManager::set_chunk_peers(const std::string& chunk_id,
                                      const std::vector<std::string>& peers) {
    std::lock_guard<std::mutex> lk(mu_);
    json::Json root = read_json_file(file_meta_path(), json::Json::object());
    if (!root.contains("chunks")) return;
    json::Json locs = json::Json::array();
    for (const auto& p : peers) locs.push_back(p);
    root["chunks"][chunk_id]["peers"] = locs;
    write_json_file(file_meta_path(), root);
}

bool MetadataManager::load_file_metadata() {
    std::lock_guard<std::mutex> lk(mu_);
    std::ifstream in(file_meta_path());
    return static_cast<bool>(in);
}

bool MetadataManager::has_file_metadata() const { return fs::exists(file_meta_path()); }

std::string MetadataManager::file_id() const {
    std::lock_guard<std::mutex> lk(mu_);
    return read_json_file(file_meta_path(), json::Json::object())["file_id"].as_string();
}
std::string MetadataManager::filename() const {
    std::lock_guard<std::mutex> lk(mu_);
    return read_json_file(file_meta_path(), json::Json::object())["filename"].as_string();
}
uint64_t MetadataManager::original_size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<uint64_t>(
        read_json_file(file_meta_path(), json::Json::object())["original_size"].as_int());
}
uint64_t MetadataManager::chunk_size() const {
    std::lock_guard<std::mutex> lk(mu_);
    return static_cast<uint64_t>(
        read_json_file(file_meta_path(), json::Json::object())["chunk_size"].as_int());
}

std::vector<ChunkMeta> MetadataManager::chunks() const {
    std::lock_guard<std::mutex> lk(mu_);
    json::Json root = read_json_file(file_meta_path(), json::Json::object());
    std::vector<ChunkMeta> out;
    if (!root.contains("chunks")) return out;
    const std::string fid = root["file_id"].as_string();
    const std::string fname = root["filename"].as_string();
    for (const auto& kv : root["chunks"].members())
        out.push_back(ChunkMeta::from_json(kv.first, fid, fname, kv.second));
    // Order by sequence number (spec §15: sorting by sequence).
    std::sort(out.begin(), out.end(),
              [](const ChunkMeta& a, const ChunkMeta& b) {
                  return a.sequence < b.sequence;
              });
    return out;
}

bool MetadataManager::get_chunk(const std::string& chunk_id, ChunkMeta& out) const {
    for (const auto& c : chunks())
        if (c.chunk_id == chunk_id) { out = c; return true; }
    return false;
}

std::string MetadataManager::next_file_id() {
    std::lock_guard<std::mutex> lk(mu_);
    int n = 0;
    {
        std::ifstream in(counter_path());
        if (in) in >> n;
    }
    ++n;
    {
        std::ofstream out(counter_path(), std::ios::trunc);
        out << n;
    }
    std::ostringstream id;
    id << "F" << std::setw(3) << std::setfill('0') << n;
    return id.str();
}

// ---- monitoring events ---------------------------------------------------------

void MetadataManager::log_event(const std::string& category,
                                const std::string& message,
                                const json::Json& details) {
    std::lock_guard<std::mutex> lk(mu_);
    json::Json root = read_json_file(events_path(), json::Json::object());
    if (!root.contains("events")) root["events"] = json::Json::array();
    json::Json ev = json::Json::object();
    ev["timestamp"] = now_iso8601();
    ev["category"] = category;
    ev["message"] = message;
    if (details.is_object() && !details.members().empty()) ev["details"] = details;
    root["events"].push_back(ev);
    write_json_file(events_path(), root);
}

json::Json MetadataManager::read_events() const {
    std::lock_guard<std::mutex> lk(mu_);
    return read_json_file(events_path(), json::Json::object());
}

// ---- peer status -----------------------------------------------------------------

void MetadataManager::set_peer_status(const std::string& peer_id, PeerState state) {
    std::lock_guard<std::mutex> lk(mu_);
    json::Json root = read_json_file(status_path(), json::Json::object());
    if (!root.contains("peers")) root["peers"] = json::Json::object();
    root["peers"][peer_id] = peer_state_str(state);
    write_json_file(status_path(), root);
}

std::vector<PeerInfo> MetadataManager::read_peer_status(
    const std::vector<PeerInfo>& configured) const {
    std::lock_guard<std::mutex> lk(mu_);
    json::Json root = read_json_file(status_path(), json::Json::object());
    std::vector<PeerInfo> out = configured;
    for (auto& p : out) {
        if (root.contains("peers") && root["peers"].contains(p.id)) {
            const std::string s = root["peers"][p.id].as_string();
            p.state = (s == "ONLINE")  ? PeerState::ONLINE
                    : (s == "OFFLINE") ? PeerState::OFFLINE
                                       : PeerState::UNKNOWN;
        }
    }
    return out;
}

void MetadataManager::reset() {
    std::lock_guard<std::mutex> lk(mu_);
    std::error_code ec;
    fs::remove(file_meta_path(), ec);
    fs::remove(events_path(), ec);
    fs::remove(status_path(), ec);
}
