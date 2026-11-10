// ============================================================================
// catalog_manager.cpp - Central Staging & Multi-File Catalog implementation
// ============================================================================
#include "catalog_manager.hpp"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

#include "../chunking/chunk_manager.hpp"

namespace fs = std::filesystem;

namespace {
std::string current_iso_time() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm_buf{};
#ifdef _WIN32
    gmtime_s(&tm_buf, &t);
#else
    gmtime_r(&t, &tm_buf);
#endif
    std::ostringstream ss;
    ss << std::put_time(&tm_buf, "%Y-%m-%dT%H:%M:%SZ");
    return ss.str();
}

std::string to_lower(std::string s) {
    for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return s;
}
}  // namespace

json::Json FileRecord::to_json() const {
    json::Json j = json::Json::object();
    j["file_id"]       = file_id;
    j["filename"]      = filename;
    j["size"]          = static_cast<long long>(size);
    j["sha256"]        = sha256;
    j["status"]        = file_status_str(status);
    j["central_path"]  = central_path;
    j["upload_time"]   = upload_time;
    j["shard_size"]    = static_cast<long long>(shard_size);
    j["k"]             = static_cast<long long>(k);
    j["m"]             = static_cast<long long>(m);
    j["stripes_count"] = static_cast<long long>(stripes_count);

    json::Json arr = json::Json::array();
    for (const auto& sh : shards) {
        arr.push_back(sh.to_json());
    }
    j["shards"] = arr;
    return j;
}

FileRecord FileRecord::from_json(const json::Json& j) {
    FileRecord r;
    if (j.contains("file_id"))       r.file_id = j["file_id"].as_string();
    if (j.contains("filename"))      r.filename = j["filename"].as_string();
    if (j.contains("size"))          r.size = static_cast<uint64_t>(j["size"].as_int());
    if (j.contains("sha256"))        r.sha256 = j["sha256"].as_string();
    if (j.contains("status"))        r.status = file_status_from_str(j["status"].as_string());
    if (j.contains("central_path"))  r.central_path = j["central_path"].as_string();
    if (j.contains("upload_time"))   r.upload_time = j["upload_time"].as_string();
    if (j.contains("shard_size"))    r.shard_size = static_cast<uint64_t>(j["shard_size"].as_int());
    if (j.contains("k"))             r.k = static_cast<uint32_t>(j["k"].as_int());
    if (j.contains("m"))             r.m = static_cast<uint32_t>(j["m"].as_int());
    if (j.contains("stripes_count")) r.stripes_count = static_cast<uint64_t>(j["stripes_count"].as_int());

    if (j.contains("shards") && j["shards"].is_array()) {
        for (const auto& item : j["shards"].items()) {
            std::string cid = item.contains("chunk_id") ? item["chunk_id"].as_string() : "";
            r.shards.push_back(ShardMeta::from_json(cid, r.file_id, r.filename, item));
        }
    }
    return r;
}

CatalogManager::CatalogManager(std::string base_metadata_dir, std::string base_storage_dir)
    : meta_dir_(std::move(base_metadata_dir)) {
    files_dir_ = meta_dir_ + "/files";
    central_dir_ = base_storage_dir + "/central";
    catalog_path_ = meta_dir_ + "/catalog.json";
    counter_path_ = meta_dir_ + "/file_counter.txt";

    std::error_code ec;
    fs::create_directories(meta_dir_, ec);
    fs::create_directories(files_dir_, ec);
    fs::create_directories(central_dir_, ec);
}

std::string CatalogManager::allocate_file_id() {
    long long count = 1;
    {
        std::ifstream in(counter_path_);
        if (in) in >> count;
    }
    std::ostringstream ss;
    ss << "F" << std::setfill('0') << std::setw(3) << count;

    {
        std::ofstream out(counter_path_, std::ios::trunc);
        out << (count + 1);
    }
    return ss.str();
}

void CatalogManager::save_file_manifest(const FileRecord& record) {
    const std::string manifest_path = files_dir_ + "/" + record.file_id + ".json";
    const std::string tmp_path = manifest_path + ".tmp";
    {
        std::ofstream out(tmp_path, std::ios::trunc);
        out << record.to_json().dump(2);
    }
    std::error_code ec;
    fs::rename(tmp_path, manifest_path, ec);
    if (ec) {
        fs::copy_file(tmp_path, manifest_path, fs::copy_options::overwrite_existing, ec);
        fs::remove(tmp_path, ec);
    }
}

void CatalogManager::persist_catalog_index(const std::vector<FileRecord>& records) {
    json::Json arr = json::Json::array();
    for (const auto& r : records) {
        json::Json item = json::Json::object();
        item["file_id"]      = r.file_id;
        item["filename"]     = r.filename;
        item["size"]         = static_cast<long long>(r.size);
        item["sha256"]       = r.sha256;
        item["status"]       = file_status_str(r.status);
        item["upload_time"]  = r.upload_time;
        item["shard_size"]   = static_cast<long long>(r.shard_size);
        item["stripes"]      = static_cast<long long>(r.stripes_count);
        arr.push_back(item);
    }
    json::Json root = json::Json::object();
    root["catalog"] = arr;

    const std::string tmp = catalog_path_ + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        out << root.dump(2);
    }
    std::error_code ec;
    fs::rename(tmp, catalog_path_, ec);
    if (ec) {
        fs::copy_file(tmp, catalog_path_, fs::copy_options::overwrite_existing, ec);
        fs::remove(tmp, ec);
    }
}

bool CatalogManager::stage_file(const std::string& original_file_path,
                               FileRecord& record_out,
                               std::string& error_out) {
    std::lock_guard<std::mutex> lock(catalog_mu_);

    fs::path orig(original_file_path);
    std::error_code ec;
    if (!fs::is_regular_file(orig, ec)) {
        error_out = "input file does not exist or is not a regular file: " + original_file_path;
        return false;
    }

    const uint64_t size = fs::file_size(orig, ec);
    const std::string sha256 = ChunkManager::sha256_of_file(original_file_path);

    const std::string file_id = allocate_file_id();
    const std::string staged_filename = file_id + "_" + orig.filename().string();
    const std::string staged_path = central_dir_ + "/" + staged_filename;

    fs::copy_file(orig, staged_path, fs::copy_options::overwrite_existing, ec);
    if (ec) {
        error_out = "failed to stage file centrally: " + ec.message();
        return false;
    }

    FileRecord rec;
    rec.file_id = file_id;
    rec.filename = orig.filename().string();
    rec.size = size;
    rec.sha256 = sha256;
    rec.status = FileDistributionStatus::STORED;
    rec.central_path = staged_path;
    rec.upload_time = current_iso_time();
    rec.shard_size = 0;
    rec.k = 2;
    rec.m = 1;
    rec.stripes_count = 0;

    save_file_manifest(rec);

    // Update catalog index
    auto all = list_files();
    all.push_back(rec);
    persist_catalog_index(all);

    record_out = rec;
    return true;
}

bool CatalogManager::stage_bytes(const std::string& filename,
                                const std::vector<uint8_t>& bytes,
                                FileRecord& record_out,
                                std::string& error_out) {
    std::lock_guard<std::mutex> lock(catalog_mu_);
    const std::string sha256 = ChunkManager::sha256_of_bytes(bytes);
    const std::string file_id = allocate_file_id();
    const std::string staged_filename = file_id + "_" + filename;
    const std::string staged_path = central_dir_ + "/" + staged_filename;

    {
        std::ofstream out(staged_path, std::ios::binary | std::ios::trunc);
        if (!out) {
            error_out = "failed to open staging path for write: " + staged_path;
            return false;
        }
        if (!bytes.empty()) {
            out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        }
    }

    FileRecord rec;
    rec.file_id = file_id;
    rec.filename = filename;
    rec.size = bytes.size();
    rec.sha256 = sha256;
    rec.status = FileDistributionStatus::STORED;
    rec.central_path = staged_path;
    rec.upload_time = current_iso_time();
    rec.shard_size = 0;
    rec.k = 2;
    rec.m = 1;
    rec.stripes_count = 0;

    save_file_manifest(rec);

    auto all = list_files();
    all.push_back(rec);
    persist_catalog_index(all);

    record_out = rec;
    return true;
}

bool CatalogManager::stage_temp_file(const std::string& filename,
                                     const std::string& temp_file_path,
                                     FileRecord& record_out,
                                     std::string& error_out) {
    std::lock_guard<std::mutex> lock(catalog_mu_);
    std::error_code ec;
    if (!fs::is_regular_file(temp_file_path, ec)) {
        error_out = "temporary upload file missing: " + temp_file_path;
        return false;
    }

    const uint64_t size = fs::file_size(temp_file_path, ec);
    const std::string sha256 = ChunkManager::sha256_of_file(temp_file_path);
    const std::string clean_name = fs::path(filename).filename().string();
    const std::string file_id = allocate_file_id();
    const std::string staged_filename = file_id + "_" + (clean_name.empty() ? "upload.bin" : clean_name);
    const std::string staged_path = central_dir_ + "/" + staged_filename;

    fs::rename(temp_file_path, staged_path, ec);
    if (ec) {
        fs::copy_file(temp_file_path, staged_path, fs::copy_options::overwrite_existing, ec);
        if (ec) {
            error_out = "failed to move staged file: " + ec.message();
            return false;
        }
        std::error_code ignore_ec;
        fs::remove(temp_file_path, ignore_ec);
    }

    FileRecord rec;
    rec.file_id = file_id;
    rec.filename = clean_name.empty() ? "upload.bin" : clean_name;
    rec.size = size;
    rec.sha256 = sha256;
    rec.status = FileDistributionStatus::STORED;
    rec.central_path = staged_path;
    rec.upload_time = current_iso_time();
    rec.shard_size = 0;
    rec.k = 2;
    rec.m = 1;
    rec.stripes_count = 0;

    save_file_manifest(rec);

    auto all = list_files();
    all.push_back(rec);
    persist_catalog_index(all);

    record_out = rec;
    return true;
}

bool CatalogManager::get_file(const std::string& file_id, FileRecord& out) const {
    std::lock_guard<std::mutex> lock(catalog_mu_);
    const std::string path = files_dir_ + "/" + file_id + ".json";
    std::ifstream in(path);
    if (!in) return false;

    std::stringstream ss;
    ss << in.rdbuf();
    try {
        auto j = json::Json::parse(ss.str());
        out = FileRecord::from_json(j);
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<FileRecord> CatalogManager::list_files() const {
    std::vector<FileRecord> result;
    std::error_code ec;
    for (const auto& entry : fs::directory_iterator(files_dir_, ec)) {
        if (entry.is_regular_file() && entry.path().extension() == ".json") {
            std::ifstream in(entry.path());
            if (in) {
                std::stringstream ss;
                ss << in.rdbuf();
                try {
                    result.push_back(FileRecord::from_json(json::Json::parse(ss.str())));
                } catch (...) {}
            }
        }
    }
    // Sort by file_id ascending
    std::sort(result.begin(), result.end(), [](const FileRecord& a, const FileRecord& b) {
        return a.file_id < b.file_id;
    });
    return result;
}

std::vector<FileRecord> CatalogManager::search_files(const std::string& query) const {
    const std::string q_lower = to_lower(query);
    std::vector<FileRecord> all = list_files();
    std::vector<FileRecord> filtered;
    for (const auto& r : all) {
        if (to_lower(r.filename).find(q_lower) != std::string::npos ||
            to_lower(r.file_id).find(q_lower) != std::string::npos) {
            filtered.push_back(r);
        }
    }
    return filtered;
}

FileDistributionStatus CatalogManager::evaluate_file_status(const FileRecord& rec) {
    if (rec.shards.empty()) {
        return rec.status == FileDistributionStatus::STORED
            ? FileDistributionStatus::STORED
            : rec.status;
    }

    std::map<std::string, std::vector<const ShardMeta*>> stripes;
    for (const auto& sh : rec.shards) {
        stripes[sh.stripe_id].push_back(&sh);
    }

    bool all_healthy = true;
    bool all_recoverable = true;
    const size_t need_k = rec.k > 0 ? rec.k : 2;
    const size_t total_n = need_k + (rec.m > 0 ? rec.m : 1);

    for (const auto& kv : stripes) {
        size_t healthy_count = 0;
        for (const ShardMeta* sh : kv.second) {
            if (sh->status == "HEALTHY" && !sh->peers.empty()) {
                ++healthy_count;
            }
        }
        if (healthy_count < total_n) all_healthy = false;
        if (healthy_count < need_k) all_recoverable = false;
    }

    if (all_healthy) return FileDistributionStatus::AVAILABLE;
    if (all_recoverable) return FileDistributionStatus::DEGRADED;
    return FileDistributionStatus::RECOVERY_PENDING;
}

bool CatalogManager::update_shards_and_status(const std::string& file_id,
                                             FileDistributionStatus status,
                                             const std::vector<ShardMeta>& shards,
                                             uint64_t shard_size,
                                             uint32_t k,
                                             uint32_t m) {
    std::lock_guard<std::mutex> lock(catalog_mu_);
    FileRecord rec;
    const std::string path = files_dir_ + "/" + file_id + ".json";
    std::ifstream in(path);
    if (!in) return false;

    std::stringstream ss;
    ss << in.rdbuf();
    try {
        rec = FileRecord::from_json(json::Json::parse(ss.str()));
    } catch (...) {
        return false;
    }

    rec.status = status;
    rec.shards = shards;
    rec.shard_size = shard_size;
    rec.k = k;
    rec.m = m;

    // Count distinct stripes
    std::map<std::string, int> stripes;
    for (const auto& sh : shards) stripes[sh.stripe_id]++;
    rec.stripes_count = stripes.size();

    save_file_manifest(rec);

    // Refresh catalog index
    auto all = list_files();
    for (auto& item : all) {
        if (item.file_id == file_id) {
            item = rec;
            break;
        }
    }
    persist_catalog_index(all);
    return true;
}

std::vector<std::string> CatalogManager::mark_peer_shards_unavailable(const std::string& peer_id) {
    std::lock_guard<std::mutex> lock(catalog_mu_);
    std::vector<std::string> affected_files;
    auto all = list_files();
    bool any_changed = false;

    for (auto& rec : all) {
        if (rec.shards.empty()) continue;
        bool file_changed = false;
        for (auto& sh : rec.shards) {
            if (std::find(sh.peers.begin(), sh.peers.end(), peer_id) != sh.peers.end()) {
                // If this shard has no other peer or primary peer went offline, mark UNAVAILABLE
                bool has_other_peer = false;
                for (const auto& pid : sh.peers) {
                    if (pid != peer_id) { has_other_peer = true; break; }
                }
                if (!has_other_peer && sh.status != "UNAVAILABLE") {
                    sh.status = "UNAVAILABLE";
                    file_changed = true;
                }
            }
        }
        if (file_changed) {
            rec.status = evaluate_file_status(rec);
            save_file_manifest(rec);
            affected_files.push_back(rec.file_id);
            any_changed = true;
        }
    }

    if (any_changed) {
        persist_catalog_index(all);
    }
    return affected_files;
}

bool CatalogManager::mark_shard_status(const std::string& file_id,
                                       const std::string& shard_id,
                                       const std::string& shard_status) {
    std::lock_guard<std::mutex> lock(catalog_mu_);
    FileRecord rec;
    const std::string path = files_dir_ + "/" + file_id + ".json";
    std::ifstream in(path);
    if (!in) return false;

    std::stringstream ss;
    ss << in.rdbuf();
    try {
        rec = FileRecord::from_json(json::Json::parse(ss.str()));
    } catch (...) {
        return false;
    }

    bool changed = false;
    for (auto& sh : rec.shards) {
        if (sh.chunk_id == shard_id) {
            if (sh.status != shard_status) {
                sh.status = shard_status;
                changed = true;
            }
            break;
        }
    }
    if (!changed) return true;

    rec.status = evaluate_file_status(rec);
    save_file_manifest(rec);

    auto all = list_files();
    for (auto& item : all) {
        if (item.file_id == file_id) {
            item = rec;
            break;
        }
    }
    persist_catalog_index(all);
    return true;
}

std::mutex& CatalogManager::get_file_mutex(const std::string& file_id) {
    std::lock_guard<std::mutex> lock(file_mutexes_mu_);
    auto it = file_mutexes_.find(file_id);
    if (it == file_mutexes_.end()) {
        file_mutexes_[file_id] = std::make_unique<std::mutex>();
    }
    return *file_mutexes_[file_id];
}
