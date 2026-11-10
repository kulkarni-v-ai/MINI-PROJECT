// ============================================================================
// session_manager.cpp - Upload API & Session Manager implementation
// ============================================================================
#include "session_manager.hpp"

#ifdef _WIN32
#include <windows.h>
#include <bcrypt.h>
#endif

#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>

#include "../../third_party/mini_json.hpp"

namespace fs = std::filesystem;

SessionManager::SessionManager(std::string auth_token,
                               std::string metadata_dir,
                               std::string storage_dir)
    : auth_token_(std::move(auth_token)),
      meta_dir_(std::move(metadata_dir)),
      tmp_upload_dir_(storage_dir + "/tmp_uploads"),
      sessions_path_(meta_dir_ + "/sessions.json") {
    std::error_code ec;
    fs::create_directories(meta_dir_, ec);
    fs::create_directories(tmp_upload_dir_, ec);
    load_sessions();
}

void SessionManager::set_auth_token(const std::string& token) {
    std::lock_guard<std::mutex> lk(mu_);
    auth_token_ = token;
}

std::string SessionManager::random_hex(size_t num_bytes) {
    std::vector<unsigned char> buf(num_bytes, 0);
    bool seeded = false;
#ifdef _WIN32
    if (BCryptGenRandom(nullptr, buf.data(), static_cast<ULONG>(num_bytes),
                        BCRYPT_USE_SYSTEM_PREFERRED_RNG) == 0) {
        seeded = true;
    }
#else
    std::ifstream urnd("/dev/urandom", std::ios::binary);
    if (urnd && urnd.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(num_bytes))) {
        seeded = true;
    }
#endif
    if (!seeded) {
        std::random_device rd;
        for (size_t i = 0; i < num_bytes; ++i) {
            buf[i] = static_cast<unsigned char>(rd() & 0xFF);
        }
    }
    std::ostringstream ss;
    ss << std::hex << std::setfill('0');
    for (unsigned char b : buf) {
        ss << std::setw(2) << static_cast<int>(b);
    }
    return ss.str();
}

std::string SessionManager::iso_now() {
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

bool SessionManager::const_time_eq(const std::string& a, const std::string& b) {
    if (a.size() != b.size()) return false;
    volatile unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i) {
        diff = static_cast<unsigned char>(diff | (static_cast<unsigned char>(a[i]) ^
                                                  static_cast<unsigned char>(b[i])));
    }
    return diff == 0;
}

void SessionManager::load_sessions() {
    std::ifstream in(sessions_path_);
    if (!in) return;
    std::stringstream ss;
    ss << in.rdbuf();
    try {
        auto root = json::Json::parse(ss.str());
        if (!root.contains("sessions") || !root["sessions"].is_array()) return;
        for (const auto& item : root["sessions"].items()) {
            ClientSession cs;
            if (item.contains("session_id")) cs.session_id = item["session_id"].as_string();
            if (item.contains("session_token")) cs.session_token = item["session_token"].as_string();
            if (item.contains("client_id")) cs.client_id = item["client_id"].as_string();
            if (item.contains("created_at")) cs.created_at = item["created_at"].as_string();
            if (item.contains("last_active_at")) cs.last_active_at = item["last_active_at"].as_string();
            if (!cs.session_token.empty()) sessions_[cs.session_token] = cs;
        }
    } catch (...) {}
}

void SessionManager::save_sessions() const {
    json::Json arr = json::Json::array();
    for (const auto& kv : sessions_) {
        json::Json j = json::Json::object();
        j["session_id"] = kv.second.session_id;
        j["session_token"] = kv.second.session_token;
        j["client_id"] = kv.second.client_id;
        j["created_at"] = kv.second.created_at;
        j["last_active_at"] = kv.second.last_active_at;
        arr.push_back(j);
    }
    json::Json root = json::Json::object();
    root["sessions"] = arr;

    const std::string tmp = sessions_path_ + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        out << root.dump(2) << "\n";
    }
    std::error_code ec;
    fs::rename(tmp, sessions_path_, ec);
    if (ec) {
        fs::copy_file(tmp, sessions_path_, fs::copy_options::overwrite_existing, ec);
        fs::remove(tmp, ec);
    }
}

bool SessionManager::authenticate(const std::string& client_id,
                                  const std::string& provided_token,
                                  ClientSession& session_out,
                                  std::string& error_out) {
    std::lock_guard<std::mutex> lk(mu_);
    if (!auth_token_.empty() && !const_time_eq(provided_token, auth_token_)) {
        error_out = "authentication failed: invalid cluster auth token";
        return false;
    }

    ClientSession cs;
    cs.session_id = "S-" + random_hex(6);
    cs.session_token = random_hex(24);
    cs.client_id = client_id.empty() ? "anonymous-cli" : client_id;
    cs.created_at = iso_now();
    cs.last_active_at = cs.created_at;

    sessions_[cs.session_token] = cs;
    save_sessions();
    session_out = cs;
    return true;
}

bool SessionManager::validate_token(const std::string& token,
                                    ClientSession* session_out,
                                    bool allow_cluster_token) {
    std::lock_guard<std::mutex> lk(mu_);
    if (auth_token_.empty()) {
        if (session_out) {
            session_out->client_id = "open";
            session_out->session_id = "open";
        }
        return true;
    }
    if (token.empty()) return false;

    auto it = sessions_.find(token);
    if (it != sessions_.end()) {
        it->second.last_active_at = iso_now();
        if (session_out) *session_out = it->second;
        return true;
    }
    if (allow_cluster_token && const_time_eq(token, auth_token_)) {
        if (session_out) {
            session_out->session_id = "direct-token";
            session_out->client_id = "token-client";
        }
        return true;
    }
    return false;
}

size_t SessionManager::active_session_count() const {
    std::lock_guard<std::mutex> lk(mu_);
    return sessions_.size();
}

bool SessionManager::begin_upload(const std::string& session_token,
                                  const std::string& filename,
                                  uint64_t expected_size,
                                  UploadSession& upload_out,
                                  std::string& error_out) {
    std::lock_guard<std::mutex> lk(mu_);
    std::error_code ec;
    fs::create_directories(tmp_upload_dir_, ec);

    UploadSession up;
    up.upload_id = "U-" + random_hex(8);
    up.session_token = session_token;
    up.filename = fs::path(filename).filename().string();
    if (up.filename.empty()) up.filename = "upload.bin";
    up.tmp_path = tmp_upload_dir_ + "/" + up.upload_id + ".part";
    up.expected_size = expected_size;
    up.bytes_received = 0;

    std::ofstream out(up.tmp_path, std::ios::binary | std::ios::trunc);
    if (!out) {
        error_out = "failed to open temporary upload file: " + up.tmp_path;
        return false;
    }
    uploads_[up.upload_id] = up;
    upload_out = up;
    return true;
}

bool SessionManager::append_upload_part(const std::string& upload_id,
                                        const std::vector<uint8_t>& bytes,
                                        uint64_t& total_received_out,
                                        std::string& error_out) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = uploads_.find(upload_id);
    if (it == uploads_.end()) {
        error_out = "upload session not found: " + upload_id;
        return false;
    }
    std::ofstream out(it->second.tmp_path, std::ios::binary | std::ios::app);
    if (!out) {
        error_out = "cannot append to upload temporary file";
        return false;
    }
    if (!bytes.empty()) {
        out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
        if (!out) {
            error_out = "disk write error during upload";
            return false;
        }
    }
    it->second.bytes_received += bytes.size();
    total_received_out = it->second.bytes_received;
    return true;
}

bool SessionManager::finish_upload(const std::string& upload_id,
                                   UploadSession& upload_out,
                                   std::string& error_out) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = uploads_.find(upload_id);
    if (it == uploads_.end()) {
        error_out = "upload session not found: " + upload_id;
        return false;
    }
    upload_out = it->second;
    uploads_.erase(it);
    return true;
}

void SessionManager::abort_upload(const std::string& upload_id) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = uploads_.find(upload_id);
    if (it != uploads_.end()) {
        std::error_code ec;
        fs::remove(it->second.tmp_path, ec);
        uploads_.erase(it);
    }
}
