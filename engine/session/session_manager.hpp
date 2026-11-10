// ============================================================================
// session_manager.hpp - Upload API & Session Manager (Control Plane #1)
//
// Handles:
//   - Client authentication via shared cluster token (constant-time verify)
//   - Cryptographically random session token issuance (OpenSSL RAND_bytes)
//   - Persistent active client sessions (metadata/sessions.json)
//   - Multi-part streaming upload session bookkeeping
// ============================================================================
#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <vector>

struct ClientSession {
    std::string session_id;
    std::string session_token;
    std::string client_id;
    std::string created_at;
    std::string last_active_at;
};

struct UploadSession {
    std::string upload_id;
    std::string session_token;
    std::string filename;
    std::string tmp_path;
    uint64_t expected_size = 0;
    uint64_t bytes_received = 0;
};

class SessionManager {
public:
    explicit SessionManager(std::string auth_token = "rfe-demo-token",
                            std::string metadata_dir = "metadata",
                            std::string storage_dir = "storage");

    void set_auth_token(const std::string& token);
    const std::string& auth_token() const { return auth_token_; }

    // Authenticate a client using the cluster auth token and issue a new session.
    bool authenticate(const std::string& client_id,
                      const std::string& provided_token,
                      ClientSession& session_out,
                      std::string& error_out);

    // Validate either an issued session_token or (optionally) raw cluster auth_token.
    bool validate_token(const std::string& token,
                        ClientSession* session_out = nullptr,
                        bool allow_cluster_token = false);

    size_t active_session_count() const;

    // Streaming multi-part upload session management
    bool begin_upload(const std::string& session_token,
                      const std::string& filename,
                      uint64_t expected_size,
                      UploadSession& upload_out,
                      std::string& error_out);

    bool append_upload_part(const std::string& upload_id,
                            const std::vector<uint8_t>& bytes,
                            uint64_t& total_received_out,
                            std::string& error_out);

    bool finish_upload(const std::string& upload_id,
                       UploadSession& upload_out,
                       std::string& error_out);

    void abort_upload(const std::string& upload_id);

private:
    std::string auth_token_;
    std::string meta_dir_;
    std::string tmp_upload_dir_;
    std::string sessions_path_;

    mutable std::mutex mu_;
    std::map<std::string, ClientSession> sessions_;  // keyed by session_token
    std::map<std::string, UploadSession> uploads_;   // keyed by upload_id

    void load_sessions();
    void save_sessions() const;
    static std::string random_hex(size_t num_bytes);
    static std::string iso_now();
    static bool const_time_eq(const std::string& a, const std::string& b);
};
