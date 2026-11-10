// ============================================================================
// coordinator_service.cpp - Central Coordination Server implementation
// ============================================================================
#include "coordinator_service.hpp"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>

#include "../coding/adaptive_chunker.hpp"
#include "../communication/protocol.hpp"
#include "../communication/tcp_client.hpp"
#include "../distribution/dynamic_distributor.hpp"
#include "http_server.hpp"

namespace fs = std::filesystem;

CoordinatorService::CoordinatorService(std::string bind_address,
                                       uint16_t port,
                                       int heartbeat_interval_ms,
                                       int heartbeat_timeout_ms,
                                       int failure_threshold,
                                       uint32_t rs_k,
                                       uint32_t rs_m,
                                       uint16_t http_port,
                                       std::string auth_token,
                                       bool auto_recover,
                                       bool require_auth)
    : bind_address_(std::move(bind_address)),
      port_(port),
      heartbeat_interval_ms_(heartbeat_interval_ms),
      heartbeat_timeout_ms_(heartbeat_timeout_ms),
      failure_threshold_(failure_threshold),
      rs_k_(rs_k > 0 ? rs_k : 2),
      rs_m_(rs_m > 0 ? rs_m : 1),
      http_port_(http_port),
      auto_recover_(auto_recover),
      require_auth_(require_auth),
      sessions_(std::move(auth_token), "metadata", "storage") {}

CoordinatorService::~CoordinatorService() {
    stop();
}

bool CoordinatorService::start() {
    if (running_.load()) return true;

    TCPServer::Handler handler = [this](const json::Json& req,
                                        const std::vector<uint8_t>& req_payload,
                                        json::Json& resp,
                                        std::vector<uint8_t>& resp_payload) -> bool {
        return handle_request(req, req_payload, resp, resp_payload);
    };

    server_ = std::make_unique<TCPServer>(bind_address_, port_, handler);
    if (!server_->start()) {
        std::cerr << "[Coordinator] Failed to bind to " << bind_address_ << ":" << port_ << "\n";
        return false;
    }

    running_.store(true);
    server_thread_ = std::thread([this]() { server_->run(); });
    monitor_thread_ = std::thread(&CoordinatorService::health_monitor_loop, this);
    if (auto_recover_) {
        recovery_thread_ = std::thread(&CoordinatorService::recovery_worker_loop, this);
    }

    if (http_port_ > 0) {
        http_server_ = std::make_unique<HttpServer>(*this, bind_address_, http_port_);
        if (http_server_->start()) {
            std::cout << "[Coordinator] HTTP Upload & Console API ACTIVE on http://"
                      << (bind_address_ == "0.0.0.0" ? "127.0.0.1" : bind_address_)
                      << ":" << http_port_ << "\n";
        } else {
            std::cerr << "[Coordinator] Warning: HTTP server failed to bind on port " << http_port_ << "\n";
        }
    }

    std::cout << "[Coordinator] Registration listener ACTIVE on " << bind_address_ << ":" << port_
              << " (RS(" << (rs_k_ + rs_m_) << "," << rs_k_ << ")"
              << ", heartbeat=" << heartbeat_interval_ms_ << "ms"
              << ", timeout=" << heartbeat_timeout_ms_ << "ms"
              << ", auto_recover=" << (auto_recover_ ? "ON" : "OFF") << ")\n";
    return true;
}

void CoordinatorService::stop() {
    if (!running_.exchange(false)) return;

    recovery_cv_.notify_all();
    if (http_server_) {
        http_server_->stop();
    }
    if (server_) {
        server_->stop();
    }
    if (server_thread_.joinable()) server_thread_.join();
    if (monitor_thread_.joinable()) monitor_thread_.join();
    if (recovery_thread_.joinable()) recovery_thread_.join();
    std::cout << "[Coordinator] Registration listener stopped\n";
}

void CoordinatorService::run() {
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}

bool CoordinatorService::check_client_auth(const json::Json& req, json::Json& resp) {
    if (!require_auth_ || sessions_.auth_token().empty()) return true;

    std::string token;
    if (req.contains("session_token") && req["session_token"].is_string()) {
        token = req["session_token"].as_string();
    } else if (req.contains("auth_token") && req["auth_token"].is_string()) {
        token = req["auth_token"].as_string();
    }

    if (sessions_.validate_token(token, nullptr, true)) return true;

    resp["type"] = proto::AUTH_ERROR;
    resp["reason"] = "authentication required: invalid or missing session_token";
    return false;
}

bool CoordinatorService::verify_peer_shard(const PeerInfo& peer, const ShardMeta& shard) {
    if (peer.state != PeerHealth::ONLINE) return false;
    TCPClient c(1500, 2000);
    if (!c.connect_to(peer.address, peer.port)) return false;

    json::Json req = json::Json::object();
    req["type"] = proto::VERIFY_CHUNK;
    req["chunk_id"] = shard.chunk_id;
    req["expected_size"] = static_cast<long long>(shard.size);
    req["expected_sha256"] = shard.sha256;

    json::Json resp;
    std::vector<uint8_t> pl;
    if (c.request(req, {}, resp, pl) != proto::RecvStatus::OK) return false;
    return resp.contains("type") &&
           resp["type"].as_string() == proto::VERIFY_CHUNK_ACK &&
           resp.contains("status") &&
           resp["status"].as_string() == "OK";
}

void CoordinatorService::reconcile_returning_peer(const std::string& peer_id) {
    PeerInfo pinfo;
    if (!registry_.get_peer(peer_id, pinfo) || pinfo.state != PeerHealth::ONLINE) return;

    auto files = catalog_.list_files();
    for (const auto& f : files) {
        if (f.shards.empty()) continue;
        bool touches_peer = false;
        for (const auto& sh : f.shards) {
            if (sh.status != "HEALTHY" &&
                std::find(sh.peers.begin(), sh.peers.end(), peer_id) != sh.peers.end()) {
                touches_peer = true;
                break;
            }
        }
        if (!touches_peer) continue;

        std::lock_guard<std::mutex> lk(catalog_.get_file_mutex(f.file_id));
        FileRecord rec;
        if (!catalog_.get_file(f.file_id, rec)) continue;
        bool changed = false;
        for (auto& sh : rec.shards) {
            if (sh.status != "HEALTHY" &&
                std::find(sh.peers.begin(), sh.peers.end(), peer_id) != sh.peers.end()) {
                if (verify_peer_shard(pinfo, sh)) {
                    sh.status = "HEALTHY";
                    changed = true;
                }
            }
        }
        if (changed) {
            FileDistributionStatus st = CatalogManager::evaluate_file_status(rec);
            catalog_.update_shards_and_status(rec.file_id, st, rec.shards, rec.shard_size, rec.k, rec.m);
            json::Json d = json::Json::object();
            d["file_id"] = rec.file_id;
            d["peer_id"] = peer_id;
            d["status"] = file_status_str(st);
            events_.log_event("PEER_SHARDS_RECONCILED",
                              "returning peer verified existing shards on disk", d);
        }
    }
}

bool CoordinatorService::process_uploaded_file(const std::string& file_id, FileRecord& rec_out) {
    std::lock_guard<std::mutex> lk(catalog_.get_file_mutex(file_id));
    if (!catalog_.get_file(file_id, rec_out)) return false;

    if (rec_out.status != FileDistributionStatus::STORED) {
        return true;
    }

    registry_.evaluate_health(heartbeat_timeout_ms_, failure_threshold_);
    auto online = registry_.get_online_peers();
    if (online.size() < rs_k_) {
        std::cout << "[Coordinator] Staged " << file_id << " centrally (STORED); "
                  << online.size() << "/" << rs_k_ << " min peers online for RS distribution.\n";
        return true;
    }

    std::cout << "[Coordinator] Encoding & distributing " << file_id
              << " with RS(" << (rs_k_ + rs_m_) << "," << rs_k_ << ")...\n";

    AdaptiveChunker chunker(AdaptivePolicy(), rs_k_, rs_m_);
    uint64_t chosen_shard_size = 0;
    const std::string staging_dir = "output/staging/" + file_id;
    rec_out.shards = chunker.encode_file(file_id, rec_out.central_path, staging_dir, chosen_shard_size);

    DynamicDistributor distributor(registry_);
    std::string dist_log;
    FileDistributionStatus new_status =
        distributor.distribute_file(file_id, rec_out.shards, staging_dir, dist_log, rs_k_);
    std::cout << dist_log;

    catalog_.update_shards_and_status(file_id, new_status, rec_out.shards, chosen_shard_size, rs_k_, rs_m_);
    catalog_.get_file(file_id, rec_out);

    std::error_code ec;
    fs::remove_all(staging_dir, ec);

    json::Json d = json::Json::object();
    d["file_id"] = file_id;
    d["filename"] = rec_out.filename;
    d["status"] = file_status_str(rec_out.status);
    d["shard_size"] = static_cast<long long>(chosen_shard_size);
    d["stripes"] = static_cast<long long>(rec_out.stripes_count);
    events_.log_event("FILE_DISTRIBUTED", "file chunked, RS-encoded, and distributed to peers", d);

    if (rec_out.status == FileDistributionStatus::DEGRADED ||
        rec_out.status == FileDistributionStatus::RECOVERY_PENDING) {
        schedule_recovery(file_id, false);
    }
    return true;
}

void CoordinatorService::schedule_recovery(const std::string& file_id, bool immediate) {
    {
        std::lock_guard<std::mutex> lk(pending_mu_);
        auto it = pending_repairs_.find(file_id);
        if (it == pending_repairs_.end()) {
            PendingRepair pr;
            pr.attempt = 0;
            pr.next_try = immediate
                ? std::chrono::steady_clock::now()
                : std::chrono::steady_clock::now() + std::chrono::seconds(2);
            pending_repairs_[file_id] = pr;
        } else if (immediate) {
            it->second.next_try = std::chrono::steady_clock::now();
        }
    }
    recovery_cv_.notify_all();
}

bool CoordinatorService::handle_request(const json::Json& req,
                                        const std::vector<uint8_t>& req_payload,
                                        json::Json& resp,
                                        std::vector<uint8_t>& resp_payload) {
    const std::string type = req["type"].as_string();

    // ---- Peer Registration & Heartbeats (Control Plane) --------------------
    if (type == proto::PEER_REGISTER) {
        PeerInfo info;
        info.id = req["peer_id"].as_string();
        info.address = req["address"].as_string();
        info.port = static_cast<uint16_t>(req["port"].as_int());

        PeerWorkload wl;
        if (req.contains("available_storage_bytes"))
            wl.available_storage_bytes = static_cast<uint64_t>(req["available_storage_bytes"].as_int());
        if (req.contains("active_transfers"))
            wl.active_transfers = static_cast<uint32_t>(req["active_transfers"].as_int());
        if (req.contains("transfer_capacity"))
            wl.transfer_capacity = static_cast<uint32_t>(req["transfer_capacity"].as_int());
        info.workload = wl;

        std::string msg;
        PeerHealth prev = PeerHealth::UNKNOWN;
        registry_.register_peer(info, msg, &prev);
        std::cout << "[Coordinator] " << msg << std::endl;

        json::Json d = json::Json::object();
        d["peer_id"] = info.id;
        d["endpoint"] = info.address + ":" + std::to_string(info.port);
        events_.log_event((prev == PeerHealth::OFFLINE || prev == PeerHealth::SUSPECT)
                              ? "PEER_RECOVERED" : "PEER_REGISTERED",
                          msg, d);

        if (prev == PeerHealth::OFFLINE || prev == PeerHealth::SUSPECT) {
            reconcile_returning_peer(info.id);
        }

        // Wake any RECOVERY_PENDING repairs now that a peer joined/returned
        {
            std::lock_guard<std::mutex> lk(pending_mu_);
            auto now = std::chrono::steady_clock::now();
            for (auto& kv : pending_repairs_) kv.second.next_try = now;
        }
        recovery_cv_.notify_all();

        resp["type"] = proto::PEER_REGISTER_ACK;
        resp["status"] = "OK";
        resp["peer_id"] = info.id;
        resp["heartbeat_interval_ms"] = static_cast<long long>(heartbeat_interval_ms_);
        return true;
    }

    if (type == proto::PEER_HEARTBEAT) {
        const std::string peer_id = req["peer_id"].as_string();
        PeerWorkload wl;
        if (req.contains("available_storage_bytes"))
            wl.available_storage_bytes = static_cast<uint64_t>(req["available_storage_bytes"].as_int());
        if (req.contains("active_transfers"))
            wl.active_transfers = static_cast<uint32_t>(req["active_transfers"].as_int());
        if (req.contains("transfer_capacity"))
            wl.transfer_capacity = static_cast<uint32_t>(req["transfer_capacity"].as_int());

        PeerHealth prev = PeerHealth::UNKNOWN;
        bool ok = registry_.record_heartbeat(peer_id, wl, &prev);
        if (ok && (prev == PeerHealth::OFFLINE || prev == PeerHealth::SUSPECT)) {
            std::cout << "[Coordinator] Peer " << peer_id << " recovered back to ONLINE\n";
            json::Json d = json::Json::object();
            d["peer_id"] = peer_id;
            events_.log_event("PEER_RECOVERED", "peer heartbeat resumed -> ONLINE", d);
            reconcile_returning_peer(peer_id);
            {
                std::lock_guard<std::mutex> lk(pending_mu_);
                auto now = std::chrono::steady_clock::now();
                for (auto& kv : pending_repairs_) kv.second.next_try = now;
            }
            recovery_cv_.notify_all();
        }
        resp["type"] = proto::PEER_HEARTBEAT_ACK;
        resp["status"] = ok ? "OK" : "NOT_REGISTERED";
        resp["peer_id"] = peer_id;
        return true;
    }

    // ---- Client Session Authentication (#1 Upload API & Session Manager) ---
    if (type == proto::SESSION_AUTH) {
        const std::string client_id = req.contains("client_id") ? req["client_id"].as_string() : "cli-client";
        const std::string provided_tok = req.contains("auth_token") ? req["auth_token"].as_string() : "";
        ClientSession cs;
        std::string err;
        if (!sessions_.authenticate(client_id, provided_tok, cs, err)) {
            json::Json d = json::Json::object();
            d["client_id"] = client_id;
            events_.log_event("AUTH_FAILURE", err, d);
            resp["type"] = proto::AUTH_ERROR;
            resp["reason"] = err;
            return true;
        }
        json::Json d = json::Json::object();
        d["client_id"] = cs.client_id;
        d["session_id"] = cs.session_id;
        events_.log_event("AUTH_SUCCESS", "client authenticated and session issued", d);

        resp["type"] = proto::SESSION_AUTH_ACK;
        resp["status"] = "OK";
        resp["session_id"] = cs.session_id;
        resp["session_token"] = cs.session_token;
        resp["http_port"] = static_cast<long long>(http_port_);
        return true;
    }

    if (type == proto::QUERY_PEERS) {
        if (!check_client_auth(req, resp)) return true;
        registry_.evaluate_health(heartbeat_timeout_ms_, failure_threshold_);
        resp["type"] = proto::QUERY_PEERS_ACK;
        resp["peers"] = registry_.to_json();
        resp["rs_k"] = static_cast<long long>(rs_k_);
        resp["rs_m"] = static_cast<long long>(rs_m_);
        resp["active_sessions"] = static_cast<long long>(sessions_.active_session_count());
        return true;
    }

    if (type == proto::QUERY_EVENTS) {
        if (!check_client_auth(req, resp)) return true;
        resp["type"] = proto::QUERY_EVENTS_ACK;
        resp["events"] = events_.read_events()["events"];
        return true;
    }

    // ---- Single-frame & Multi-part Streaming Upload ------------------------
    if (type == "UPLOAD_FILE") {
        if (!check_client_auth(req, resp)) return true;
        const std::string filename = req.contains("filename") ? req["filename"].as_string() : "upload.bin";
        const bool defer = req.contains("defer_distribution") && req["defer_distribution"].is_bool() &&
                           req["defer_distribution"].as_bool();

        FileRecord rec;
        std::string err;
        if (!catalog_.stage_bytes(filename, req_payload, rec, err)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "failed to stage file centrally: " + err;
            return true;
        }

        {
            json::Json d = json::Json::object();
            d["file_id"] = rec.file_id;
            d["filename"] = rec.filename;
            d["size"] = static_cast<long long>(rec.size);
            events_.log_event("FILE_UPLOADED", "file uploaded and staged centrally", d);
        }

        if (!defer) {
            process_uploaded_file(rec.file_id, rec);
        }

        resp["type"] = "UPLOAD_FILE_ACK";
        resp["status"] = "OK";
        resp["file_id"] = rec.file_id;
        resp["filename"] = rec.filename;
        resp["size"] = static_cast<long long>(rec.size);
        resp["sha256"] = rec.sha256;
        resp["status_str"] = file_status_str(rec.status);
        resp["central_path"] = rec.central_path;
        resp["upload_time"] = rec.upload_time;
        resp["stripes_count"] = static_cast<long long>(rec.stripes_count);
        resp["shard_size"] = static_cast<long long>(rec.shard_size);
        resp["k"] = static_cast<long long>(rec.k);
        resp["m"] = static_cast<long long>(rec.m);
        return true;
    }

    if (type == proto::UPLOAD_INIT) {
        if (!check_client_auth(req, resp)) return true;
        const std::string filename = req.contains("filename") ? req["filename"].as_string() : "upload.bin";
        const uint64_t expected_size = req.contains("size") ? static_cast<uint64_t>(req["size"].as_int()) : 0;
        const std::string tok = req.contains("session_token") ? req["session_token"].as_string() : "";

        UploadSession up;
        std::string err;
        if (!sessions_.begin_upload(tok, filename, expected_size, up, err)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = err;
            return true;
        }
        resp["type"] = proto::UPLOAD_INIT_ACK;
        resp["status"] = "OK";
        resp["upload_id"] = up.upload_id;
        return true;
    }

    if (type == proto::UPLOAD_PART) {
        if (!check_client_auth(req, resp)) return true;
        const std::string upload_id = req["upload_id"].as_string();
        uint64_t total_rx = 0;
        std::string err;
        if (!sessions_.append_upload_part(upload_id, req_payload, total_rx, err)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = err;
            return true;
        }
        resp["type"] = proto::UPLOAD_PART_ACK;
        resp["status"] = "OK";
        resp["bytes_received"] = static_cast<long long>(total_rx);
        return true;
    }

    if (type == proto::UPLOAD_COMPLETE) {
        if (!check_client_auth(req, resp)) return true;
        const std::string upload_id = req["upload_id"].as_string();
        const bool defer = req.contains("defer_distribution") && req["defer_distribution"].is_bool() &&
                           req["defer_distribution"].as_bool();
        UploadSession up;
        std::string err;
        if (!sessions_.finish_upload(upload_id, up, err)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = err;
            return true;
        }

        FileRecord rec;
        if (!catalog_.stage_temp_file(up.filename, up.tmp_path, rec, err)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = err;
            return true;
        }

        {
            json::Json d = json::Json::object();
            d["file_id"] = rec.file_id;
            d["filename"] = rec.filename;
            d["size"] = static_cast<long long>(rec.size);
            events_.log_event("FILE_UPLOADED", "file uploaded (streamed) and staged centrally", d);
        }

        if (!defer) {
            process_uploaded_file(rec.file_id, rec);
        }

        resp["type"] = proto::UPLOAD_COMPLETE_ACK;
        resp["status"] = "OK";
        resp["file_id"] = rec.file_id;
        resp["filename"] = rec.filename;
        resp["size"] = static_cast<long long>(rec.size);
        resp["sha256"] = rec.sha256;
        resp["status_str"] = file_status_str(rec.status);
        resp["central_path"] = rec.central_path;
        resp["upload_time"] = rec.upload_time;
        resp["stripes_count"] = static_cast<long long>(rec.stripes_count);
        resp["shard_size"] = static_cast<long long>(rec.shard_size);
        resp["k"] = static_cast<long long>(rec.k);
        resp["m"] = static_cast<long long>(rec.m);
        return true;
    }

    // ---- Download Manifest & Central Fallback ------------------------------
    if (type == "DOWNLOAD_MANIFEST") {
        if (!check_client_auth(req, resp)) return true;
        const std::string file_id = req["file_id"].as_string();

        FileRecord rec;
        if (!catalog_.get_file(file_id, rec)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "file not found: " + file_id;
            return true;
        }

        if (rec.status == FileDistributionStatus::STORED) {
            process_uploaded_file(file_id, rec);
        }

        registry_.evaluate_health(heartbeat_timeout_ms_, failure_threshold_);
        resp["type"] = "DOWNLOAD_MANIFEST_ACK";
        resp["status"] = "OK";
        resp["manifest"] = rec.to_json();
        resp["peers"] = registry_.to_json();
        return true;
    }

    if (type == proto::DOWNLOAD_CENTRAL) {
        if (!check_client_auth(req, resp)) return true;
        const std::string file_id = req["file_id"].as_string();
        FileRecord rec;
        if (!catalog_.get_file(file_id, rec) || !fs::exists(rec.central_path)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "central staged file not found for " + file_id;
            return true;
        }
        uint64_t offset = req.contains("offset") ? static_cast<uint64_t>(req["offset"].as_int()) : 0;
        uint64_t length = req.contains("length") ? static_cast<uint64_t>(req["length"].as_int()) : (4ULL * 1024ULL * 1024ULL);
        if (length > 8ULL * 1024ULL * 1024ULL) length = 8ULL * 1024ULL * 1024ULL;

        std::ifstream in(rec.central_path, std::ios::binary);
        if (!in) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "cannot open central staged file";
            return true;
        }
        uint64_t total_sz = rec.size;
        if (offset > total_sz) offset = total_sz;
        uint64_t to_read = std::min(length, total_sz - offset);
        resp_payload.resize(static_cast<size_t>(to_read));
        if (to_read > 0) {
            in.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
            in.read(reinterpret_cast<char*>(resp_payload.data()), static_cast<std::streamsize>(to_read));
            std::streamsize got = in.gcount();
            resp_payload.resize(static_cast<size_t>(got));
        }

        resp["type"] = proto::DOWNLOAD_CENTRAL_ACK;
        resp["status"] = "OK";
        resp["total_size"] = static_cast<long long>(total_sz);
        resp["offset"] = static_cast<long long>(offset);
        resp["eof"] = (offset + resp_payload.size() >= total_sz);
        return true;
    }

    // ---- Catalog Queries ---------------------------------------------------
    if (type == "CATALOG_LIST") {
        if (!check_client_auth(req, resp)) return true;
        auto files = catalog_.list_files();
        json::Json arr = json::Json::array();
        for (const auto& f : files) arr.push_back(f.to_json());
        resp["type"] = "CATALOG_LIST_ACK";
        resp["files"] = arr;
        return true;
    }

    if (type == "CATALOG_SEARCH") {
        if (!check_client_auth(req, resp)) return true;
        const std::string q = req.contains("query") ? req["query"].as_string() : "";
        auto files = catalog_.search_files(q);
        json::Json arr = json::Json::array();
        for (const auto& f : files) arr.push_back(f.to_json());
        resp["type"] = "CATALOG_SEARCH_ACK";
        resp["files"] = arr;
        return true;
    }

    if (type == "CATALOG_INFO") {
        if (!check_client_auth(req, resp)) return true;
        const std::string file_id = req["file_id"].as_string();
        FileRecord rec;
        if (!catalog_.get_file(file_id, rec)) {
            resp["type"] = proto::TRANSFER_ERROR;
            resp["reason"] = "file not found: " + file_id;
            return true;
        }
        resp["type"] = "CATALOG_INFO_ACK";
        resp["manifest"] = rec.to_json();
        return true;
    }

    // ---- Step 1 & 2 of Recovery Flowchart: Client-reported Shard Issue -----
    if (type == proto::REPORT_SHARD_ISSUE) {
        if (!check_client_auth(req, resp)) return true;
        const std::string file_id = req["file_id"].as_string();
        const std::string shard_id = req["shard_id"].as_string();
        const std::string issue = req.contains("issue") ? req["issue"].as_string() : "CORRUPT";
        const std::string peer_id = req.contains("peer_id") ? req["peer_id"].as_string() : "";

        const std::string new_shard_status = (issue == "CORRUPT") ? "CORRUPT" : "UNAVAILABLE";
        catalog_.mark_shard_status(file_id, shard_id, new_shard_status);

        json::Json d = json::Json::object();
        d["file_id"] = file_id;
        d["shard_id"] = shard_id;
        d["peer_id"] = peer_id;
        d["issue"] = new_shard_status;
        events_.log_event(issue == "CORRUPT" ? "SHARD_CORRUPT" : "SHARD_UNAVAILABLE",
                          "client read detected " + new_shard_status + " shard", d);

        schedule_recovery(file_id, true);

        resp["type"] = proto::REPORT_SHARD_ISSUE_ACK;
        resp["status"] = "OK";
        return true;
    }

    if (type == "REPAIR_FILE") {
        if (!check_client_auth(req, resp)) return true;
        const std::string file_id = req["file_id"].as_string();
        std::string report;
        bool ok = repair_file(file_id, report);
        resp["type"] = "REPAIR_FILE_ACK";
        resp["status"] = ok ? "OK" : "PENDING_OR_FAILED";
        resp["report"] = report;
        return true;
    }

    resp["type"] = proto::TRANSFER_ERROR;
    resp["reason"] = "unsupported coordinator request type: " + type;
    return true;
}

void CoordinatorService::health_monitor_loop() {
    const int step_ms = std::max(100, heartbeat_interval_ms_ / 2);
    while (running_.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(step_ms));
        if (!running_.load()) break;

        auto transitions = registry_.evaluate_health_detailed(heartbeat_timeout_ms_, failure_threshold_);
        for (const auto& t : transitions) {
            std::cout << "[Coordinator] HEALTH CHANGE: " << t.message << std::endl;

            json::Json d = json::Json::object();
            d["peer_id"] = t.peer_id;
            d["state"] = peer_health_str(t.new_state);
            events_.log_event(t.new_state == PeerHealth::OFFLINE ? "PEER_OFFLINE" : "PEER_SUSPECT",
                              t.message, d);

            // Step 1 & 2 of Recovery Flowchart: Peer Timeout -> Mark Shards Unavailable
            if (t.new_state == PeerHealth::OFFLINE) {
                auto affected = catalog_.mark_peer_shards_unavailable(t.peer_id);
                for (const auto& fid : affected) {
                    json::Json sd = json::Json::object();
                    sd["file_id"] = fid;
                    sd["peer_id"] = t.peer_id;
                    events_.log_event("SHARD_UNAVAILABLE",
                                      "marked shards on offline peer as UNAVAILABLE", sd);
                    schedule_recovery(fid, true);
                }
            }
        }
    }
}

void CoordinatorService::recovery_worker_loop() {
    while (running_.load()) {
        std::vector<std::string> due_files;
        auto now = std::chrono::steady_clock::now();
        {
            std::unique_lock<std::mutex> lk(pending_mu_);
            if (pending_repairs_.empty()) {
                recovery_cv_.wait_for(lk, std::chrono::milliseconds(500), [this]() {
                    return !running_.load() || !pending_repairs_.empty();
                });
            } else {
                auto earliest = now + std::chrono::seconds(15);
                for (const auto& kv : pending_repairs_) {
                    if (kv.second.next_try < earliest) earliest = kv.second.next_try;
                }
                if (earliest > now) {
                    recovery_cv_.wait_until(lk, earliest, [this]() {
                        if (!running_.load()) return true;
                        auto cur = std::chrono::steady_clock::now();
                        for (const auto& kv : pending_repairs_) {
                            if (kv.second.next_try <= cur) return true;
                        }
                        return false;
                    });
                }
            }
            if (!running_.load()) break;

            now = std::chrono::steady_clock::now();
            for (const auto& kv : pending_repairs_) {
                if (kv.second.next_try <= now) {
                    due_files.push_back(kv.first);
                }
            }
        }

        for (const auto& fid : due_files) {
            if (!running_.load()) break;
            std::string report;
            repair_file(fid, report);

            FileRecord rec;
            bool done = catalog_.get_file(fid, rec) &&
                        rec.status == FileDistributionStatus::AVAILABLE;
            std::lock_guard<std::mutex> lk(pending_mu_);
            auto it = pending_repairs_.find(fid);
            if (it != pending_repairs_.end()) {
                if (done) {
                    pending_repairs_.erase(it);
                } else {
                    it->second.attempt += 1;
                    int backoff_sec = std::min(15, 2 << std::min(it->second.attempt - 1, 3));
                    it->second.next_try = std::chrono::steady_clock::now() + std::chrono::seconds(backoff_sec);
                }
            }
        }
    }
}

bool CoordinatorService::repair_file(const std::string& file_id, std::string& report_out) {
    std::lock_guard<std::mutex> lk(catalog_.get_file_mutex(file_id));
    FileRecord rec;
    if (!catalog_.get_file(file_id, rec)) {
        report_out = "file ID not found: " + file_id;
        return false;
    }

    registry_.evaluate_health(heartbeat_timeout_ms_, failure_threshold_);
    auto online_peers = registry_.get_online_peers();
    std::map<std::string, PeerInfo> peer_map;
    for (const auto& p : registry_.get_all_peers()) peer_map[p.id] = p;

    const size_t need_k = rec.k > 0 ? rec.k : rs_k_;
    const size_t total_m = rec.m > 0 ? rec.m : rs_m_;

    // Group shards by stripe
    std::map<std::string, std::vector<size_t>> stripes;
    for (size_t i = 0; i < rec.shards.size(); ++i) {
        stripes[rec.shards[i].stripe_id].push_back(i);
    }

    std::ostringstream ss;
    bool any_repaired = false;
    bool all_shards_healthy = true;
    bool insufficient_surviving = false;

    for (const auto& kv : stripes) {
        const auto& shard_indices = kv.second;

        std::vector<size_t> missing_indices;
        std::vector<size_t> surviving_indices;
        std::vector<std::string> healthy_used_peers;

        for (size_t idx : shard_indices) {
            ShardMeta& sh = rec.shards[idx];
            bool shard_ok = false;

            if (sh.status != "CORRUPT") {
                for (const auto& pid : sh.peers) {
                    auto it = peer_map.find(pid);
                    if (it != peer_map.end() && it->second.state == PeerHealth::ONLINE) {
                        if (sh.status == "HEALTHY") {
                            shard_ok = true;
                            healthy_used_peers.push_back(pid);
                            break;
                        } else if (verify_peer_shard(it->second, sh)) {
                            // Returning peer still has valid shard on disk
                            sh.status = "HEALTHY";
                            sh.peers = {pid};
                            shard_ok = true;
                            any_repaired = true;
                            healthy_used_peers.push_back(pid);
                            ss << "[In-Place Reconciled] " << sh.chunk_id
                               << " verified SHA-256 on returning Peer " << pid << "\n";
                            break;
                        }
                    }
                }
            }

            if (shard_ok) {
                surviving_indices.push_back(idx);
            } else {
                if (sh.status == "HEALTHY") sh.status = "UNAVAILABLE";
                missing_indices.push_back(idx);
            }
        }

        if (missing_indices.empty()) continue;

        for (size_t m_idx : missing_indices) {
            ShardMeta& missing_shard = rec.shards[m_idx];

            // Step 3 & 4: Check if an alternate online replica peer holds this shard
            bool restored_via_replica = false;
            std::vector<PeerInfo> healthy_replicas;
            for (const auto& pid : missing_shard.peers) {
                auto it = peer_map.find(pid);
                if (it != peer_map.end() && it->second.state == PeerHealth::ONLINE &&
                    verify_peer_shard(it->second, missing_shard)) {
                    healthy_replicas.push_back(it->second);
                }
            }

            // Pick a target peer that doesn't already hold a shard of this stripe
            auto pick_target_peer = [&]() -> std::pair<bool, PeerInfo> {
                for (const auto& candidate : online_peers) {
                    if (candidate.workload.is_busy()) continue;
                    if (std::find(healthy_used_peers.begin(), healthy_used_peers.end(), candidate.id) ==
                        healthy_used_peers.end()) {
                        return {true, candidate};
                    }
                }
                for (const auto& candidate : online_peers) {
                    if (std::find(healthy_used_peers.begin(), healthy_used_peers.end(), candidate.id) ==
                        healthy_used_peers.end()) {
                        return {true, candidate};
                    }
                }
                return {false, PeerInfo{}};
            };

            if (!healthy_replicas.empty()) {
                auto [found_repl, repl_peer] = pick_target_peer();
                if (found_repl) {
                    const PeerInfo& alt = healthy_replicas.front();
                    json::Json req = json::Json::object();
                    req["type"] = proto::COPY_SHARD;
                    req["target_shard_id"] = missing_shard.chunk_id;
                    req["target_shard_size"] = static_cast<long long>(missing_shard.size);
                    req["expected_sha256"] = missing_shard.sha256;
                    req["source_address"] = alt.address;
                    req["source_port"] = static_cast<long long>(alt.port);
                    req["source_shard_id"] = missing_shard.chunk_id;

                    TCPClient client(5000, 10000);
                    if (client.connect_to(repl_peer.address, repl_peer.port)) {
                        json::Json resp;
                        std::vector<uint8_t> pl;
                        if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                            resp["type"].as_string() == proto::COPY_SHARD_ACK &&
                            resp["status"].as_string() == "OK") {
                            missing_shard.peers = {repl_peer.id};
                            missing_shard.status = "HEALTHY";
                            healthy_used_peers.push_back(repl_peer.id);
                            surviving_indices.push_back(m_idx);
                            any_repaired = true;
                            restored_via_replica = true;
                            ss << "[Replica Copy Repair] " << missing_shard.chunk_id
                               << " fetched from alternate Peer " << alt.id
                               << " -> Peer " << repl_peer.id << " (SHA-256 verified)\n";
                            json::Json d = json::Json::object();
                            d["file_id"] = file_id;
                            d["shard_id"] = missing_shard.chunk_id;
                            d["target_peer"] = repl_peer.id;
                            events_.log_event("REPAIR_REPLICA_OK", "shard repaired from alternate replica", d);
                        }
                    }
                }
            }
            if (restored_via_replica) continue;

            // Step 5 & 6: Need >= k valid surviving shards from the same stripe
            if (surviving_indices.size() >= need_k) {
                auto [found_repl, repl_peer] = pick_target_peer();
                if (found_repl) {
                    json::Json req = json::Json::object();
                    req["type"] = "REPAIR_SHARD";
                    req["target_shard_id"] = missing_shard.chunk_id;
                    req["target_shard_size"] = static_cast<long long>(missing_shard.size);
                    req["expected_sha256"] = missing_shard.sha256;
                    req["rs_k"] = static_cast<long long>(need_k);
                    req["rs_m"] = static_cast<long long>(total_m);

                    json::Json sources = json::Json::array();
                    for (size_t s_idx : surviving_indices) {
                        const auto& surv_shard = rec.shards[s_idx];
                        std::string src_pid;
                        for (const auto& sp : surv_shard.peers) {
                            auto it = peer_map.find(sp);
                            if (it != peer_map.end() && it->second.state == PeerHealth::ONLINE) {
                                src_pid = sp;
                                break;
                            }
                        }
                        if (src_pid.empty()) continue;
                        const auto& p_info = peer_map[src_pid];
                        json::Json src = json::Json::object();
                        src["peer_id"] = p_info.id;
                        src["address"] = p_info.address;
                        src["port"] = static_cast<long long>(p_info.port);
                        src["shard_id"] = surv_shard.chunk_id;
                        src["sha256"] = surv_shard.sha256;
                        sources.push_back(src);
                    }
                    req["sources"] = sources;

                    TCPClient client(5000, 15000);
                    if (client.connect_to(repl_peer.address, repl_peer.port)) {
                        json::Json resp;
                        std::vector<uint8_t> pl;
                        if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                            resp["type"].as_string() == "REPAIR_SHARD_ACK" &&
                            resp["status"].as_string() == "OK") {
                            missing_shard.peers = {repl_peer.id};
                            missing_shard.status = "HEALTHY";
                            healthy_used_peers.push_back(repl_peer.id);
                            surviving_indices.push_back(m_idx);
                            any_repaired = true;
                            ss << "[Peer-to-Peer Repair] " << missing_shard.chunk_id
                               << " restored onto replacement Peer " << repl_peer.id
                               << " via direct RS(" << (need_k + total_m) << "," << need_k << ") reconstruction\n";
                            json::Json d = json::Json::object();
                            d["file_id"] = file_id;
                            d["shard_id"] = missing_shard.chunk_id;
                            d["target_peer"] = repl_peer.id;
                            events_.log_event("REPAIR_RS_OK",
                                              "shard reconstructed via direct peer-to-peer RS decode", d);
                            continue;
                        }
                    }
                }
            } else {
                insufficient_surviving = true;
            }

            // Explicit last-resort fallback ONLY if enough peers ARE online to host the stripe,
            // yet surviving peer shards were somehow lost on disk (corrupt/missing disks).
            if (online_peers.size() >= need_k + total_m && fs::exists(rec.central_path)) {
                auto [found, repl_peer] = pick_target_peer();
                if (found) {
                    AdaptiveChunker chunker(AdaptivePolicy(), need_k, total_m);
                    uint64_t dummy = 0;
                    const std::string staging_dir = "output/staging/" + file_id;
                    chunker.encode_file(file_id, rec.central_path, staging_dir, dummy);
                    const std::string sh_file = staging_dir + "/" + missing_shard.chunk_id + ".chunk";

                    std::ifstream in_sh(sh_file, std::ios::binary);
                    if (in_sh) {
                        in_sh.seekg(0, std::ios::end);
                        size_t fsz = static_cast<size_t>(in_sh.tellg());
                        in_sh.seekg(0, std::ios::beg);
                        std::vector<uint8_t> sh_bytes(fsz);
                        if (fsz > 0) in_sh.read(reinterpret_cast<char*>(sh_bytes.data()), static_cast<std::streamsize>(fsz));
                        in_sh.close();
                        std::error_code ec;
                        fs::remove_all(staging_dir, ec);

                        TCPClient client(3000, 5000);
                        if (client.connect_to(repl_peer.address, repl_peer.port)) {
                            json::Json store_req = json::Json::object();
                            store_req["type"] = proto::STORE_CHUNK;
                            store_req["chunk_id"] = missing_shard.chunk_id;
                            store_req["sha256"] = missing_shard.sha256;
                            json::Json store_resp;
                            std::vector<uint8_t> rpl;
                            if (client.request(store_req, sh_bytes, store_resp, rpl) == proto::RecvStatus::OK &&
                                store_resp["type"].as_string() == proto::STORE_ACK) {
                                missing_shard.peers = {repl_peer.id};
                                missing_shard.status = "HEALTHY";
                                healthy_used_peers.push_back(repl_peer.id);
                                surviving_indices.push_back(m_idx);
                                any_repaired = true;
                                ss << "[Central-Assisted Repair] " << missing_shard.chunk_id
                                   << " restored onto Peer " << repl_peer.id << "\n";
                                continue;
                            }
                        }
                    }
                }
            }

            all_shards_healthy = false;
            ss << "[Recovery Pending] " << missing_shard.chunk_id
               << (insufficient_surviving
                       ? " (fewer than " + std::to_string(need_k) + " valid shards; waiting with backoff)\n"
                       : " (waiting for an eligible healthy replacement peer)\n");
        }
    }

    if (all_shards_healthy && !any_repaired) {
        if (rec.status != FileDistributionStatus::AVAILABLE) {
            catalog_.update_shards_and_status(file_id, FileDistributionStatus::AVAILABLE,
                                              rec.shards, rec.shard_size, rec.k, rec.m);
        }
        report_out = "All shards are healthy and online across peers. File is fully protected.";
        return true;
    }

    FileDistributionStatus new_status = CatalogManager::evaluate_file_status(rec);
    if (!all_shards_healthy && insufficient_surviving) {
        new_status = FileDistributionStatus::RECOVERY_PENDING;
    }
    catalog_.update_shards_and_status(file_id, new_status, rec.shards, rec.shard_size, rec.k, rec.m);

    if (!all_shards_healthy) {
        json::Json d = json::Json::object();
        d["file_id"] = file_id;
        d["status"] = file_status_str(new_status);
        events_.log_event("REPAIR_PENDING", "recovery pending: waiting for peers/shards with backoff", d);
        schedule_recovery(file_id, false);
    } else {
        json::Json d = json::Json::object();
        d["file_id"] = file_id;
        events_.log_event("REPAIR_COMPLETED", "all missing/corrupt shards repaired and verified", d);
    }

    report_out = ss.str();
    return all_shards_healthy || any_repaired;
}
