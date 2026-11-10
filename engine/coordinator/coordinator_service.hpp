// ============================================================================
// coordinator_service.hpp - Central Coordination Server (Control Plane & Recovery Orchestrator)
//
// Implements:
//   - Session & Upload API (TCP + optional HTTP/1.1 server)
//   - Adaptive Chunking, SHA-256 hashing, Systematic RS(k+m, k) encoding
//   - Dynamic Placement Scheduler across healthy storage peers
//   - Multi-file Catalog & atomic Metadata Store + Event Audit Log
//   - Real-time Peer Health Monitor (ONLINE -> SUSPECT -> OFFLINE)
//   - Automated & On-demand 9-Step Recovery Manager with exponential backoff
// ============================================================================
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../catalog/catalog_manager.hpp"
#include "../communication/tcp_server.hpp"
#include "../metadata/metadata_manager.hpp"
#include "../registry/peer_registry.hpp"
#include "../session/session_manager.hpp"

class HttpServer;

class CoordinatorService {
public:
    CoordinatorService(std::string bind_address = "0.0.0.0",
                       uint16_t port = 5000,
                       int heartbeat_interval_ms = 500,
                       int heartbeat_timeout_ms = 1000,
                       int failure_threshold = 2,
                       uint32_t rs_k = 2,
                       uint32_t rs_m = 1,
                       uint16_t http_port = 0,
                       std::string auth_token = "rfe-demo-token",
                       bool auto_recover = true,
                       bool require_auth = true);
    ~CoordinatorService();

    bool start();
    void stop();
    void run();

    PeerRegistry& registry() { return registry_; }
    const PeerRegistry& registry() const { return registry_; }

    CatalogManager& catalog() { return catalog_; }
    const CatalogManager& catalog() const { return catalog_; }

    SessionManager& sessions() { return sessions_; }
    const SessionManager& sessions() const { return sessions_; }

    uint16_t port() const { return port_; }
    uint16_t http_port() const { return http_port_; }
    std::string address() const { return bind_address_; }
    int heartbeat_timeout_ms() const { return heartbeat_timeout_ms_; }
    int failure_threshold() const { return failure_threshold_; }
    uint32_t rs_k() const { return rs_k_; }
    uint32_t rs_m() const { return rs_m_; }
    bool require_auth() const { return require_auth_; }

    // Execute Chunking -> SHA-256 -> RS Encoding -> Dynamic Placement -> Peer Push
    bool process_uploaded_file(const std::string& file_id, FileRecord& rec_out);

    // 9-Step Recovery Process: Replica fetch or RS(k+m, k) peer-to-peer repair
    bool repair_file(const std::string& file_id, std::string& report_out);

    // Enqueue a file for background recovery retry with exponential backoff
    void schedule_recovery(const std::string& file_id, bool immediate = true);

    json::Json read_events() const { return events_.read_events(); }

private:
    struct PendingRepair {
        int attempt = 0;
        std::chrono::steady_clock::time_point next_try;
    };

    std::string bind_address_;
    uint16_t port_;
    int heartbeat_interval_ms_;
    int heartbeat_timeout_ms_;
    int failure_threshold_;
    uint32_t rs_k_;
    uint32_t rs_m_;
    uint16_t http_port_;
    bool auto_recover_;
    bool require_auth_;

    PeerRegistry registry_;
    CatalogManager catalog_{"metadata", "storage"};
    MetadataManager events_{"metadata"};
    SessionManager sessions_;

    std::unique_ptr<TCPServer> server_;
    std::unique_ptr<HttpServer> http_server_;
    std::atomic<bool> running_{false};
    std::thread server_thread_;
    std::thread monitor_thread_;
    std::thread recovery_thread_;

    std::mutex pending_mu_;
    std::condition_variable recovery_cv_;
    std::map<std::string, PendingRepair> pending_repairs_;

    bool check_client_auth(const json::Json& req, json::Json& resp);
    bool verify_peer_shard(const PeerInfo& peer, const ShardMeta& shard);
    void reconcile_returning_peer(const std::string& peer_id);

    bool handle_request(const json::Json& req,
                        const std::vector<uint8_t>& req_payload,
                        json::Json& resp,
                        std::vector<uint8_t>& resp_payload);

    void health_monitor_loop();
    void recovery_worker_loop();
};
