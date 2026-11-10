// ============================================================================
// http_server.hpp - Native HTTP/1.1 Upload API & Live Cluster Console
//
// Exposes the Control Plane over HTTP/1.1 on a configurable port (default 8080)
// using Winsock2/POSIX sockets with zero external HTTP libraries:
//   - GET  /                  Interactive Cluster & Recovery Console
//   - POST /api/auth          Authenticate client & issue session token
//   - POST /api/upload        Upload file -> Chunk -> Hash -> RS Encode -> Distribute
//   - GET  /api/status        Cluster summary
//   - GET  /api/peers         Live peer registry & workloads
//   - GET  /api/files         Catalog list
//   - GET  /api/files/<id>    File manifest & per-shard health/locations
//   - GET  /api/events        Audit & recovery log
//   - POST /api/repair/<id>   Trigger durability repair orchestration
//   - GET  /api/download/<id> Central staging fallback download
// ============================================================================
#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include "../communication/socket_compat.hpp"

class CoordinatorService;

class HttpServer {
public:
    HttpServer(CoordinatorService& coordinator,
               std::string bind_address = "0.0.0.0",
               uint16_t port = 8080);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    bool start();
    void stop();
    uint16_t port() const { return port_; }

private:
    CoordinatorService& coord_;
    std::string bind_address_;
    uint16_t port_;

    socket_t listen_fd_ = INVALID_SOCK;
    std::mutex listen_mu_;
    std::atomic<bool> running_{false};
    std::atomic<int> active_{0};
    std::thread accept_thread_;

    void accept_loop();
    void handle_client(socket_t client_fd);
    void close_listener();
};
