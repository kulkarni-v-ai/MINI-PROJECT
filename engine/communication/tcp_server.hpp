// ============================================================================
// tcp_server.hpp - E2: TCP server (one std::thread per connection)
// ============================================================================
#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "../../third_party/mini_json.hpp"
#include "socket_compat.hpp"

class TCPServer {
public:
    // Handler: given a request header + binary payload, fill the response
    // header + binary payload. Return false to close the connection.
    using Handler = std::function<bool(const json::Json& req_header,
                                       const std::vector<uint8_t>& req_payload,
                                       json::Json& resp_header,
                                       std::vector<uint8_t>& resp_payload)>;

    TCPServer(std::string address, uint16_t port, Handler handler);
    ~TCPServer();

    TCPServer(const TCPServer&) = delete;
    TCPServer& operator=(const TCPServer&) = delete;

    bool start();   // bind + listen; returns false on error
    void run();     // accept loop (blocking); spawns a thread per connection
    void stop();    // graceful shutdown; safe to call from another thread

    bool is_running() const { return running_; }

private:
    std::string address_;
    uint16_t port_;
    Handler handler_;
    socket_t listen_fd_ = INVALID_SOCK;
    std::mutex listen_mu_;                 // guards listen_fd_
    std::atomic<bool> running_{false};
    std::atomic<bool> in_run_{false};
    std::atomic<int> active_{0};           // connections being served

    void serve_connection(socket_t client_fd);
    void close_listener();
};
