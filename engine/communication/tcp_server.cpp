// ============================================================================
// tcp_server.cpp - E2: TCP server (implementation)
// ============================================================================
#include "tcp_server.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <iostream>
#include <thread>

#include "protocol.hpp"

TCPServer::TCPServer(std::string address, uint16_t port, Handler handler)
    : address_(std::move(address)), port_(port), handler_(std::move(handler)) {}

TCPServer::~TCPServer() { stop(); }

bool TCPServer::start() {
    if (!sock_init()) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (::inet_pton(AF_INET, address_.c_str(), &addr.sin_addr) != 1) return false;

    // A peer that was just killed may still hold its port for a moment
    // (process teardown / TIME_WAIT), so retry the bind for a few seconds.
    for (int attempt = 0; attempt < 40; ++attempt) {
        socket_t fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd == INVALID_SOCK) return false;
#ifndef _WIN32
        // POSIX: allow quick restarts. Deliberately NOT set on Windows, where
        // SO_REUSEADDR would let a second process hijack a port in use.
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<const char*>(&opt), sizeof(opt));
#endif
        if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0 &&
            ::listen(fd, 16) == 0) {
            std::lock_guard<std::mutex> lk(listen_mu_);
            listen_fd_ = fd;
            running_ = true;
            return true;
        }
        const int e = sock_last_error();
        sock_close(fd);
        if (!sock_err_addr_in_use(e)) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return false;
}

void TCPServer::close_listener() {
    std::lock_guard<std::mutex> lk(listen_mu_);
    if (listen_fd_ != INVALID_SOCK) {
        sock_close(listen_fd_);
        listen_fd_ = INVALID_SOCK;
    }
}

void TCPServer::run() {
    in_run_ = true;
    socket_t lfd;
    {
        std::lock_guard<std::mutex> lk(listen_mu_);
        lfd = listen_fd_;
    }
    int consecutive_errors = 0;
    while (running_ && lfd != INVALID_SOCK) {
        sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        socket_t client =
            ::accept(lfd, reinterpret_cast<sockaddr*>(&client_addr), &len);
        if (client == INVALID_SOCK) {
            if (!running_) break;                  // shutting down
            if (++consecutive_errors > 50) break;  // listener is broken
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            continue;                              // transient accept error
        }
        consecutive_errors = 0;
        if (!running_) {  // this was the shutdown wake-up connection
            sock_close(client);
            break;
        }
        ++active_;
        try {
            std::thread([this, client]() {
                serve_connection(client);
                --active_;
            }).detach();
        } catch (...) {  // could not create a thread: drop this connection
            --active_;
            sock_close(client);
        }
    }
    running_ = false;
    close_listener();

    // Let in-flight requests finish (bounded) before the caller tears the
    // server down.
    for (int i = 0; i < 200 && active_ > 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    in_run_ = false;
}

void TCPServer::serve_connection(socket_t client_fd) {
    proto::tune_socket(client_fd);
    proto::set_timeouts(client_fd, 5000, 5000);
    // Request/response loop: one connection may serve several requests.
    while (running_) {
        json::Json req_header;
        std::vector<uint8_t> req_payload;
        proto::RecvStatus st = proto::recv_message(client_fd, req_header, req_payload);
        if (st != proto::RecvStatus::OK) break;  // closed / timeout / error

        json::Json resp_header = json::Json::object();
        std::vector<uint8_t> resp_payload;
        bool keep = true;
        try {
            keep = handler_(req_header, req_payload, resp_header, resp_payload);
        } catch (const std::exception& e) {
            // Never silently ignore failures: report TRANSFER_ERROR.
            resp_header = json::Json::object();
            resp_header["type"] = proto::TRANSFER_ERROR;
            resp_header["reason"] = std::string("internal: ") + e.what();
            resp_payload.clear();
        }

        // Test hook (T03 interrupted transfer): if the handler sets
        // "truncate_payload_to", the header still declares the FULL payload
        // size but only that many payload bytes go onto the wire, then the
        // connection closes — a genuine mid-transfer interruption.
        long long truncate_to = -1;
        if (resp_header.contains("truncate_payload_to")) {
            truncate_to = resp_header["truncate_payload_to"].as_int();
            resp_header.members().erase("truncate_payload_to");
        }
        if (truncate_to >= 0) {
            json::Json h = resp_header;
            h["payload_size"] = static_cast<long long>(resp_payload.size());
            const std::string hs = h.dump();
            uint32_t len = htonl(static_cast<uint32_t>(hs.size()));
            proto::send_all(client_fd, &len, sizeof(len));
            proto::send_all(client_fd, hs.data(), hs.size());
            size_t n = std::min(static_cast<size_t>(truncate_to),
                                resp_payload.size());
            if (n > 0) proto::send_all(client_fd, resp_payload.data(), n);
            break;  // close: receiver sees EOF before the declared length
        }

        if (!proto::send_message(client_fd, resp_header, resp_payload)) break;
        if (!keep) break;
    }
    sock_close(client_fd);
}

void TCPServer::stop() {
    const bool was_running = running_.exchange(false);

    if (was_running) {
        // Wake a blocked accept() so run() can exit. A self-connect is
        // portable across Windows and POSIX; the accept loop observes
        // running_ == false immediately after accepting it.
        socket_t w = ::socket(AF_INET, SOCK_STREAM, 0);
        if (w != INVALID_SOCK) {
            sockaddr_in a{};
            a.sin_family = AF_INET;
            a.sin_port = htons(port_);
            const char* host = (address_ == "0.0.0.0") ? "127.0.0.1" : address_.c_str();
            ::inet_pton(AF_INET, host, &a.sin_addr);
            ::connect(w, reinterpret_cast<sockaddr*>(&a), sizeof(a));
            sock_close(w);
        }
    }
    // If run() is not active (never started / already finished), nobody else
    // will close the listener.
    if (!in_run_) close_listener();
}
