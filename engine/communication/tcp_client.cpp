// ============================================================================
// tcp_client.cpp - E2: TCP client (implementation)
// ============================================================================
#include "tcp_client.hpp"

TCPClient::TCPClient(int connect_timeout_ms, int io_timeout_ms)
    : connect_timeout_ms_(connect_timeout_ms), io_timeout_ms_(io_timeout_ms) {}

TCPClient::~TCPClient() { close(); }

bool TCPClient::connect_to(const std::string& address, uint16_t port) {
    close();
    if (!sock_init()) return false;

    fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd_ == INVALID_SOCK) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, address.c_str(), &addr.sin_addr) != 1) {
        close();
        return false;
    }

    // Non-blocking connect bounded by connect_timeout_ms_, then back to
    // blocking mode. Identical on POSIX and Winsock; this matters on Windows,
    // where a plain blocking connect() to a dead local port can take seconds
    // and would stretch heartbeat probes far beyond the configured timeout.
    if (!sock_set_nonblocking(fd_, true)) {
        close();
        return false;
    }
    if (::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        if (!sock_err_in_progress(sock_last_error())) {
            close();
            return false;
        }
        fd_set wfds, efds;
        FD_ZERO(&wfds);
        FD_ZERO(&efds);
        FD_SET(fd_, &wfds);
        FD_SET(fd_, &efds);  // Winsock reports a failed connect via exceptfds
        timeval tv{};
        tv.tv_sec = connect_timeout_ms_ / 1000;
        tv.tv_usec = (connect_timeout_ms_ % 1000) * 1000;
        const int n = ::select(static_cast<int>(fd_) + 1, nullptr, &wfds, &efds, &tv);
        if (n <= 0) {  // timeout or error: peer is unreachable
            close();
            return false;
        }
        int err = 0;
        socklen_t elen = sizeof(err);
        if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR,
                         reinterpret_cast<char*>(&err), &elen) != 0 ||
            err != 0 || !FD_ISSET(fd_, &wfds)) {
            close();
            return false;
        }
    }
    if (!sock_set_nonblocking(fd_, false)) {
        close();
        return false;
    }

    proto::tune_socket(fd_);
    proto::set_timeouts(fd_, io_timeout_ms_, io_timeout_ms_);
    return true;
}

void TCPClient::close() {
    if (fd_ != INVALID_SOCK) {
        sock_close(fd_);
        fd_ = INVALID_SOCK;
    }
}

void TCPClient::set_io_timeout_ms(int ms) {
    io_timeout_ms_ = ms;
    if (fd_ != INVALID_SOCK) proto::set_timeouts(fd_, ms, ms);
}

proto::RecvStatus TCPClient::request(const json::Json& req_header,
                                     const std::vector<uint8_t>& req_payload,
                                     json::Json& resp_header,
                                     std::vector<uint8_t>& resp_payload) {
    if (fd_ == INVALID_SOCK) return proto::RecvStatus::IO_ERROR;
    if (!proto::send_message(fd_, req_header, req_payload))
        return proto::RecvStatus::IO_ERROR;
    return proto::recv_message(fd_, resp_header, resp_payload);
}
