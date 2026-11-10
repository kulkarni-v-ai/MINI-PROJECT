// ============================================================================
// protocol.cpp - E2: application-level wire protocol over TCP (implementation)
// ============================================================================
#include "protocol.hpp"

#include <cstring>

namespace proto {

bool set_timeouts(socket_t fd, int recv_ms, int send_ms) {
#ifdef _WIN32
    // Winsock takes a DWORD of milliseconds (not a timeval).
    DWORD r = static_cast<DWORD>(recv_ms), s = static_cast<DWORD>(send_ms);
    bool ok1 = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,
                          reinterpret_cast<const char*>(&r), sizeof(r)) == 0;
    bool ok2 = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO,
                          reinterpret_cast<const char*>(&s), sizeof(s)) == 0;
    return ok1 && ok2;
#else
    timeval rtv{recv_ms / 1000, (recv_ms % 1000) * 1000};
    timeval stv{send_ms / 1000, (send_ms % 1000) * 1000};
    bool ok1 = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rtv, sizeof(rtv)) == 0;
    bool ok2 = setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &stv, sizeof(stv)) == 0;
    return ok1 && ok2;
#endif
}

void tune_socket(socket_t fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY,
               reinterpret_cast<const char*>(&one), sizeof(one));
#ifdef SO_NOSIGPIPE
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE,
               reinterpret_cast<const char*>(&one), sizeof(one));
#endif
}

bool send_all(socket_t fd, const void* data, size_t n) {
    const char* p = static_cast<const char*>(data);
    size_t sent = 0;
    while (sent < n) {  // loop: a single send() may write only part of the data
#ifdef _WIN32
        const int r = ::send(fd, p + sent, static_cast<int>(n - sent), 0);
#else
        const ssize_t r = ::send(fd, p + sent, n - sent, MSG_NOSIGNAL);
#endif
        if (r < 0) {
            if (sock_err_interrupted(sock_last_error())) continue;
            return false;
        }
        if (r == 0) return false;
        sent += static_cast<size_t>(r);
    }
    return true;
}

RecvStatus recv_all(socket_t fd, void* data, size_t n) {
    char* p = static_cast<char*>(data);
    size_t got = 0;
    while (got < n) {  // loop: one recv() may return only part of the message
#ifdef _WIN32
        const int r = ::recv(fd, p + got, static_cast<int>(n - got), 0);
#else
        const ssize_t r = ::recv(fd, p + got, n - got, 0);
#endif
        if (r < 0) {
            const int e = sock_last_error();
            if (sock_err_interrupted(e)) continue;
            if (sock_err_timeout(e)) return RecvStatus::TIMEOUT;
            return RecvStatus::IO_ERROR;
        }
        if (r == 0) return RecvStatus::CLOSED;  // peer closed the connection
        got += static_cast<size_t>(r);
    }
    return RecvStatus::OK;
}

bool send_message(socket_t fd, const json::Json& header,
                  const std::vector<uint8_t>& payload) {
    json::Json h = header;
    h["payload_size"] = static_cast<long long>(payload.size());
    const std::string hs = h.dump();  // compact JSON header
    if (hs.size() > MAX_HEADER_SIZE) return false;

    // Length prefix + header go out in ONE send (a single small segment);
    // the binary payload follows. Wire format is unchanged.
    std::string frame;
    frame.resize(sizeof(uint32_t) + hs.size());
    const uint32_t len = htonl(static_cast<uint32_t>(hs.size()));
    std::memcpy(&frame[0], &len, sizeof(len));
    std::memcpy(&frame[sizeof(len)], hs.data(), hs.size());
    if (!send_all(fd, frame.data(), frame.size())) return false;
    if (!payload.empty() && !send_all(fd, payload.data(), payload.size()))
        return false;
    return true;
}

RecvStatus recv_message(socket_t fd, json::Json& header,
                        std::vector<uint8_t>& payload) {
    uint32_t len_net = 0;
    RecvStatus st = recv_all(fd, &len_net, sizeof(len_net));
    if (st != RecvStatus::OK) return st;

    uint32_t len = ntohl(len_net);
    if (len == 0 || len > MAX_HEADER_SIZE) return RecvStatus::IO_ERROR;  // validate lengths

    std::string hs(len, '\0');
    st = recv_all(fd, &hs[0], len);
    if (st != RecvStatus::OK) return st;

    try {
        header = json::Json::parse(hs);
    } catch (...) {
        return RecvStatus::IO_ERROR;  // malformed header
    }

    uint32_t payload_size = 0;
    if (header.contains("payload_size") && header["payload_size"].is_number()) {
        long long ps = header["payload_size"].as_int();
        if (ps < 0 || ps > MAX_PAYLOAD_SIZE) return RecvStatus::IO_ERROR;
        payload_size = static_cast<uint32_t>(ps);
    }

    payload.clear();
    if (payload_size > 0) {
        payload.resize(payload_size);
        st = recv_all(fd, payload.data(), payload_size);
        if (st != RecvStatus::OK) {
            payload.clear();  // never hand out a partial payload
            return st;
        }
    }
    return RecvStatus::OK;
}

}  // namespace proto
