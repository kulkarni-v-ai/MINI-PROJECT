// ============================================================================
// tcp_client.hpp - E2: TCP client (request/response with timeouts)
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../third_party/mini_json.hpp"
#include "protocol.hpp"
#include "socket_compat.hpp"

class TCPClient {
public:
    // connect_timeout_ms applies to connect; io_timeout_ms to send/recv.
    TCPClient(int connect_timeout_ms = 2000, int io_timeout_ms = 3000);
    ~TCPClient();

    bool connect_to(const std::string& address, uint16_t port);
    void close();
    bool is_connected() const { return fd_ != INVALID_SOCK; }

    // Send one request and wait for one response.
    // Returns the receive status; on OK, resp_header/resp_payload are filled.
    proto::RecvStatus request(const json::Json& req_header,
                              const std::vector<uint8_t>& req_payload,
                              json::Json& resp_header,
                              std::vector<uint8_t>& resp_payload);

    void set_io_timeout_ms(int ms);

private:
    socket_t fd_ = INVALID_SOCK;
    int connect_timeout_ms_;
    int io_timeout_ms_;
};
