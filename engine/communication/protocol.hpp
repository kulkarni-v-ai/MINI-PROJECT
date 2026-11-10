// ============================================================================
// protocol.hpp - E2: application-level wire protocol over TCP
//
// Frame format (simple request/response, spec §5):
//
//   +--------------------+-----------------------+------------------------+
//   | header_len (4B BE) | JSON header (UTF-8)   | binary payload         |
//   +--------------------+-----------------------+------------------------+
//
// The JSON header always carries "type". Chunk transfers declare
// "payload_size" in the header; the payload follows as raw binary
// (chunk payloads are NEVER converted to text, spec §19).
//
// Message types: REGISTER, REGISTER_ACK, HEARTBEAT, HEARTBEAT_ACK,
//                STORE_CHUNK, STORE_ACK, GET_CHUNK, CHUNK_RESPONSE,
//                TRANSFER_ERROR.
// ============================================================================
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "../../third_party/mini_json.hpp"
#include "socket_compat.hpp"

namespace proto {

// Message type constants.
constexpr const char* REGISTER       = "REGISTER";
constexpr const char* REGISTER_ACK   = "REGISTER_ACK";
constexpr const char* HEARTBEAT      = "HEARTBEAT";
constexpr const char* HEARTBEAT_ACK  = "HEARTBEAT_ACK";
constexpr const char* STORE_CHUNK    = "STORE_CHUNK";
constexpr const char* STORE_ACK      = "STORE_ACK";
constexpr const char* GET_CHUNK      = "GET_CHUNK";
constexpr const char* CHUNK_RESPONSE = "CHUNK_RESPONSE";
constexpr const char* TRANSFER_ERROR = "TRANSFER_ERROR";

// Gate-2 Dynamic Peer Registration & Cluster operations
constexpr const char* PEER_REGISTER       = "PEER_REGISTER";
constexpr const char* PEER_REGISTER_ACK   = "PEER_REGISTER_ACK";
constexpr const char* PEER_HEARTBEAT      = "PEER_HEARTBEAT";
constexpr const char* PEER_HEARTBEAT_ACK  = "PEER_HEARTBEAT_ACK";
constexpr const char* QUERY_PEERS         = "QUERY_PEERS";
constexpr const char* QUERY_PEERS_ACK     = "QUERY_PEERS_ACK";

// Session / Auth, Streaming Upload/Download, Verification & Recovery
constexpr const char* SESSION_AUTH            = "SESSION_AUTH";
constexpr const char* SESSION_AUTH_ACK        = "SESSION_AUTH_ACK";
constexpr const char* AUTH_ERROR              = "AUTH_ERROR";
constexpr const char* UPLOAD_INIT             = "UPLOAD_INIT";
constexpr const char* UPLOAD_INIT_ACK         = "UPLOAD_INIT_ACK";
constexpr const char* UPLOAD_PART             = "UPLOAD_PART";
constexpr const char* UPLOAD_PART_ACK         = "UPLOAD_PART_ACK";
constexpr const char* UPLOAD_COMPLETE         = "UPLOAD_COMPLETE";
constexpr const char* UPLOAD_COMPLETE_ACK     = "UPLOAD_COMPLETE_ACK";
constexpr const char* DOWNLOAD_CENTRAL        = "DOWNLOAD_CENTRAL";
constexpr const char* DOWNLOAD_CENTRAL_ACK    = "DOWNLOAD_CENTRAL_ACK";
constexpr const char* VERIFY_CHUNK            = "VERIFY_CHUNK";
constexpr const char* VERIFY_CHUNK_ACK        = "VERIFY_CHUNK_ACK";
constexpr const char* COPY_SHARD              = "COPY_SHARD";
constexpr const char* COPY_SHARD_ACK          = "COPY_SHARD_ACK";
constexpr const char* REPORT_SHARD_ISSUE      = "REPORT_SHARD_ISSUE";
constexpr const char* REPORT_SHARD_ISSUE_ACK  = "REPORT_SHARD_ISSUE_ACK";
constexpr const char* QUERY_EVENTS            = "QUERY_EVENTS";
constexpr const char* QUERY_EVENTS_ACK        = "QUERY_EVENTS_ACK";

constexpr uint32_t MAX_HEADER_SIZE  = 64 * 1024;        // header sanity bound
constexpr uint32_t MAX_PAYLOAD_SIZE = 16 * 1024 * 1024; // payload sanity bound

enum class RecvStatus { OK, CLOSED, TIMEOUT, IO_ERROR };

// Set send/recv timeouts on a socket (milliseconds).
bool set_timeouts(socket_t fd, int recv_ms, int send_ms);

// Per-socket tuning shared by client and server sockets: TCP_NODELAY (the
// protocol sends small frames and waits for the reply, so Nagle + delayed
// ACK would add up to ~200 ms per round trip on Windows) and, on BSD/macOS,
// SO_NOSIGPIPE.
void tune_socket(socket_t fd);

// Reliable send/recv of exactly n bytes — handles partial socket
// reads/writes (spec §19: never assume one recv() gets a whole message).
bool send_all(socket_t fd, const void* data, size_t n);
RecvStatus recv_all(socket_t fd, void* data, size_t n);

// Send one framed message: JSON header + optional binary payload.
bool send_message(socket_t fd, const json::Json& header,
                  const std::vector<uint8_t>& payload = {});

// Receive one framed message. On OK, `header` and `payload` are filled;
// payload size is taken from the header's "payload_size" field and
// validated against MAX_PAYLOAD_SIZE.
RecvStatus recv_message(socket_t fd, json::Json& header,
                        std::vector<uint8_t>& payload);

}  // namespace proto
