// ============================================================================
// peer.cpp - Peer process (spec §5, §10, Gate-2 E2/E4)
//
// A peer is an independent OS process running a TCP server. It:
//   - answers REGISTER / HEARTBEAT (Gate-1 probe compatibility)
//   - accepts STORE_CHUNK / GET_CHUNK (shard storage & retrieval)
//   - connects to Coordinator over TCP to dynamically register (PEER_REGISTER)
//   - periodically pushes PEER_HEARTBEAT with available storage & active transfers
//
// Usage: peer <ID> <address> <port> [coordinator_address] [coordinator_port]
// ============================================================================
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/coding/reed_solomon.hpp"
#include "../engine/communication/protocol.hpp"
#include "../engine/communication/tcp_client.hpp"
#include "../engine/communication/tcp_server.hpp"
#include "../engine/process/process_manager.hpp"
#include "../third_party/mini_json.hpp"

namespace fs = std::filesystem;

namespace {
std::atomic<uint32_t> g_active_transfers{0};

struct TransferGuard {
    std::atomic<uint32_t>& counter;
    explicit TransferGuard(std::atomic<uint32_t>& c) : counter(c) { ++counter; }
    ~TransferGuard() { --counter; }
};

bool safe_chunk_id(const std::string& id) {
    if (id.empty() || id.size() > 128 || id.find("..") != std::string::npos) return false;
    for (unsigned char c : id) {
        if (c < 0x20 || c == '/' || c == '\\' || c == ':' || c == '*' || c == '?' ||
            c == '"' || c == '<' || c == '>' || c == '|')
            return false;
    }
    return true;
}

bool parse_shard_slot(const std::string& cid, size_t rs_k, size_t rs_m, size_t& slot_out) {
    size_t d_pos = cid.rfind("-D");
    if (d_pos != std::string::npos) {
        try {
            size_t idx = std::stoul(cid.substr(d_pos + 2));
            if (idx < rs_k) { slot_out = idx; return true; }
        } catch (...) {}
    }
    size_t p_pos = cid.rfind("-P");
    if (p_pos != std::string::npos) {
        try {
            size_t idx = std::stoul(cid.substr(p_pos + 2));
            if (idx < rs_m) { slot_out = rs_k + idx; return true; }
        } catch (...) {}
    }
    return false;
}

std::vector<uint8_t> read_binary(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return {};
    return std::vector<uint8_t>(std::istreambuf_iterator<char>(in),
                                std::istreambuf_iterator<char>());
}

class PeerRegistrar {
public:
    PeerRegistrar(std::string id, std::string peer_address, uint16_t peer_port,
                  std::string coord_address, uint16_t coord_port,
                  std::string storage_dir)
        : id_(std::move(id)),
          peer_address_(std::move(peer_address)),
          peer_port_(peer_port),
          coord_address_(std::move(coord_address)),
          coord_port_(coord_port),
          storage_dir_(std::move(storage_dir)) {}

    ~PeerRegistrar() { stop(); }

    void start() {
        if (coord_address_.empty() || coord_port_ == 0) return;
        running_.store(true);
        thread_ = std::thread(&PeerRegistrar::run, this);
    }

    void stop() {
        if (!running_.exchange(false)) return;
        if (thread_.joinable()) thread_.join();
    }

private:
    std::string id_;
    std::string peer_address_;
    uint16_t peer_port_;
    std::string coord_address_;
    uint16_t coord_port_;
    std::string storage_dir_;

    std::atomic<bool> running_{false};
    std::thread thread_;

    uint64_t get_available_storage() const {
        std::error_code ec;
        auto space = fs::space(storage_dir_, ec);
        return ec ? 0 : space.available;
    }

    void run() {
        bool registered = false;
        int heartbeat_interval_ms = 500;

        // Dynamic registration with retry
        while (running_.load() && !registered) {
            TCPClient client(1500, 1500);
            if (client.connect_to(coord_address_, coord_port_)) {
                json::Json req = json::Json::object();
                req["type"] = proto::PEER_REGISTER;
                req["peer_id"] = id_;
                req["address"] = peer_address_;
                req["port"] = static_cast<long long>(peer_port_);
                req["available_storage_bytes"] = static_cast<long long>(get_available_storage());
                req["active_transfers"] = static_cast<long long>(g_active_transfers.load());
                req["transfer_capacity"] = 10LL;

                json::Json resp;
                std::vector<uint8_t> pl;
                if (client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
                    resp["type"].as_string() == proto::PEER_REGISTER_ACK) {
                    registered = true;
                    if (resp.contains("heartbeat_interval_ms"))
                        heartbeat_interval_ms = static_cast<int>(resp["heartbeat_interval_ms"].as_int());
                    std::cout << "[peer " << id_ << "] Dynamically registered with Coordinator at "
                              << coord_address_ << ":" << coord_port_ << std::endl;
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }

        // Periodic heartbeat loop
        while (running_.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_interval_ms));
            if (!running_.load()) break;

            TCPClient client(1000, 1000);
            if (client.connect_to(coord_address_, coord_port_)) {
                json::Json req = json::Json::object();
                req["type"] = proto::PEER_HEARTBEAT;
                req["peer_id"] = id_;
                req["available_storage_bytes"] = static_cast<long long>(get_available_storage());
                req["active_transfers"] = static_cast<long long>(g_active_transfers.load());
                req["transfer_capacity"] = 10LL;

                json::Json resp;
                std::vector<uint8_t> pl;
                client.request(req, {}, resp, pl);
            }
        }
    }
};
}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string> args = proc::command_line_args(argc, argv);
    if (args.size() < 4) {
        std::cerr << "usage: peer <ID> <address> <port> [coordinator_address] [coordinator_port]\n";
        return 1;
    }
    const std::string id = args[1];
    const std::string address = args[2];
    int port_i = 0;
    try {
        port_i = std::stoi(args[3]);
    } catch (...) {
        port_i = 0;
    }
    if (port_i < 1 || port_i > 65535) {
        std::cerr << "invalid port: " << args[3] << "\n";
        return 1;
    }
    const auto port = static_cast<uint16_t>(port_i);

    std::string coord_address;
    uint16_t coord_port = 0;
    if (args.size() >= 6) {
        coord_address = args[4];
        coord_port = static_cast<uint16_t>(std::stoi(args[5]));
    } else {
        // Optional configuration fallback if present in config.json
        std::ifstream cfg("config.json");
        if (cfg) {
            try {
                std::stringstream ss;
                ss << cfg.rdbuf();
                auto j = json::Json::parse(ss.str());
                if (j.contains("coordinator")) {
                    coord_address = j["coordinator"]["address"].as_string();
                    coord_port = static_cast<uint16_t>(j["coordinator"]["port"].as_int());
                }
            } catch (...) {}
        }
    }

    if (!sock_init()) {
        std::cerr << "[peer " << id << "] socket subsystem initialisation failed\n";
        return 1;
    }

    // Isolated per-peer storage (spec §10).
    const std::string storage_dir = "storage/peer_" + id + "/chunks";
    fs::create_directories(storage_dir);

    TCPServer::Handler handler =
        [&id, &storage_dir](const json::Json& req,
                            const std::vector<uint8_t>& req_payload,
                            json::Json& resp,
                            std::vector<uint8_t>& resp_payload) -> bool {
        const std::string type = req["type"].as_string();

        if (type == proto::REGISTER) {
            resp["type"] = proto::REGISTER_ACK;
            resp["peer_id"] = id;
            resp["status"] = "OK";
            return true;
        }

        if (type == proto::HEARTBEAT) {
            resp["type"] = proto::HEARTBEAT_ACK;
            resp["peer_id"] = id;
            resp["status"] = "ALIVE";
            return true;
        }

        if (type == proto::STORE_CHUNK) {
            TransferGuard guard(g_active_transfers);
            const std::string chunk_id = req["chunk_id"].as_string();
            if (!safe_chunk_id(chunk_id)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "bad chunk_id";
                return true;
            }
            if (req.contains("sha256")) {
                const std::string got =
                    ChunkManager::sha256_of_bytes(req_payload);
                if (got != req["sha256"].as_string()) {
                    resp["type"] = proto::TRANSFER_ERROR;
                    resp["reason"] = "sha256 mismatch on store";
                    return true;
                }
            }
            const std::string path = storage_dir + "/" + chunk_id + ".chunk";
            const std::string tmp = path + ".tmp";
            {
                std::ofstream out(tmp, std::ios::binary);
                out.write(reinterpret_cast<const char*>(req_payload.data()),
                          static_cast<std::streamsize>(req_payload.size()));
            }
            fs::rename(tmp, path);
            resp["type"] = proto::STORE_ACK;
            resp["chunk_id"] = chunk_id;
            resp["status"] = "OK";
            resp["size"] = static_cast<long long>(req_payload.size());
            return true;
        }

        if (type == proto::GET_CHUNK) {
            TransferGuard guard(g_active_transfers);
            const std::string chunk_id = req["chunk_id"].as_string();
            if (!safe_chunk_id(chunk_id)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "bad chunk_id";
                return true;
            }
            const std::string path = storage_dir + "/" + chunk_id + ".chunk";
            if (!fs::exists(path)) {
                resp["type"] = proto::TRANSFER_ERROR;
                resp["reason"] = "chunk not found on peer " + id;
                return true;
            }
            std::vector<uint8_t> data = read_binary(path);
            resp["type"] = proto::CHUNK_RESPONSE;
            resp["chunk_id"] = chunk_id;
            resp["peer_id"] = id;

            if (req.contains("simulate") &&
                req["simulate"].as_string() == "interrupt") {
                resp["truncate_payload_to"] =
                    static_cast<long long>(data.size() / 2);
            }
            resp_payload = std::move(data);
            return true;
        }

        if (type == proto::VERIFY_CHUNK) {
            const std::string chunk_id = req["chunk_id"].as_string();
            if (!safe_chunk_id(chunk_id)) {
                resp["type"] = proto::VERIFY_CHUNK_ACK;
                resp["status"] = "BAD_ID";
                return true;
            }
            const std::string path = storage_dir + "/" + chunk_id + ".chunk";
            if (!fs::exists(path)) {
                resp["type"] = proto::VERIFY_CHUNK_ACK;
                resp["status"] = "MISSING";
                return true;
            }
            const std::string got_sha = ChunkManager::sha256_of_file(path);
            const uint64_t got_size = fs::file_size(path);
            bool size_ok = !req.contains("expected_size") ||
                           got_size == static_cast<uint64_t>(req["expected_size"].as_int());
            bool sha_ok = !req.contains("expected_sha256") ||
                          req["expected_sha256"].as_string().empty() ||
                          got_sha == req["expected_sha256"].as_string();

            resp["type"] = proto::VERIFY_CHUNK_ACK;
            resp["chunk_id"] = chunk_id;
            resp["actual_sha256"] = got_sha;
            resp["size"] = static_cast<long long>(got_size);
            resp["status"] = (size_ok && sha_ok) ? "OK" : "CORRUPT";
            return true;
        }

        if (type == proto::COPY_SHARD) {
            TransferGuard guard(g_active_transfers);
            const std::string target_shard_id = req["target_shard_id"].as_string();
            const size_t shard_size = static_cast<size_t>(req["target_shard_size"].as_int());
            const std::string expected_sha256 = req["expected_sha256"].as_string();
            const std::string src_addr = req["source_address"].as_string();
            const uint16_t src_port = static_cast<uint16_t>(req["source_port"].as_int());
            const std::string src_cid = req.contains("source_shard_id")
                ? req["source_shard_id"].as_string()
                : target_shard_id;

            if (!safe_chunk_id(target_shard_id)) {
                resp["type"] = proto::COPY_SHARD_ACK;
                resp["status"] = "FAILED";
                resp["reason"] = "bad target_shard_id";
                return true;
            }

            TCPClient c(3000, 5000);
            if (!c.connect_to(src_addr, src_port)) {
                resp["type"] = proto::COPY_SHARD_ACK;
                resp["status"] = "FAILED";
                resp["reason"] = "cannot connect to replica peer " + src_addr + ":" + std::to_string(src_port);
                return true;
            }

            json::Json get_req = json::Json::object();
            get_req["type"] = proto::GET_CHUNK;
            get_req["chunk_id"] = src_cid;
            json::Json get_resp;
            std::vector<uint8_t> pl;
            if (c.request(get_req, {}, get_resp, pl) != proto::RecvStatus::OK ||
                get_resp["type"].as_string() != proto::CHUNK_RESPONSE ||
                pl.size() != shard_size ||
                (!expected_sha256.empty() && ChunkManager::sha256_of_bytes(pl) != expected_sha256)) {
                resp["type"] = proto::COPY_SHARD_ACK;
                resp["status"] = "FAILED";
                resp["reason"] = "replica fetch failed or SHA-256 mismatch";
                return true;
            }

            const std::string path = storage_dir + "/" + target_shard_id + ".chunk";
            const std::string tmp = path + ".tmp";
            {
                std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(pl.data()), static_cast<std::streamsize>(pl.size()));
            }
            fs::rename(tmp, path);

            resp["type"] = proto::COPY_SHARD_ACK;
            resp["status"] = "OK";
            resp["target_shard_id"] = target_shard_id;
            resp["peer_id"] = id;
            return true;
        }

        if (type == "REPAIR_SHARD") {
            TransferGuard guard(g_active_transfers);
            const std::string target_shard_id = req["target_shard_id"].as_string();
            const size_t shard_size = static_cast<size_t>(req["target_shard_size"].as_int());
            const std::string expected_sha256 = req["expected_sha256"].as_string();
            const size_t rs_k = req.contains("rs_k") ? static_cast<size_t>(req["rs_k"].as_int()) : 2;
            const size_t rs_m = req.contains("rs_m") ? static_cast<size_t>(req["rs_m"].as_int()) : 1;
            const size_t rs_n = rs_k + rs_m;

            if (!safe_chunk_id(target_shard_id)) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "bad target_shard_id";
                return true;
            }

            // Direct Peer-to-Peer retrieval: Fetch shards directly from surviving peer sources
            std::vector<std::vector<uint8_t>> bufs(rs_n, std::vector<uint8_t>(shard_size, 0));
            std::vector<bool> present(rs_n, false);

            if (req.contains("sources") && req["sources"].is_array()) {
                for (const auto& src : req["sources"].items()) {
                    const std::string src_addr = src["address"].as_string();
                    const uint16_t s_port = static_cast<uint16_t>(src["port"].as_int());
                    const std::string s_cid = src["shard_id"].as_string();
                    const std::string s_sha = src["sha256"].as_string();

                    size_t slot = 0;
                    if (!parse_shard_slot(s_cid, rs_k, rs_m, slot)) continue;
                    if (present[slot]) continue;

                    TCPClient c(3000, 5000);
                    if (c.connect_to(src_addr, s_port)) {
                        json::Json get_req = json::Json::object();
                        get_req["type"] = proto::GET_CHUNK;
                        get_req["chunk_id"] = s_cid;
                        json::Json get_resp;
                        std::vector<uint8_t> pl;
                        if (c.request(get_req, {}, get_resp, pl) == proto::RecvStatus::OK &&
                            get_resp["type"].as_string() == proto::CHUNK_RESPONSE &&
                            pl.size() == shard_size &&
                            ChunkManager::sha256_of_bytes(pl) == s_sha) {
                            bufs[slot] = std::move(pl);
                            present[slot] = true;
                        }
                    }
                }
            }

            size_t count_avail = 0;
            for (bool b : present) if (b) ++count_avail;

            if (count_avail < rs_k) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "insufficient surviving peer sources reached (" +
                                 std::to_string(count_avail) + "/" + std::to_string(rs_k) + ")";
                return true;
            }

            // Perform local Reed-Solomon decode inside target peer
            ReedSolomon rs(rs_k, rs_m);
            std::vector<uint8_t*> ptrs(rs_n);
            for (size_t i = 0; i < rs_n; ++i) ptrs[i] = bufs[i].data();
            if (!rs.decode(ptrs, present, shard_size)) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "RS decoding failed";
                return true;
            }

            size_t target_slot = 0;
            if (!parse_shard_slot(target_shard_id, rs_k, rs_m, target_slot)) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "cannot determine RS slot from target_shard_id";
                return true;
            }

            const std::string actual_sha = ChunkManager::sha256_of_bytes(bufs[target_slot]);
            if (!expected_sha256.empty() && actual_sha != expected_sha256) {
                resp["type"] = "REPAIR_SHARD_ACK";
                resp["status"] = "FAILED";
                resp["reason"] = "reconstructed shard SHA-256 mismatch";
                return true;
            }

            // Save reconstructed shard atomically
            const std::string path = storage_dir + "/" + target_shard_id + ".chunk";
            const std::string tmp = path + ".tmp";
            {
                std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(bufs[target_slot].data()), static_cast<std::streamsize>(shard_size));
            }
            fs::rename(tmp, path);

            resp["type"] = "REPAIR_SHARD_ACK";
            resp["status"] = "OK";
            resp["target_shard_id"] = target_shard_id;
            resp["peer_id"] = id;
            return true;
        }

        resp["type"] = proto::TRANSFER_ERROR;
        resp["reason"] = "unknown message type: " + type;
        return true;
    };

    TCPServer server(address, port, handler);
    if (!server.start()) {
        std::cerr << "[peer " << id << "] failed to bind " << address << ":"
                  << port << "\n";
        return 1;
    }
    std::cout << "[peer " << id << "] ONLINE at " << address << ":" << port
              << " storage=" << storage_dir << std::endl;

    // Start background dynamic registration & heartbeat publisher if coordinator is configured
    PeerRegistrar registrar(id, address, port, coord_address, coord_port, storage_dir);
    if (!coord_address.empty() && coord_port != 0) {
        registrar.start();
    }

    // Controller-requested shutdown handler
    proc::install_shutdown_handler([&server, &registrar]() {
        registrar.stop();
        server.stop();
    });

    server.run();
    registrar.stop();
    proc::remove_shutdown_handler();
    std::cout << "[peer " << id << "] stopped" << std::endl;
    return 0;
}
