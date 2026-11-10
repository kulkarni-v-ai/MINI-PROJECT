// ============================================================================
// rfe.cpp - Resilient File Engine Native Terminal CLI
//
// Commands:
//   rfe connect [ip:port] [--user <name>] [--token <token>]
//   rfe upload <file-path> [--defer]
//   rfe files
//   rfe search <filename>
//   rfe info <file-id>
//   rfe download <file-id> [--source central]
//   rfe peers
//   rfe status
//   rfe transfers
//   rfe repair <file-id>
//   rfe events [count]
// ============================================================================
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "../engine/catalog/catalog_manager.hpp"
#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/communication/protocol.hpp"
#include "../engine/communication/socket_compat.hpp"
#include "../engine/communication/tcp_client.hpp"
#include "../engine/process/process_manager.hpp"
#include "../engine/recovery/client_download.hpp"
#include "../engine/registry/peer_registry.hpp"
#include "../models/peer.hpp"
#include "../third_party/mini_json.hpp"

namespace fs = std::filesystem;

namespace {

std::string format_bytes(uint64_t bytes) {
    std::ostringstream ss;
    ss << std::fixed << std::setprecision(2);
    if (bytes >= 1024ULL * 1024ULL * 1024ULL) {
        ss << (static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0)) << " GB";
    } else if (bytes >= 1024ULL * 1024ULL) {
        ss << (static_cast<double>(bytes) / (1024.0 * 1024.0)) << " MB";
    } else if (bytes >= 1024ULL) {
        ss << (static_cast<double>(bytes) / 1024.0) << " KB";
    } else {
        ss << bytes << " B";
    }
    return ss.str();
}

struct SessionConfig {
    std::string coord_address = "127.0.0.1";
    uint16_t coord_port = 5000;
    uint16_t http_port = 8080;
    std::string client_id = "cli-operator";
    std::string auth_token = "rfe-demo-token";
    std::string session_id;
    std::string session_token;

    static SessionConfig load() {
        SessionConfig s;
        for (const char* cfg_path : {"config.json", "config/config.json"}) {
            std::ifstream cfg(cfg_path);
            if (cfg) {
                try {
                    std::stringstream ss;
                    ss << cfg.rdbuf();
                    auto j = json::Json::parse(ss.str());
                    if (j.contains("coordinator")) {
                        s.coord_address = j["coordinator"]["address"].as_string();
                        s.coord_port = static_cast<uint16_t>(j["coordinator"]["port"].as_int());
                        if (s.coord_address == "0.0.0.0") s.coord_address = "127.0.0.1";
                    }
                    if (j.contains("http_port")) s.http_port = static_cast<uint16_t>(j["http_port"].as_int());
                    if (j.contains("auth_token")) s.auth_token = j["auth_token"].as_string();
                } catch (...) {}
                break;
            }
        }

        std::ifstream session_in("metadata/session.json");
        if (session_in) {
            try {
                std::stringstream ss;
                ss << session_in.rdbuf();
                auto j = json::Json::parse(ss.str());
                if (j.contains("address")) s.coord_address = j["address"].as_string();
                if (j.contains("port")) s.coord_port = static_cast<uint16_t>(j["port"].as_int());
                if (j.contains("http_port")) s.http_port = static_cast<uint16_t>(j["http_port"].as_int());
                if (j.contains("client_id")) s.client_id = j["client_id"].as_string();
                if (j.contains("auth_token")) s.auth_token = j["auth_token"].as_string();
                if (j.contains("session_id")) s.session_id = j["session_id"].as_string();
                if (j.contains("session_token")) s.session_token = j["session_token"].as_string();
            } catch (...) {}
        }
        return s;
    }

    void save() const {
        std::error_code ec;
        fs::create_directories("metadata", ec);
        std::ofstream out("metadata/session.json", std::ios::trunc);
        json::Json j = json::Json::object();
        j["address"] = coord_address;
        j["port"] = static_cast<long long>(coord_port);
        j["http_port"] = static_cast<long long>(http_port);
        j["client_id"] = client_id;
        j["auth_token"] = auth_token;
        j["session_id"] = session_id;
        j["session_token"] = session_token;
        out << j.dump(2);
    }
};

bool authenticate_session(SessionConfig& session, std::string& err_out, long long* latency_ms_out = nullptr) {
    TCPClient client(3000, 5000);
    auto start_t = std::chrono::steady_clock::now();
    if (!client.connect_to(session.coord_address, session.coord_port)) {
        err_out = "Coordinator unreachable at " + session.coord_address + ":" + std::to_string(session.coord_port);
        return false;
    }

    json::Json req = json::Json::object();
    req["type"] = proto::SESSION_AUTH;
    req["client_id"] = session.client_id;
    req["auth_token"] = session.auth_token;

    json::Json resp;
    std::vector<uint8_t> pl;
    if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK) {
        err_out = "handshake failed with Coordinator";
        return false;
    }

    auto lat = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start_t).count();
    if (latency_ms_out) *latency_ms_out = lat;

    if (resp["type"].as_string() == proto::AUTH_ERROR) {
        err_out = resp.contains("reason") ? resp["reason"].as_string() : "authentication rejected";
        return false;
    }
    if (resp["type"].as_string() != proto::SESSION_AUTH_ACK) {
        err_out = "unexpected auth response: " + resp["type"].as_string();
        return false;
    }

    session.session_id = resp["session_id"].as_string();
    session.session_token = resp["session_token"].as_string();
    if (resp.contains("http_port")) {
        session.http_port = static_cast<uint16_t>(resp["http_port"].as_int());
    }
    session.save();
    return true;
}

bool coordinator_request(SessionConfig& session,
                         json::Json req,
                         const std::vector<uint8_t>& req_payload,
                         json::Json& resp,
                         std::vector<uint8_t>& resp_payload,
                         std::string& err_out,
                         int recv_ms = 10000) {
    if (session.session_token.empty()) {
        std::string auth_err;
        if (!authenticate_session(session, auth_err)) {
            err_out = auth_err;
            return false;
        }
    }

    for (int attempt = 0; attempt < 2; ++attempt) {
        TCPClient client(3000, recv_ms);
        if (!client.connect_to(session.coord_address, session.coord_port)) {
            err_out = "Coordinator unreachable at " + session.coord_address + ":" +
                      std::to_string(session.coord_port);
            return false;
        }

        req["session_token"] = session.session_token;
        if (client.request(req, req_payload, resp, resp_payload) != proto::RecvStatus::OK) {
            err_out = "request timed out or connection reset";
            return false;
        }

        if (resp.contains("type") && resp["type"].as_string() == proto::AUTH_ERROR && attempt == 0) {
            std::string auth_err;
            session.session_token.clear();
            if (!authenticate_session(session, auth_err)) {
                err_out = auth_err;
                return false;
            }
            continue;
        }
        return true;
    }
    err_out = "authentication failed";
    return false;
}

void show_usage() {
    std::cout <<
        "==========================================================================\n"
        "  RESILIENT MULTI-PEER FILE DISTRIBUTION & RECOVERY ENGINE (RFE CLI)      \n"
        "==========================================================================\n"
        "Usage: rfe <command> [arguments]\n\n"
        "Commands:\n"
        "  connect [ip:port] [--user <id>] [--token <tok>]\n"
        "                                 Authenticate session with Coordinator (default 127.0.0.1:5000)\n"
        "  upload <file-path> [--defer]   Stream upload; chunk, SHA-256 hash, RS-encode & distribute\n"
        "                                 (--defer stages backup only until first download)\n"
        "  files                          List all files in the Coordinator catalog\n"
        "  search <filename>              Search catalog by filename substring\n"
        "  info <file-id>                 Display detailed file manifest & shard health map\n"
        "  download <file-id> [--source central]\n"
        "                                 Download shards directly from peers & RS-reconstruct\n"
        "  peers                          Display live peer registry, health and workloads\n"
        "  status                         Display cluster health, storage, and catalog summary\n"
        "  transfers                      Display live active transfer statistics\n"
        "  repair <file-id>               Run 9-step durability repair on Coordinator\n"
        "  events [count]                 Show recent cluster recovery & integrity audit events\n"
        "==========================================================================\n";
}

}  // namespace

int main(int argc, char** argv) {
    const std::vector<std::string> args = proc::command_line_args(argc, argv);
    if (args.size() < 2) {
        show_usage();
        return 1;
    }

    const std::string cmd = args[1];
    if (cmd == "help" || cmd == "--help" || cmd == "-h") {
        show_usage();
        return 0;
    }

    if (!sock_init()) {
        std::cerr << "rfe error: socket subsystem initialization failed\n";
        return 1;
    }

    std::error_code ec;
    const fs::path orig_cwd = fs::current_path(ec);
    std::string root, err;
    proc::enter_project_root(root, err);

    SessionConfig session = SessionConfig::load();

    // 1. rfe connect [ip:port] [--user <name>] [--token <token>]
    if (cmd == "connect") {
        for (size_t i = 2; i < args.size(); ++i) {
            if (args[i] == "--user" && i + 1 < args.size()) {
                session.client_id = args[++i];
            } else if (args[i] == "--token" && i + 1 < args.size()) {
                session.auth_token = args[++i];
            } else if (args[i].rfind("--", 0) != 0) {
                const std::string& ep = args[i];
                size_t colon = ep.find(':');
                if (colon != std::string::npos) {
                    session.coord_address = ep.substr(0, colon);
                    session.coord_port = static_cast<uint16_t>(std::stoi(ep.substr(colon + 1)));
                } else {
                    session.coord_address = ep;
                }
            }
        }
        std::cout << "[RFE] Authenticating session at " << session.coord_address << ":" << session.coord_port << "...\n";
        long long lat_ms = 0;
        std::string auth_err;
        if (!authenticate_session(session, auth_err, &lat_ms)) {
            std::cerr << "[RFE] Connection/Authentication failed: " << auth_err << "\n";
            return 1;
        }
        std::cout << "[RFE] Connected & authenticated with Coordinator at "
                  << session.coord_address << ":" << session.coord_port
                  << " (latency: " << lat_ms << " ms)\n";
        std::cout << "[RFE] Session ID: " << session.session_id << " (client=" << session.client_id << ")\n";
        if (session.http_port > 0) {
            std::cout << "[RFE] Web Console: http://" << session.coord_address << ":" << session.http_port << "/\n";
        }
        return 0;
    }

    // 2. rfe upload <file-path> [--defer]
    if (cmd == "upload") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe upload <file-path> [--defer]\n";
            return 1;
        }
        fs::path input_path = fs::u8path(args[2]);
        if (input_path.is_relative()) {
            fs::path from_orig = orig_cwd / input_path;
            if (fs::exists(from_orig, ec)) input_path = from_orig;
        }
        bool defer = false;
        for (size_t i = 3; i < args.size(); ++i) {
            if (args[i] == "--defer") defer = true;
        }

        if (!fs::is_regular_file(input_path, ec)) {
            std::cerr << "[RFE] Upload failed: file does not exist or is not a regular file: "
                      << input_path.u8string() << "\n";
            return 1;
        }

        const uint64_t fsize = fs::file_size(input_path, ec);
        std::ifstream in(input_path, std::ios::binary);
        if (!in) {
            std::cerr << "[RFE] Upload failed: cannot read file: " << input_path.u8string() << "\n";
            return 1;
        }

        // Step A: UPLOAD_INIT
        json::Json init_req = json::Json::object();
        init_req["type"] = proto::UPLOAD_INIT;
        init_req["filename"] = input_path.filename().u8string();
        init_req["size"] = static_cast<long long>(fsize);

        json::Json init_resp;
        std::vector<uint8_t> rpl;
        std::string req_err;
        if (!coordinator_request(session, init_req, {}, init_resp, rpl, req_err, 5000) ||
            init_resp["type"].as_string() != proto::UPLOAD_INIT_ACK) {
            std::cerr << "[RFE] Upload initialization failed: "
                      << (init_resp.contains("reason") ? init_resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }
        const std::string upload_id = init_resp["upload_id"].as_string();

        // Step B: Stream parts via UPLOAD_PART (1 MB per slice, well below 16 MB frame limit)
        constexpr size_t SLICE_SIZE = 1024 * 1024;
        std::vector<uint8_t> buf(SLICE_SIZE);
        uint64_t sent_bytes = 0;
        while (in && sent_bytes < fsize) {
            in.read(reinterpret_cast<char*>(buf.data()), static_cast<std::streamsize>(SLICE_SIZE));
            std::streamsize got = in.gcount();
            if (got <= 0) break;

            std::vector<uint8_t> slice(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(got));
            json::Json part_req = json::Json::object();
            part_req["type"] = proto::UPLOAD_PART;
            part_req["upload_id"] = upload_id;

            json::Json part_resp;
            if (!coordinator_request(session, part_req, slice, part_resp, rpl, req_err, 10000) ||
                part_resp["type"].as_string() != proto::UPLOAD_PART_ACK) {
                std::cerr << "[RFE] Upload part failed: "
                          << (part_resp.contains("reason") ? part_resp["reason"].as_string() : req_err) << "\n";
                return 1;
            }
            sent_bytes += static_cast<uint64_t>(got);
        }

        // Step C: UPLOAD_COMPLETE -> triggers Chunk -> SHA-256 -> RS Encode -> Placement
        json::Json fin_req = json::Json::object();
        fin_req["type"] = proto::UPLOAD_COMPLETE;
        fin_req["upload_id"] = upload_id;
        fin_req["defer_distribution"] = defer;

        json::Json fin_resp;
        if (!coordinator_request(session, fin_req, {}, fin_resp, rpl, req_err, 30000) ||
            fin_resp["type"].as_string() != proto::UPLOAD_COMPLETE_ACK) {
            std::cerr << "[RFE] Upload commit failed: "
                      << (fin_resp.contains("reason") ? fin_resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }

        const std::string status_str = fin_resp["status_str"].as_string();
        const uint64_t stripes = fin_resp.contains("stripes_count") ? static_cast<uint64_t>(fin_resp["stripes_count"].as_int()) : 0;
        const uint64_t sh_size = fin_resp.contains("shard_size") ? static_cast<uint64_t>(fin_resp["shard_size"].as_int()) : 0;
        const int rs_k = fin_resp.contains("k") ? static_cast<int>(fin_resp["k"].as_int()) : 2;
        const int rs_m = fin_resp.contains("m") ? static_cast<int>(fin_resp["m"].as_int()) : 1;

        std::cout << "========================== FILE UPLOAD SUCCESS ==========================\n"
                  << "  File ID:      " << fin_resp["file_id"].as_string() << "\n"
                  << "  Filename:     " << fin_resp["filename"].as_string() << "\n"
                  << "  Size:         " << format_bytes(fsize) << " (" << fsize << " bytes)\n"
                  << "  SHA-256:      " << fin_resp["sha256"].as_string() << "\n"
                  << "  Status:       " << status_str << "\n";
        if (stripes > 0) {
            std::cout << "  Coding:       RS(" << (rs_k + rs_m) << "," << rs_k << ")  Stripes: "
                      << stripes << "  Shard Size: " << format_bytes(sh_size) << "\n";
        }
        std::cout << "  Staged Path:  " << fin_resp["central_path"].as_string() << "\n"
                  << "  Upload Time:  " << fin_resp["upload_time"].as_string() << "\n"
                  << "=========================================================================\n";
        if (status_str == "STORED") {
            std::cout << "Note: File staged centrally; RS shard distribution will trigger when peers are online or on download.\n";
        }
        return 0;
    }

    // 3. rfe files
    if (cmd == "files") {
        json::Json req = json::Json::object();
        req["type"] = "CATALOG_LIST";
        json::Json resp;
        std::vector<uint8_t> pl;
        std::string req_err;
        if (!coordinator_request(session, req, {}, resp, pl, req_err, 5000) ||
            !resp.contains("files") || !resp["files"].is_array()) {
            std::cerr << "[RFE] Failed to list files: "
                      << (resp.contains("reason") ? resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }

        std::vector<FileRecord> files;
        for (const auto& item : resp["files"].items()) {
            files.push_back(FileRecord::from_json(item));
        }

        std::cout << "==================================== FILE CATALOGUE ====================================\n"
                  << std::left
                  << std::setw(10) << "FILE ID"
                  << std::setw(28) << "FILENAME"
                  << std::setw(14) << "SIZE"
                  << std::setw(18) << "STATUS"
                  << std::setw(10) << "STRIPES"
                  << "SHA-256\n"
                  << "----------------------------------------------------------------------------------------\n";
        if (files.empty()) {
            std::cout << "  (no files uploaded yet - use 'rfe upload <file-path>')\n";
        } else {
            for (const auto& f : files) {
                std::cout << std::left
                          << std::setw(10) << f.file_id
                          << std::setw(28) << (f.filename.size() > 26 ? f.filename.substr(0, 23) + "..." : f.filename)
                          << std::setw(14) << format_bytes(f.size)
                          << std::setw(18) << file_status_str(f.status)
                          << std::setw(10) << f.stripes_count
                          << (f.sha256.size() >= 16 ? f.sha256.substr(0, 16) + "..." : f.sha256) << "\n";
            }
        }
        std::cout << "========================================================================================\n";
        return 0;
    }

    // 4. rfe search <filename>
    if (cmd == "search") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe search <query>\n";
            return 1;
        }
        const std::string query = args[2];
        json::Json req = json::Json::object();
        req["type"] = "CATALOG_SEARCH";
        req["query"] = query;
        json::Json resp;
        std::vector<uint8_t> pl;
        std::string req_err;
        if (!coordinator_request(session, req, {}, resp, pl, req_err, 5000) ||
            !resp.contains("files") || !resp["files"].is_array()) {
            std::cerr << "[RFE] Search failed: "
                      << (resp.contains("reason") ? resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }

        std::cout << "Search results for: \"" << query << "\":\n";
        const auto& items = resp["files"].items();
        for (const auto& item : items) {
            FileRecord f = FileRecord::from_json(item);
            std::cout << "  [" << f.file_id << "] " << f.filename
                      << " (" << format_bytes(f.size) << ") - " << file_status_str(f.status) << "\n";
        }
        if (items.empty()) std::cout << "  (no matching files found)\n";
        return 0;
    }

    // 5. rfe info <file-id>
    if (cmd == "info") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe info <file-id>\n";
            return 1;
        }
        const std::string fid = args[2];
        json::Json req = json::Json::object();
        req["type"] = "CATALOG_INFO";
        req["file_id"] = fid;
        json::Json resp;
        std::vector<uint8_t> pl;
        std::string req_err;
        if (!coordinator_request(session, req, {}, resp, pl, req_err, 5000) ||
            !resp.contains("manifest")) {
            std::cerr << "[RFE] File lookup failed: "
                      << (resp.contains("reason") ? resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }

        FileRecord rec = FileRecord::from_json(resp["manifest"]);
        std::cout << "============================ FILE MANIFEST INFO ============================\n"
                  << "  File ID:         " << rec.file_id << "\n"
                  << "  Filename:        " << rec.filename << "\n"
                  << "  Original Size:   " << format_bytes(rec.size) << " (" << rec.size << " bytes)\n"
                  << "  Full SHA-256:    " << rec.sha256 << "\n"
                  << "  Status:          " << file_status_str(rec.status) << "\n"
                  << "  Coding Policy:   RS(" << (rec.k + rec.m) << "," << rec.k << ") Systematic Reed-Solomon\n"
                  << "  Data Shard Size: " << format_bytes(rec.shard_size) << "\n"
                  << "  Total Stripes:   " << rec.stripes_count << "\n"
                  << "  Total Shards:    " << rec.shards.size() << "\n"
                  << "  Central Staged:  " << rec.central_path << "\n"
                  << "----------------------------------------------------------------------------\n"
                  << "  SHARDS DISTRIBUTION MAP:\n";
        for (const auto& sh : rec.shards) {
            std::string peer_str;
            for (size_t i = 0; i < sh.peers.size(); ++i) {
                if (i > 0) peer_str += ",";
                peer_str += sh.peers[i];
            }
            if (peer_str.empty()) peer_str = "(none)";
            std::cout << "    [" << sh.stripe_id << "] " << sh.chunk_id
                      << "  Type: " << std::left << std::setw(7) << sh.shard_type
                      << " State: " << std::setw(12) << sh.status
                      << " Size: " << std::setw(9) << sh.size
                      << " Peer: " << std::setw(8) << peer_str
                      << " SHA: " << (sh.sha256.size() >= 12 ? sh.sha256.substr(0, 12) + "..." : sh.sha256) << "\n";
        }
        std::cout << "============================================================================\n";
        return 0;
    }

    // 6. rfe download <file-id> [--source central]
    if (cmd == "download") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe download <file-id> [--source central]\n";
            return 1;
        }
        const std::string fid = args[2];
        DownloadSource src = DownloadSource::PEERS;
        if (args.size() >= 4 && args[3] == "--source" && args.size() >= 5 && args[4] == "central") {
            src = DownloadSource::CENTRAL;
        }

        std::cout << "[RFE] Requesting download for " << fid << " (source: "
                  << (src == DownloadSource::CENTRAL ? "CENTRAL STAGING" : "MULTI-PEER TCP DIRECT") << ")...\n";

        ClientDownload downloader(session.coord_address, session.coord_port,
                                  "output", session.session_token, session.auth_token);
        auto res = downloader.download(fid, src);

        if (!res.success) {
            std::cerr << "[RFE] Download FAILED: " << res.message << "\n";
            return 1;
        }

        std::cout << "=========================== DOWNLOAD COMPLETE ===========================\n"
                  << "  File ID:         " << res.file_id << "\n"
                  << "  Output File:     " << res.output_path << "\n"
                  << "  File Size:       " << format_bytes(res.file_size) << "\n"
                  << "  Stripes:         " << res.stripes_total << "\n"
                  << "  Shards Fetched:  " << res.shards_retrieved << " (Direct peer-to-client)\n";
        if (res.corrupted_shards_detected > 0) {
            std::cout << "  Corrupt Rejected:" << res.corrupted_shards_detected
                      << " (Recovered automatically via parity & reported to Coordinator)\n";
        }
        std::cout << "  Integrity Check: MATCH VERIFIED (" << res.actual_sha256 << ")\n"
                  << "  Message:         " << res.message << "\n"
                  << "=========================================================================\n";
        return 0;
    }

    // 7. rfe peers
    if (cmd == "peers") {
        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_PEERS;
        json::Json resp;
        std::vector<uint8_t> pl;
        std::string req_err;
        if (!coordinator_request(session, req, {}, resp, pl, req_err, 5000) ||
            resp["type"].as_string() != proto::QUERY_PEERS_ACK) {
            std::cerr << "[RFE] Failed to query peers: "
                      << (resp.contains("reason") ? resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }
        PeerRegistry reg;
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& item : resp["peers"].items()) {
                reg.set_peer_snapshot(PeerInfo::from_json(item));
            }
        }
        std::cout << reg.format_table();
        return 0;
    }

    // 8. rfe status
    if (cmd == "status") {
        json::Json preq = json::Json::object();
        preq["type"] = proto::QUERY_PEERS;
        json::Json presp;
        std::vector<uint8_t> ppl;
        std::string req_err;
        if (!coordinator_request(session, preq, {}, presp, ppl, req_err, 5000)) {
            std::cerr << "[RFE] Cluster unreachable: " << req_err << "\n";
            return 1;
        }

        size_t online = 0, busy = 0, suspect = 0, offline = 0;
        uint64_t total_storage = 0;
        if (presp.contains("peers") && presp["peers"].is_array()) {
            for (const auto& item : presp["peers"].items()) {
                PeerInfo pi = PeerInfo::from_json(item);
                if (pi.state == PeerHealth::ONLINE) {
                    ++online;
                    if (pi.workload.is_busy()) ++busy;
                    total_storage += pi.workload.available_storage_bytes;
                } else if (pi.state == PeerHealth::SUSPECT) {
                    ++suspect;
                } else {
                    ++offline;
                }
            }
        }

        json::Json freq = json::Json::object();
        freq["type"] = "CATALOG_LIST";
        json::Json fresp;
        std::vector<uint8_t> fpl;
        size_t total_files = 0, avail_files = 0, degraded_files = 0, pending_files = 0;
        if (coordinator_request(session, freq, {}, fresp, fpl, req_err, 5000) &&
            fresp.contains("files") && fresp["files"].is_array()) {
            total_files = fresp["files"].items().size();
            for (const auto& item : fresp["files"].items()) {
                FileRecord f = FileRecord::from_json(item);
                if (f.status == FileDistributionStatus::AVAILABLE) ++avail_files;
                else if (f.status == FileDistributionStatus::DEGRADED) ++degraded_files;
                else if (f.status == FileDistributionStatus::RECOVERY_PENDING) ++pending_files;
            }
        }

        const int rs_k = presp.contains("rs_k") ? static_cast<int>(presp["rs_k"].as_int()) : 2;
        const int rs_m = presp.contains("rs_m") ? static_cast<int>(presp["rs_m"].as_int()) : 1;
        const int active_sess = presp.contains("active_sessions") ? static_cast<int>(presp["active_sessions"].as_int()) : 0;

        std::cout << "============================ CLUSTER STATUS ============================\n"
                  << "  Coordinator:       ONLINE (" << session.coord_address << ":" << session.coord_port << ")\n";
        if (session.http_port > 0) {
            std::cout << "  Web Console / API: http://" << session.coord_address << ":" << session.http_port << "/\n";
        }
        std::cout << "  Cluster Health:    " << ((offline > 0 || pending_files > 0) ? "DEGRADED" : "HEALTHY") << "\n"
                  << "  Coding Policy:     RS(" << (rs_k + rs_m) << "," << rs_k << ")\n"
                  << "  Peers:             " << (online + suspect + offline)
                  << " total (" << online << " ONLINE [" << busy << " BUSY], "
                  << suspect << " SUSPECT, " << offline << " OFFLINE)\n"
                  << "  Available Storage: " << format_bytes(total_storage) << "\n"
                  << "  Active Sessions:   " << active_sess << "\n"
                  << "  Tracked Files:     " << total_files
                  << " (" << avail_files << " AVAILABLE, " << degraded_files
                  << " DEGRADED, " << pending_files << " RECOVERY_PENDING)\n"
                  << "========================================================================\n";
        return 0;
    }

    // 9. rfe transfers
    if (cmd == "transfers") {
        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_PEERS;
        json::Json resp;
        std::vector<uint8_t> pl;
        std::string req_err;
        if (!coordinator_request(session, req, {}, resp, pl, req_err, 5000)) {
            std::cerr << "[RFE] Coordinator unreachable: " << req_err << "\n";
            return 1;
        }

        std::cout << "======================== ACTIVE WORKLOAD & TRANSFERS ========================\n"
                  << std::left
                  << std::setw(10) << "PEER ID"
                  << std::setw(22) << "ENDPOINT"
                  << std::setw(14) << "HEALTH"
                  << std::setw(18) << "ACTIVE / CAPACITY"
                  << "BUSY STATE\n"
                  << "-----------------------------------------------------------------------------\n";
        if (resp.contains("peers") && resp["peers"].is_array()) {
            for (const auto& item : resp["peers"].items()) {
                PeerInfo pi = PeerInfo::from_json(item);
                std::string ep = pi.address + ":" + std::to_string(pi.port);
                std::string trans = std::to_string(pi.workload.active_transfers) + " / " +
                                    std::to_string(pi.workload.transfer_capacity);
                std::string h = (pi.state == PeerHealth::ONLINE && pi.workload.is_busy())
                    ? "BUSY"
                    : peer_health_str(pi.state);
                std::cout << std::left
                          << std::setw(10) << pi.id
                          << std::setw(22) << ep
                          << std::setw(14) << h
                          << std::setw(18) << trans
                          << (pi.workload.is_busy() ? "BUSY (Capacity saturated)" : "IDLE / ACCEPTING") << "\n";
            }
        }
        std::cout << "=============================================================================\n";
        return 0;
    }

    // 10. rfe repair <file-id>
    if (cmd == "repair") {
        if (args.size() < 3) {
            std::cerr << "usage: rfe repair <file-id>\n";
            return 1;
        }
        const std::string fid = args[2];
        std::cout << "[RFE] Requesting durability repair from Coordinator for " << fid << "...\n";
        json::Json req = json::Json::object();
        req["type"] = "REPAIR_FILE";
        req["file_id"] = fid;
        json::Json resp;
        std::vector<uint8_t> pl;
        std::string req_err;
        if (!coordinator_request(session, req, {}, resp, pl, req_err, 30000) ||
            resp["type"].as_string() != "REPAIR_FILE_ACK") {
            std::cerr << "[RFE] Repair failed: "
                      << (resp.contains("reason") ? resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }
        std::cout << "Repair result: " << resp["status"].as_string() << "\n"
                  << resp["report"].as_string();
        if (!resp["report"].as_string().empty() && resp["report"].as_string().back() != '\n') {
            std::cout << "\n";
        }
        return 0;
    }

    // 11. rfe events [count]
    if (cmd == "events") {
        size_t limit = 25;
        if (args.size() >= 3) {
            try { limit = static_cast<size_t>(std::stoul(args[2])); } catch (...) {}
        }
        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_EVENTS;
        json::Json resp;
        std::vector<uint8_t> pl;
        std::string req_err;
        if (!coordinator_request(session, req, {}, resp, pl, req_err, 5000) ||
            !resp.contains("events") || !resp["events"].is_array()) {
            std::cerr << "[RFE] Failed to query audit events: "
                      << (resp.contains("reason") ? resp["reason"].as_string() : req_err) << "\n";
            return 1;
        }
        const auto& items = resp["events"].items();
        size_t start = items.size() > limit ? items.size() - limit : 0;
        std::cout << "========================== CLUSTER AUDIT EVENTS ==========================\n";
        for (size_t i = start; i < items.size(); ++i) {
            const auto& ev = items[i];
            std::cout << "  [" << ev["timestamp"].as_string() << "] "
                      << std::left << std::setw(22) << ev["category"].as_string()
                      << " " << ev["message"].as_string() << "\n";
        }
        if (items.empty()) std::cout << "  (no events recorded yet)\n";
        std::cout << "==========================================================================\n";
        return 0;
    }

    show_usage();
    return 1;
}
