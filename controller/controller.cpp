// ============================================================================
// controller.cpp - Controller: orchestrates the six engines (spec §14)
//
// The Controller composes E1..E6 without absorbing their responsibilities:
//   E1 ChunkManager     - chunking, SHA-256, chunk metadata
//   E2 TCPClient        - request/response communication with peers
//   E3 Distributor      - round-robin placement + replication
//   E4 FailureDetector  - heartbeat-based failure detection
//   E5 RecoveryManager  - recovery + reconstruction
//   E6 MetadataManager  - metadata + monitoring
//
// CLI:
//   controller sys <start|stop|status>   start / stop / list the 3 peer processes
//   controller fail <A|B|C|random>       terminate one peer (simulated failure)
//   controller demo [file]               full Gate-1 demonstration
//   controller test [T01 ...]            run the Gate-1 tests T01..T07
//   controller chunk <file>
//   controller distribute
//   controller ping
//   controller detect [--all] [--duration-ms N]
//   controller recover [--failed <id>] [--simulate-interrupt]
//   controller reconstruct [original_file]
//   controller status
//   controller reset
//
// The controller finds the project root (the directory holding config.json)
// by itself, so it can be started from any working directory.
// ============================================================================
#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <iterator>
#include <mutex>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <random>
#include <string>
#include <sstream>
#include <thread>
#include <vector>

#include "../engine/chunking/chunk_manager.hpp"
#include "../engine/coding/adaptive_chunker.hpp"
#include "../engine/coding/reed_solomon.hpp"
#include "../engine/communication/protocol.hpp"
#include "../engine/communication/socket_compat.hpp"
#include "../engine/communication/tcp_client.hpp"
#include "../engine/coordinator/coordinator_service.hpp"
#include "../engine/distribution/distributor.hpp"
#include "../engine/failure_detection/failure_detector.hpp"
#include "../engine/metadata/metadata_manager.hpp"
#include "../engine/process/process_manager.hpp"
#include "../engine/recovery/client_download.hpp"
#include "../engine/recovery/recovery_manager.hpp"
#include "../engine/registry/peer_registry.hpp"
#include "../models/peer.hpp"
#include "../third_party/mini_json.hpp"

namespace fs = std::filesystem;

class Controller {
public:
    Controller() { load_config(); }

    // ---- configuration -------------------------------------------------------
    void load_config() {
        std::ifstream in("config.json");
        if (!in) in.open("config/config.json");
        if (!in) throw std::runtime_error("config.json not found - run from the project root");
        std::stringstream ss;
        ss << in.rdbuf();
        config_ = json::Json::parse(ss.str());

        chunk_size_ = static_cast<uint64_t>(config_["chunk_size"].as_int());
        hb_interval_ = static_cast<int>(config_["heartbeat_interval_ms"].as_int());
        hb_timeout_ = static_cast<int>(config_["heartbeat_timeout_ms"].as_int());
        hb_threshold_ = static_cast<int>(config_["failure_threshold"].as_int());

        for (const auto& p : config_["peers"].items()) {
            PeerInfo pi;
            pi.id = p["id"].as_string();
            pi.address = p["address"].as_string();
            pi.port = static_cast<uint16_t>(p["port"].as_int());
            peers_.push_back(pi);
        }

        if (config_.contains("coordinator")) {
            coord_bind_address_ = config_["coordinator"]["address"].as_string();
            coord_port_ = static_cast<uint16_t>(config_["coordinator"]["port"].as_int());
        }
        if (config_.contains("rs_k")) rs_k_ = static_cast<uint32_t>(config_["rs_k"].as_int());
        if (config_.contains("rs_m")) rs_m_ = static_cast<uint32_t>(config_["rs_m"].as_int());
        if (config_.contains("http_port")) http_port_ = static_cast<uint16_t>(config_["http_port"].as_int());
        if (config_.contains("auth_token")) auth_token_ = config_["auth_token"].as_string();
        if (config_.contains("auto_recover") && config_["auto_recover"].is_bool())
            auto_recover_ = config_["auto_recover"].as_bool();
        if (config_.contains("require_auth") && config_["require_auth"].is_bool())
            require_auth_ = config_["require_auth"].as_bool();
        if (config_.contains("adaptive_replicas") && config_["adaptive_replicas"].is_bool())
            adaptive_replicas_ = config_["adaptive_replicas"].as_bool();
        if (config_.contains("hot_replica_count"))
            hot_replica_count_ = static_cast<size_t>(config_["hot_replica_count"].as_int());
    }

    // ---- Gate-2: Coordinator Service & Peer Registry commands -----------------
    int cmd_coordinator(uint16_t port = 0, const std::string& bind_addr = "", int http_port_arg = -1) {
        if (port == 0) port = coord_port_;
        std::string addr = bind_addr.empty() ? coord_bind_address_ : bind_addr;
        uint16_t actual_http = (http_port_arg >= 0) ? static_cast<uint16_t>(http_port_arg) : http_port_;

        CoordinatorService coordinator(addr, port, hb_interval_, hb_timeout_, hb_threshold_,
                                       rs_k_, rs_m_, actual_http, auth_token_,
                                       auto_recover_, require_auth_,
                                       adaptive_replicas_, hot_replica_count_);
        if (!coordinator.start()) return 1;

        std::cout << "[Coordinator] Service listening. Press Ctrl+C to terminate.\n";
        proc::install_shutdown_handler([&coordinator]() {
            coordinator.stop();
        });
        coordinator.run();
        proc::remove_shutdown_handler();
        return 0;
    }

    int cmd_peers(const std::string& coord_addr = "", uint16_t coord_port = 0) {
        std::string addr = coord_addr.empty() ? (coord_bind_address_ == "0.0.0.0" ? "127.0.0.1" : coord_bind_address_) : coord_addr;
        uint16_t port = (coord_port == 0) ? coord_port_ : coord_port;

        TCPClient client(1500, 1500);
        if (!client.connect_to(addr, port)) {
            std::cerr << "controller error: cannot connect to Coordinator at " << addr << ":" << port << "\n";
            return 1;
        }

        json::Json req = json::Json::object();
        req["type"] = proto::QUERY_PEERS;
        if (!auth_token_.empty()) req["auth_token"] = auth_token_;
        json::Json resp;
        std::vector<uint8_t> pl;
        if (client.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
            resp["type"].as_string() != proto::QUERY_PEERS_ACK) {
            std::cerr << "controller error: failed to query peers from Coordinator\n";
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

    // ---- E1: chunk ------------------------------------------------------------
    int cmd_chunk(const std::string& file) {
        std::error_code fec;
        const fs::path file_path = fs::u8path(file);  // UTF-8 -> native path
        if (!fs::is_regular_file(file_path, fec)) {
            std::cerr << "input file not found: " << file << "\n";
            return 1;
        }
        const uint64_t size = fs::file_size(file_path);
        const std::string fid = meta_.next_file_id();

        ChunkManager cm(chunk_size_);
        auto chunks = cm.chunk_file(fid, file, staging_dir());

        meta_.init_file_metadata(fid, file_path.filename().u8string(),
                                 size, chunk_size_, chunks);
        // Remember where the original lives, for the final byte-comparison.
        {
            json::Json d = json::Json::object();
            d["file_id"] = fid;
            d["source_path"] = fs::absolute(file_path).u8string();
            meta_.log_event("FILE_CHUNKED", "file split into fixed-size chunks", d);
        }
        // Store source path in metadata for reconstruct's default.
        {
            // cheap approach: separate sidecar, keeps file_metadata.json schema clean
            std::ofstream out("metadata/source_path.txt", std::ios::trunc);
            out << fs::absolute(file_path).u8string();
        }

        std::cout << "[E1] file=" << file << " id=" << fid << " size=" << size
                  << " chunks=" << chunks.size() << " chunk_size=" << chunk_size_
                  << "\n";
        for (const auto& c : chunks)
            std::cout << "  " << c.chunk_id << " seq=" << c.sequence
                      << " size=" << c.size << " sha256=" << c.sha256.substr(0, 16)
                      << "...\n";
        return 0;
    }

    // ---- E3: distribute ---------------------------------------------------------
    int cmd_distribute() {
        if (!meta_.has_file_metadata()) {
            std::cerr << "no file metadata - run 'chunk' first\n";
            return 1;
        }
        auto chunks = meta_.chunks();
        Distributor dist(peers_);
        std::string err;
        if (!dist.distribute(staging_dir(), chunks, err)) {
            std::cerr << "[E3] distribution failed: " << err << "\n";
            meta_.log_event("DISTRIBUTION_FAILED", err);
            return 1;
        }
        for (const auto& c : chunks) meta_.set_chunk_peers(c.chunk_id, c.peers);

        std::cout << "[E3] round-robin placement, replication factor 2:\n";
        for (const auto& c : chunks)
            std::cout << "  " << c.chunk_id << " -> " << c.peers[0] << " + "
                      << c.peers[1] << "\n";
        json::Json d = json::Json::object();
        d["total_chunks"] = static_cast<long long>(chunks.size());
        d["replication_factor"] = 2;
        meta_.log_event("DISTRIBUTION_DONE",
                        "chunks distributed round-robin with replication", d);
        return 0;
    }

    // ---- E2: REGISTER handshake ----------------------------------------------------
    bool ping_peer(const PeerInfo& p, int timeout_ms = 1000) {
        TCPClient client(timeout_ms, timeout_ms);
        if (!client.connect_to(p.address, p.port)) return false;
        json::Json req = json::Json::object();
        req["type"] = proto::REGISTER;
        json::Json resp;
        std::vector<uint8_t> pl;
        return client.request(req, {}, resp, pl) == proto::RecvStatus::OK &&
               resp["type"].as_string() == proto::REGISTER_ACK;
    }

    int cmd_ping() {
        bool all_ok = true;
        for (const auto& p : peers_) {
            const bool ok = ping_peer(p);
            std::cout << "  peer " << p.id << " (" << p.address << ":" << p.port
                      << ") " << (ok ? "ONLINE" : "UNREACHABLE") << "\n";
            all_ok &= ok;
        }
        return all_ok ? 0 : 1;
    }

    // ---- E4: heartbeat failure detection --------------------------------------------
    // duration_ms > 0 bounds the monitoring time (used with --all).
    int cmd_detect(bool stop_on_first, long long duration_ms = 0) {
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        std::cout << "[E4] heartbeat monitor: interval=" << hb_interval_
                  << "ms timeout=" << hb_timeout_
                  << "ms threshold=" << hb_threshold_ << " missed\n";

        for (const auto& p : fd.probe_once())
            meta_.set_peer_status(p.id, p.state);

        std::mutex mu;
        std::string failed;
        long long detection_ms = 0;
        std::atomic<bool> finished{false};
        std::thread timer;
        if (duration_ms > 0) {
            timer = std::thread([&]() {
                const auto until = std::chrono::steady_clock::now() +
                                   std::chrono::milliseconds(duration_ms);
                while (!finished && std::chrono::steady_clock::now() < until)
                    std::this_thread::sleep_for(std::chrono::milliseconds(20));
                fd.stop();
            });
        }
        fd.run([&](const PeerInfo& peer, long long ms) {
            {
                std::lock_guard<std::mutex> lk(mu);
                if (failed.empty()) {
                    failed = peer.id;
                    detection_ms = ms;
                }
            }
            meta_.set_peer_status(peer.id, PeerState::OFFLINE);
            json::Json d = json::Json::object();
            d["peer"] = peer.id;
            d["detection_ms"] = ms;
            meta_.log_event("PEER_FAILURE_DETECTED",
                            "heartbeat threshold reached; peer marked OFFLINE", d);
            std::lock_guard<std::mutex> lk(mu);
            std::cout << "[E4] peer " << peer.id << " OFFLINE (detection time "
                      << ms << " ms, measured)\n";
        }, stop_on_first);
        finished = true;
        if (timer.joinable()) timer.join();

        if (!failed.empty()) {
            std::cout << failed << " " << detection_ms << "\n";  // machine-readable
            return 0;
        }
        return 2;  // no failure observed
    }

    // ---- failure simulation: terminate one peer process ------------------------------
    // Accepts a configured peer id (case-insensitive); returns the canonical id.
    bool resolve_peer_id(const std::string& wanted, std::string& id_out) const {
        for (const auto& p : peers_) {
            if (p.id.size() == wanted.size() &&
                std::equal(p.id.begin(), p.id.end(), wanted.begin(),
                           [](char x, char y) {
                               return std::toupper(static_cast<unsigned char>(x)) ==
                                      std::toupper(static_cast<unsigned char>(y));
                           })) {
                id_out = p.id;
                return true;
            }
        }
        return false;
    }

    bool kill_peer_process(const std::string& target, long& pid_out) {
        const long pid = proc::read_pid_file(proc::pid_file_for(target));
        if (pid <= 0) {
            std::cerr << "no pid file for peer " << target
                      << " (is the system running? try: controller sys start)\n";
            return false;
        }
        if (!proc::peer_running(pid)) {
            std::cerr << "peer " << target << " (pid " << pid
                      << ") is not running\n";
            return false;
        }
        std::string err;
        if (!proc::terminate_peer(pid, err)) {
            std::cerr << "failed to terminate peer " << target << ": " << err << "\n";
            return false;
        }
        pid_out = pid;
        return true;
    }

    int cmd_fail(const std::string& which) {
        std::string target;
        if (which == "random") {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> pick(0, peers_.size() - 1);
            target = peers_[pick(gen)].id;
        } else if (!resolve_peer_id(which, target)) {
            std::cerr << "unknown peer '" << which << "' (expected";
            for (const auto& p : peers_) std::cerr << " " << p.id;
            std::cerr << " or random)\n";
            return 1;
        }
        long pid = 0;
        if (!kill_peer_process(target, pid)) return 1;
        json::Json d = json::Json::object();
        d["peer"] = target;
        d["pid"] = pid;
        meta_.log_event("PEER_KILLED", "peer process terminated (simulated failure)", d);
        std::cout << "[demo] peer " << target << " (pid " << pid
                  << ") terminated - failure simulated\n";
        std::cout << target << "\n";
        return 0;
    }

    // ---- system process control (native; replaces the run_system script) ------------
    // Starts every peer that is not already running, then waits until all of
    // them answer the REGISTER handshake.
    int cmd_sys_start() {
        const std::string exe = proc::find_peer_exe();
        if (exe.empty()) {
            std::cerr << "peer executable not found next to the controller - build first:\n"
                      << "  cmake -S . -B build && cmake --build build --config Release\n";
            return 1;
        }
        std::error_code ec;
        fs::create_directories("run", ec);

        int rc = 0;
        std::vector<const PeerInfo*> waiting;
        for (const auto& p : peers_) {
            const long old_pid = proc::read_pid_file(proc::pid_file_for(p.id));
            if (old_pid > 0 && proc::peer_running(old_pid)) {
                std::cout << "peer " << p.id << " already running (pid " << old_pid << ")\n";
                waiting.push_back(&p);
                continue;
            }
            long pid = 0;
            std::string err;
            if (!proc::start_peer(exe, p.id, p.address, p.port, pid, err)) {
                std::cerr << "peer " << p.id << " failed to start: " << err << "\n";
                rc = 1;
                continue;
            }
            std::cout << "peer " << p.id << " started on " << p.address << ":"
                      << p.port << " (pid " << pid << ")\n";
            waiting.push_back(&p);
        }

        // Readiness: process alive AND REGISTER_ACK received (max ~10 s).
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!waiting.empty() && std::chrono::steady_clock::now() < deadline) {
            for (auto it = waiting.begin(); it != waiting.end();) {
                const long pid = proc::read_pid_file(proc::pid_file_for((*it)->id));
                if (!proc::peer_running(pid)) {
                    std::cerr << "peer " << (*it)->id << " exited during start-up - see "
                              << proc::log_file_for((*it)->id) << "\n";
                    print_log_tail((*it)->id);
                    rc = 1;
                    it = waiting.erase(it);
                } else if (ping_peer(**it, 300)) {
                    it = waiting.erase(it);
                } else {
                    ++it;
                }
            }
            if (!waiting.empty())
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        for (const PeerInfo* p : waiting) {
            std::cerr << "peer " << p->id << " did not become ready in time - see "
                      << proc::log_file_for(p->id) << "\n";
            print_log_tail(p->id);
            rc = 1;
        }
        if (rc == 0) std::cout << "all peers started and answering\n";
        return rc;
    }

    void print_log_tail(const std::string& id) {
        std::ifstream in(proc::log_file_for(id));
        std::string line;
        int n = 0;
        while (std::getline(in, line) && n < 20) { std::cerr << "    | " << line << "\n"; ++n; }
    }

    int cmd_sys_stop() {
        int rc = 0;
        for (const auto& p : peers_) {
            const std::string pidfile = proc::pid_file_for(p.id);
            const long pid = proc::read_pid_file(pidfile);
            if (pid <= 0) {
                std::cout << "peer " << p.id << " not running (no pid file)\n";
            } else if (!proc::peer_running(pid)) {
                std::cout << "peer " << p.id << " (pid " << pid
                          << ") not running (stale pid file removed)\n";
            } else {
                std::string err;
                if (proc::stop_peer(pid, err)) {
                    std::cout << "peer " << p.id << " (pid " << pid << ") stopped\n";
                } else {
                    std::cerr << "peer " << p.id << ": " << err << "\n";
                    rc = 1;
                    continue;  // keep the pid file: it may still be running
                }
            }
            proc::remove_pid_file(pidfile);
        }
        return rc;
    }

    // Returns 0 only if all peers are RUNNING.
    int cmd_sys_status() {
        int rc = 0;
        for (const auto& p : peers_) {
            const long pid = proc::read_pid_file(proc::pid_file_for(p.id));
            const bool alive = pid > 0 && proc::peer_running(pid);
            const char* state = "STOPPED";
            if (alive) state = ping_peer(p, 500) ? "RUNNING" : "UNRESPONSIVE";
            std::cout << "  peer " << p.id << " " << state;
            if (alive) std::cout << " (pid " << pid << ") " << p.address << ":" << p.port;
            std::cout << "\n";
            if (std::string(state) != "RUNNING") rc = 1;
        }
        return rc;
    }

    // ---- E5: recovery -------------------------------------------------------------------
    int cmd_recover(const std::string& failed_arg, bool simulate_interrupt) {
        if (!meta_.has_file_metadata()) {
            std::cerr << "no file metadata\n";
            return 1;
        }
        // Determine which peers are offline by probing (heartbeats, not TCP
        // liveness assumptions).
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        std::vector<std::string> offline;
        for (const auto& p : fd.probe_once()) {
            meta_.set_peer_status(p.id, p.state);
            if (p.state == PeerState::OFFLINE) offline.push_back(p.id);
        }
        std::string failed = failed_arg;
        if (failed.empty()) {
            if (offline.empty()) {
                std::cerr << "no failed peer detected\n";
                return 1;
            }
            failed = offline.front();
        }

        RecoveryManager rm(peers_, meta_, output_dir());
        RecoveryResult r = rm.recover(failed, offline, simulate_interrupt);

        std::cout << "[E5] failed peer: " << failed << "\n";
        std::cout << "  affected chunks (" << r.affected_chunks.size() << "):";
        for (const auto& a : r.affected_chunks) std::cout << " " << a;
        std::cout << "\n  recovered (" << r.recovered_chunks.size() << "):";
        for (const auto& c : r.recovered_chunks) std::cout << " " << c;
        std::cout << "\n  retries=" << r.retry_count
                  << " integrity_failures=" << r.integrity_failures
                  << " bytes_transferred=" << r.bytes_transferred << "\n";
        if (!r.unrecoverable_chunks.empty()) {
            std::cout << "  UNRECOVERABLE:";
            for (const auto& u : r.unrecoverable_chunks) std::cout << " " << u;
            std::cout << "\n";
        }
        return r.success ? 0 : 3;
    }

    // ---- E5: reconstruction + final comparison ---------------------------------------------
    int cmd_reconstruct(const std::string& original) {
        std::string src = original;
        if (src.empty()) {
            std::ifstream in("metadata/source_path.txt");
            if (!in || !std::getline(in, src)) {
                std::cerr << "original file path unknown - pass it explicitly\n";
                return 1;
            }
        }
        RecoveryManager rm(peers_, meta_, output_dir());
        std::string recovered, err;
        if (!rm.reconstruct(src, recovered, err)) {
            std::cerr << "[E5] reconstruction failed: " << err << "\n";
            return 1;
        }
        std::cout << "[E5] reconstruction OK: " << recovered
                  << " is byte-identical to the original\n";
        return 0;
    }

    // ---- status ----------------------------------------------------------------------------
    int cmd_status() {
        FailureDetector fd(peers_, hb_interval_, hb_timeout_, hb_threshold_);
        for (const auto& p : fd.probe_once()) {
            meta_.set_peer_status(p.id, p.state);
            std::cout << "  peer " << p.id << " " << peer_state_str(p.state) << "\n";
        }
        return 0;
    }

    // ---- reset --------------------------------------------------------------------------------
    int cmd_reset() {
        meta_.reset();
        std::error_code ec;
        for (const auto& p : peers_)
            for (const auto& e : fs::directory_iterator(p.storage_dir(), ec))
                fs::remove(e.path(), ec);
        // Clean dynamic peer & central directories (Gate-2 + tmp uploads + catalog)
        for (const auto& e : fs::directory_iterator("storage", ec)) {
            fs::remove_all(e.path(), ec);
        }
        fs::remove_all("metadata/files", ec);
        fs::remove("metadata/catalog.json", ec);
        fs::remove("metadata/file_counter.txt", ec);
        fs::remove("metadata/sessions.json", ec);
        fs::remove("metadata/session.json", ec);
        fs::remove("metadata/source_path.txt", ec);
        fs::remove_all(output_dir(), ec);
        fs::create_directories(output_dir());
        std::cout << "reset done (metadata, peer storage, output cleared)\n";
        return 0;
    }

    // ---- full Gate-1 demonstration (spec §23) --------------------------------------------------
    int cmd_demo(const std::string& file_arg) {
        const std::string file = file_arg.empty() ? "input/sample_file.bin" : file_arg;
        auto step = [](const std::string& s) {
            std::cout << "\n==== " << s << " ====\n";
        };
        step("0. start / verify the 3 peer processes");
        if (cmd_sys_start() != 0) return 1;

        step("1. chunk file (E1)");
        if (cmd_chunk(file) != 0) return 1;

        step("2. distribute + replicate (E3)");
        if (cmd_distribute() != 0) return 1;

        step("3. verify all peers ONLINE (E2 REGISTER)");
        if (cmd_ping() != 0) return 1;

        step("4. simulate random single-peer failure");
        // inline to capture the randomly selected peer id
        std::string failed;
        {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<size_t> pick(0, peers_.size() - 1);
            failed = peers_[pick(gen)].id;
            long pid = 0;
            if (!kill_peer_process(failed, pid)) return 1;
            json::Json d = json::Json::object();
            d["peer"] = failed;
            d["pid"] = pid;
            meta_.log_event("PEER_KILLED",
                            "peer process terminated (simulated failure)", d);
            std::cout << "[demo] randomly selected peer: " << failed
                      << " (pid " << pid << ") - terminated\n";
        }

        step("5. heartbeat failure detection (E4)");
        if (cmd_detect(true) != 0) {
            std::cerr << "failure was not detected\n";
            return 1;
        }

        step("6. recover affected chunks from surviving replicas (E5)");
        int rc = cmd_recover(failed, false);
        if (rc != 0) return rc;

        step("7. reconstruct + compare with original (E5)");
        if (cmd_reconstruct("") != 0) return 1;

        std::cout << "\n==== DEMO SUCCESS: peer " << failed
                  << " failed, all chunks recovered, file reconstructed "
                     "byte-identical ====\n";
        std::cout << "(peers are still running; use 'controller sys stop' to stop them)\n";
        return 0;
    }

    // ---- Gate-1 tests T01..T07 (spec §12) ------------------------------------------------
    // In-process equivalent of tests/run_tests.sh: identical scenarios, but it
    // drives the very same code paths (sys start/stop, fail, detect, recover,
    // reconstruct) without needing bash or PowerShell, so it behaves the same
    // on Windows and Linux.
    int cmd_test(const std::vector<std::string>& only) {
        struct Capture {
            std::ostringstream buf;
            std::streambuf* o;
            std::streambuf* e;
            Capture() : o(std::cout.rdbuf(buf.rdbuf())), e(std::cerr.rdbuf(buf.rdbuf())) {}
            ~Capture() { std::cout.rdbuf(o); std::cerr.rdbuf(e); }
        };
        auto quiet = [](const std::function<int()>& f, std::string* out = nullptr) {
            Capture c;
            const int rc = f();
            if (out) *out = c.buf.str();
            return rc;
        };
        auto slurp = [](const std::string& path) {
            std::ifstream in(fs::u8path(path), std::ios::binary);
            return std::string((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
        };
        auto identical = [&](const std::string& a, const std::string& b) {
            std::error_code ec;
            return fs::is_regular_file(fs::u8path(b), ec) && slurp(a) == slurp(b);
        };
        auto events_contain = [&](const std::string& needle) {
            return slurp("metadata/events.json").find(needle) != std::string::npos;
        };
        const std::string in_file = "input/test_input.bin";
        const std::string out_file = "output/recovered_test_input.bin";

        auto fresh = [&]() {
            quiet([&] { return cmd_sys_stop(); });
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            quiet([&] { return cmd_reset(); });
            if (quiet([&] { return cmd_sys_start(); }) != 0) return false;
            std::error_code ec;
            fs::create_directories("input", ec);
            std::vector<char> data(300000);
            std::random_device rd;
            std::mt19937_64 gen(rd());
            for (auto& byte : data) byte = static_cast<char>(gen() & 0xFF);
            std::ofstream out(in_file, std::ios::binary | std::ios::trunc);
            out.write(data.data(), static_cast<std::streamsize>(data.size()));
            return static_cast<bool>(out);
        };
        auto chunk_and_distribute = [&]() {
            return quiet([&] { return cmd_chunk(in_file); }) == 0 &&
                   quiet([&] { return cmd_distribute(); }) == 0;
        };
        auto fail = [&](const std::string& id) {
            return quiet([&] { return cmd_fail(id); }) == 0;
        };
        auto count_chunks = [&](const std::string& peer, const std::string& fid) {
            int n = 0;
            std::error_code ec;
            for (const auto& e : fs::directory_iterator("storage/peer_" + peer + "/chunks", ec))
                if (e.path().filename().string().rfind(fid, 0) == 0 &&
                    e.path().extension() == ".chunk") ++n;
            return n;
        };

        using Result = std::pair<bool, std::string>;
        auto t01 = [&]() -> Result {
            if (!fresh()) return {false, "could not start peers"};
            if (quiet([&] { return cmd_chunk(in_file); }) != 0) return {false, "chunking failed"};
            if (quiet([&] { return cmd_distribute(); }) != 0) return {false, "distribution failed"};
            const std::string fid = meta_.file_id();
            const int a = count_chunks("A", fid), b = count_chunks("B", fid), c = count_chunks("C", fid);
            if (a == 3 && b == 4 && c == 3)
                return {true, "round-robin placement A=3,B=4,C=3, replication factor 2"};
            return {false, "unexpected placement A=" + std::to_string(a) + ",B=" +
                               std::to_string(b) + ",C=" + std::to_string(c)};
        };
        auto t02 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("B")) return {false, "could not fail peer B"};
            quiet([&] { return cmd_detect(true); });
            if (quiet([&] { return cmd_recover("B", false); }) != 0) return {false, "recovery failed"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "single peer failure: detected, recovered, byte-identical"};
            return {false, "reconstruction mismatch after peer B failure"};
        };
        auto t03 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("C")) return {false, "could not fail peer C"};
            quiet([&] { return cmd_detect(true); });
            if (quiet([&] { return cmd_recover("C", true); }) != 0) return {false, "recovery failed"};
            if (!events_contain("TRANSFER_INTERRUPTED")) return {false, "no TRANSFER_INTERRUPTED event logged"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "interrupted transfer discarded, full retry succeeded"};
            return {false, "reconstruction failed after retry"};
        };
        auto t04 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            const std::string path = "storage/peer_A/chunks/" + meta_.file_id() + "-C001.chunk";
            {
                // corrupt peer A's copy of chunk 1; B's copy stays intact
                std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
                if (!f) return {false, "cannot open " + path};
                f.write("CORRUPTED", 9);
            }
            if (quiet([&] { return cmd_recover("A", false); }) != 0) return {false, "recovery failed"};
            if (!events_contain("INTEGRITY_FAILURE")) return {false, "no INTEGRITY_FAILURE event logged"};
            if (quiet([&] { return cmd_reconstruct(""); }) == 0 && identical(in_file, out_file))
                return {true, "SHA-256 mismatch rejected; recovered from second source"};
            return {false, "reconstruction failed after integrity rejection"};
        };
        auto t05 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("A") || !fail("B")) return {false, "could not fail peers"};
            quiet([&] { return cmd_detect(false, 3000); });
            const int rc = quiet([&] { return cmd_recover("A", false); });
            if (rc == 3 && events_contain("UNRECOVERABLE")) {
                if (quiet([&] { return cmd_reconstruct(""); }) != 0)
                    return {true, "no surviving source -> UNRECOVERABLE reported, reconstruction refused"};
                return {false, "reconstruction should have been refused"};
            }
            return {false, "expected UNRECOVERABLE (rc=" + std::to_string(rc) + ")"};
        };
        auto t06 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("A")) return {false, "could not fail peer A"};
            quiet([&] { return cmd_detect(true); });
            quiet([&] { return cmd_recover("A", false); });
            const int rc = quiet([&] { return cmd_reconstruct(""); });
            if (rc == 0 && identical(in_file, out_file) &&
                events_contain("\"byte_identical\": true"))
                return {true, "reconstruction in sequence order, byte-identical to original"};
            return {false, "reconstruction comparison failed (rc=" + std::to_string(rc) + ")"};
        };
        auto t07 = [&]() -> Result {
            if (!fresh() || !chunk_and_distribute()) return {false, "setup failed"};
            if (!fail("B") || !fail("C")) return {false, "could not fail peers"};
            quiet([&] { return cmd_detect(false, 3000); });
            std::string out;
            const int rc = quiet([&] { return cmd_recover("B", false); }, &out);
            // rf=2: chunks held by B+C (C2, C5) have no surviving source; the
            // rest must still be recovered and the loss reported explicitly.
            if (rc == 3 && out.find("UNRECOVERABLE") != std::string::npos &&
                out.find("recovered (3)") != std::string::npos)
                return {true, "two-peer failure: surviving chunks recovered, rest UNRECOVERABLE"};
            return {false, "unexpected result for two-peer failure (rc=" + std::to_string(rc) + ")"};
        };

        auto t08 = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            std::this_thread::sleep_for(std::chrono::milliseconds(300));
            quiet([&] { return cmd_reset(); });

            const uint16_t test_coord_port = 5500;
            const std::string test_coord_addr = "127.0.0.1";
            CoordinatorService coord("127.0.0.1", test_coord_port, 250, 600, 2);
            if (!coord.start()) return {false, "could not start coordinator service"};

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) {
                coord.stop();
                return {false, "peer executable not found"};
            }

            // Start 3 peers configured to dynamically register with coordinator
            std::vector<long> pids;
            std::vector<std::string> ids = {"M2_A", "M2_B", "M2_C"};
            std::vector<uint16_t> ports = {5501, 5502, 5503};
            for (size_t i = 0; i < 3; ++i) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, ids[i], "127.0.0.1", ports[i], pid, err,
                                      test_coord_addr, test_coord_port)) {
                    coord.stop();
                    for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                    return {false, "failed to start peer " + ids[i] + ": " + err};
                }
                pids.push_back(pid);
            }

            // Wait up to 4 seconds for all 3 peers to register with coordinator
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
            while (coord.registry().get_online_peers().size() < 3 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            auto online = coord.registry().get_online_peers();
            if (online.size() != 3) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "expected 3 online peers, got " + std::to_string(online.size())};
            }

            // Verify workload info (available storage > 0, active transfers == 0)
            for (const auto& peer_item : online) {
                if (peer_item.workload.available_storage_bytes == 0) {
                    for (long pid_val : pids) { std::string e; proc::terminate_peer(pid_val, e); }
                    coord.stop();
                    return {false, "peer " + peer_item.id + " reported 0 available storage"};
                }
                if (peer_item.workload.active_transfers != 0) {
                    for (long pid_val : pids) { std::string e; proc::terminate_peer(pid_val, e); }
                    coord.stop();
                    return {false, "peer " + peer_item.id + " active transfers non-zero"};
                }
            }

            // Terminate Peer M2_B (simulate failure)
            std::string terr;
            proc::terminate_peer(pids[1], terr);

            // Wait for failure detection: timeout=600ms, threshold=2 missed -> ~1500ms
            std::this_thread::sleep_for(std::chrono::milliseconds(1500));

            PeerInfo peer_b;
            if (!coord.registry().get_peer("M2_B", peer_b)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "peer M2_B not in registry"};
            }

            if (peer_b.state != PeerHealth::OFFLINE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "peer M2_B expected OFFLINE, got " + std::string(peer_health_str(peer_b.state))};
            }

            // Verify M2_A and M2_C are still ONLINE
            PeerInfo peer_a, peer_c;
            coord.registry().get_peer("M2_A", peer_a);
            coord.registry().get_peer("M2_C", peer_c);
            if (peer_a.state != PeerHealth::ONLINE || peer_c.state != PeerHealth::ONLINE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "peers A/C did not remain ONLINE during peer B failure"};
            }

            // Re-start Peer M2_B to verify re-registration and recovery to ONLINE
            long new_b_pid = 0;
            std::string err;
            if (!proc::start_peer(exe, "M2_B", "127.0.0.1", 5502, new_b_pid, err,
                                  test_coord_addr, test_coord_port)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "failed to re-start peer M2_B: " + err};
            }
            pids[1] = new_b_pid;

            // Wait for re-registration
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            coord.registry().get_peer("M2_B", peer_b);
            if (peer_b.state != PeerHealth::ONLINE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "re-started peer M2_B did not return to ONLINE"};
            }

            // Cleanup
            for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
            coord.stop();
            return {true, "dynamic registration, storage reporting, heartbeats & state transitions verified"};
        };

        auto t09_rs = [&]() -> Result {
            const std::string test_in = "input/rs_test_input.bin";
            std::error_code ec;
            fs::create_directories("input", ec);
            fs::create_directories("output/rs_test_shards", ec);

            std::vector<uint8_t> test_data(150000);
            for (size_t i = 0; i < test_data.size(); ++i) {
                test_data[i] = static_cast<uint8_t>((i * 73 + 19) & 0xFF);
            }
            {
                std::ofstream out(test_in, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(test_data.data()), static_cast<std::streamsize>(test_data.size()));
            }

            AdaptiveChunker chunker;
            uint64_t chosen_shard_size = 0;
            auto shards = chunker.encode_file("F_RS_TEST", test_in, "output/rs_test_shards", chosen_shard_size);

            if (chosen_shard_size != 65536) {
                return {false, "expected 64KB shard size for 150KB file, got " + std::to_string(chosen_shard_size)};
            }

            // Verify reconstruction from Pair 1: D0 + D1 (P0 erased)
            {
                std::string staging = "output/rs_test_pair_d0_d1";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.shard_type == "DATA") {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                    }
                }
                std::string out_rec = "output/rs_rec_d0_d1.bin";
                std::string err;
                if (!chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "reconstruction failed with D0+D1: " + err};
                }
                if (slurp(test_in) != slurp(out_rec)) {
                    return {false, "reconstructed file from D0+D1 is not byte-identical"};
                }
            }

            // Verify reconstruction from Pair 2: D0 + P0 (D1 erased)
            {
                std::string staging = "output/rs_test_pair_d0_p0";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.chunk_id.find("-D1") == std::string::npos) {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                    }
                }
                std::string out_rec = "output/rs_rec_d0_p0.bin";
                std::string err;
                if (!chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "reconstruction failed with D0+P0: " + err};
                }
                if (slurp(test_in) != slurp(out_rec)) {
                    return {false, "reconstructed file from D0+P0 is not byte-identical"};
                }
            }

            // Verify reconstruction from Pair 3: D1 + P0 (D0 erased)
            {
                std::string staging = "output/rs_test_pair_d1_p0";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.chunk_id.find("-D0") == std::string::npos) {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                    }
                }
                std::string out_rec = "output/rs_rec_d1_p0.bin";
                std::string err;
                if (!chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "reconstruction failed with D1+P0: " + err};
                }
                if (slurp(test_in) != slurp(out_rec)) {
                    return {false, "reconstructed file from D1+P0 is not byte-identical"};
                }
            }

            // Verify failure when only 1 shard is present
            {
                std::string staging = "output/rs_test_single_shard";
                fs::create_directories(staging, ec);
                for (const auto& sm : shards) {
                    if (sm.chunk_id.find("-D0") != std::string::npos) {
                        fs::copy_file("output/rs_test_shards/" + sm.chunk_id + ".chunk",
                                      staging + "/" + sm.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                        break;
                    }
                }
                std::string out_rec = "output/rs_rec_single.bin";
                std::string err;
                if (chunker.reconstruct_file("F_RS_TEST", test_data.size(), shards, staging, out_rec, err)) {
                    return {false, "expected reconstruction to fail with only 1 shard, but reported success"};
                }
            }

            return {true, "RS(3,2) systematic encoding & reconstruction verified across D0+D1, D0+P0, D1+P0 pairs"};
        };

        auto t10_gate2_e2e = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            // 1. Start Coordinator Service on port 5600
            CoordinatorService coord("127.0.0.1", 5600, 200, 500, 2);
            if (!coord.start()) {
                return {false, "failed to start Gate-2 Coordinator Service on port 5600"};
            }

            // 2. Start 3 Peer processes dynamically registering with coordinator (127.0.0.1:5600)
            std::vector<std::pair<std::string, uint16_t>> peer_cfgs = {
                {"G2_A", 5601}, {"G2_B", 5602}, {"G2_C", 5603}
            };
            std::vector<long> pids;
            for (const auto& pc : peer_cfgs) {
                long pid = 0;
                std::string err;
                if (!proc::start_peer(exe, pc.first, "127.0.0.1", pc.second, pid, err, "127.0.0.1", 5600)) {
                    for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                    coord.stop();
                    return {false, "failed to start peer " + pc.first + ": " + err};
                }
                pids.push_back(pid);
            }

            // Wait for dynamic registration
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            coord.registry().evaluate_health(500, 2);
            auto online = coord.registry().get_online_peers();
            if (online.size() != 3) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "expected 3 dynamically registered online peers, got " + std::to_string(online.size())};
            }

            // 3. Stage a file centrally in Coordinator catalog
            fs::create_directories("output");
            const std::string test_in = "output/gate2_e2e_input.bin";
            std::vector<uint8_t> test_bytes(140000);
            for (size_t i = 0; i < test_bytes.size(); ++i) {
                test_bytes[i] = static_cast<uint8_t>((i * 73 + 19) & 0xFF);
            }
            {
                std::ofstream out(test_in, std::ios::binary | std::ios::trunc);
                out.write(reinterpret_cast<const char*>(test_bytes.data()), static_cast<std::streamsize>(test_bytes.size()));
            }

            FileRecord rec;
            std::string stage_err;
            if (!coord.catalog().stage_file(test_in, rec, stage_err)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "failed to stage file: " + stage_err};
            }
            if (rec.status != FileDistributionStatus::STORED) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "initial file status is not STORED"};
            }

            // 4. Test on-demand first download with direct peer retrieval & local RS(3,2) reconstruction
            ClientDownload client("127.0.0.1", 5600, "output/gate2_e2e_client");
            DownloadResult dl_res = client.download(rec.file_id, DownloadSource::PEERS, &coord);
            if (!dl_res.success) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Gate-2 peer-based download & RS reconstruction failed: " + dl_res.message};
            }

            if (dl_res.actual_sha256 != rec.sha256) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Gate-2 reconstructed file SHA-256 mismatch"};
            }

            // Verify status transitioned to AVAILABLE
            FileRecord post_dist_rec;
            coord.catalog().get_file(rec.file_id, post_dist_rec);
            if (post_dist_rec.status != FileDistributionStatus::AVAILABLE) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, std::string("expected file status AVAILABLE after first download, got ") + file_status_str(post_dist_rec.status)};
            }

            // 5. Test central fallback (--source central)
            DownloadResult central_res = client.download(rec.file_id, DownloadSource::CENTRAL, &coord);
            if (!central_res.success || central_res.actual_sha256 != rec.sha256) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Gate-2 central fallback download failed: " + central_res.message};
            }

            // 6. Test Failure & Peer-to-Peer Durability Repair
            // Terminate Peer G2_B (pids[1])
            std::string term_err;
            proc::terminate_peer(pids[1], term_err);

            // Start replacement Peer G2_D
            long pid_d = 0;
            std::string err_d;
            if (!proc::start_peer(exe, "G2_D", "127.0.0.1", 5604, pid_d, err_d, "127.0.0.1", 5600)) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "failed to start replacement peer G2_D: " + err_d};
            }
            pids.push_back(pid_d);

            // Wait for Peer G2_B to be detected as OFFLINE and Peer G2_D to register ONLINE
            std::this_thread::sleep_for(std::chrono::milliseconds(1200));
            coord.registry().evaluate_health(500, 2);

            std::string repair_log;
            bool rep_ok = coord.repair_file(rec.file_id, repair_log);
            if (!rep_ok) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Coordinator repair_file failed: " + repair_log};
            }

            // Download again from surviving peers (now including repaired peer G2_D)
            DownloadResult dl_post_repair = client.download(rec.file_id, DownloadSource::PEERS, &coord);
            if (!dl_post_repair.success || dl_post_repair.actual_sha256 != rec.sha256) {
                for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
                coord.stop();
                return {false, "Download after repair failed: " + dl_post_repair.message};
            }

            // Cleanup
            for (long p : pids) { std::string e; proc::terminate_peer(p, e); }
            coord.stop();
            return {true, "Gate-2 E2E verified: On-demand RS(3,2) distribution, direct peer retrieval, central fallback, and peer-to-peer durability repair"};
        };

        auto t11_recovery_flow = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            const std::string secret = "t11-secret";
            CoordinatorService coord("127.0.0.1", 5650, 200, 500, 2, 2, 1, 0, secret, true, true);
            if (!coord.start()) return {false, "failed to start coordinator on port 5650"};

            struct PeerSpec { std::string id; uint16_t port; long pid = 0; };
            std::vector<PeerSpec> pspecs = {
                {"R11_A", 5651, 0}, {"R11_B", 5652, 0}, {"R11_C", 5653, 0}, {"R11_D", 5654, 0}
            };
            auto cleanup = [&]() {
                for (const auto& ps : pspecs) {
                    if (ps.pid > 0) { std::string e; proc::terminate_peer(ps.pid, e); }
                }
                coord.stop();
            };

            for (auto& ps : pspecs) {
                std::string err;
                if (!proc::start_peer(exe, ps.id, "127.0.0.1", ps.port, ps.pid, err, "127.0.0.1", 5650)) {
                    cleanup();
                    return {false, "failed to start " + ps.id + ": " + err};
                }
            }

            auto wait_online = [&](size_t target_count, int max_ms) -> bool {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(max_ms);
                while (std::chrono::steady_clock::now() < deadline) {
                    if (coord.registry().get_online_peers().size() == target_count) return true;
                    std::this_thread::sleep_for(std::chrono::milliseconds(80));
                }
                return false;
            };
            if (!wait_online(4, 5000)) {
                cleanup();
                return {false, "expected 4 online peers"};
            }

            // 1. Verify unauthenticated TCP call gets AUTH_ERROR
            {
                TCPClient tc(2000, 2000);
                if (!tc.connect_to("127.0.0.1", 5650)) { cleanup(); return {false, "TCP connect failed"}; }
                json::Json req = json::Json::object();
                req["type"] = "CATALOG_LIST";
                json::Json resp;
                std::vector<uint8_t> pl;
                if (tc.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
                    resp["type"].as_string() != proto::AUTH_ERROR) {
                    cleanup();
                    return {false, "expected AUTH_ERROR for unauthenticated TCP request"};
                }
            }

            // 2. Authenticate TCP session
            std::string sess_tok;
            {
                TCPClient tc(2000, 2000);
                if (!tc.connect_to("127.0.0.1", 5650)) { cleanup(); return {false, "TCP auth connect failed"}; }
                json::Json req = json::Json::object();
                req["type"] = proto::SESSION_AUTH;
                req["client_id"] = "t11-client";
                req["auth_token"] = secret;
                json::Json resp;
                std::vector<uint8_t> pl;
                if (tc.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
                    resp["type"].as_string() != proto::SESSION_AUTH_ACK) {
                    cleanup();
                    return {false, "SESSION_AUTH failed"};
                }
                sess_tok = resp["session_token"].as_string();
            }

            // 3. Stream upload in 2 parts -> immediate chunking + RS(3,2) + placement
            std::vector<uint8_t> payload(180000);
            for (size_t i = 0; i < payload.size(); ++i) {
                payload[i] = static_cast<uint8_t>((i * 131 + 7) & 0xFF);
            }
            const std::string expected_sha = ChunkManager::sha256_of_bytes(payload);

            std::string fid;
            {
                TCPClient tc(2000, 15000);
                tc.connect_to("127.0.0.1", 5650);
                json::Json init = json::Json::object();
                init["type"] = proto::UPLOAD_INIT;
                init["session_token"] = sess_tok;
                init["filename"] = "t11_resilient.bin";
                init["size"] = static_cast<long long>(payload.size());
                json::Json r;
                std::vector<uint8_t> pl;
                if (tc.request(init, {}, r, pl) != proto::RecvStatus::OK ||
                    r["type"].as_string() != proto::UPLOAD_INIT_ACK) {
                    cleanup();
                    return {false, "UPLOAD_INIT failed"};
                }
                const std::string up_id = r["upload_id"].as_string();

                std::vector<uint8_t> p1(payload.begin(), payload.begin() + 90000);
                std::vector<uint8_t> p2(payload.begin() + 90000, payload.end());
                for (const auto& part : {p1, p2}) {
                    TCPClient ptc(2000, 10000);
                    ptc.connect_to("127.0.0.1", 5650);
                    json::Json pr = json::Json::object();
                    pr["type"] = proto::UPLOAD_PART;
                    pr["session_token"] = sess_tok;
                    pr["upload_id"] = up_id;
                    json::Json pr_ack;
                    if (ptc.request(pr, part, pr_ack, pl) != proto::RecvStatus::OK ||
                        pr_ack["type"].as_string() != proto::UPLOAD_PART_ACK) {
                        cleanup();
                        return {false, "UPLOAD_PART failed"};
                    }
                }

                TCPClient ftc(2000, 20000);
                ftc.connect_to("127.0.0.1", 5650);
                json::Json fin = json::Json::object();
                fin["type"] = proto::UPLOAD_COMPLETE;
                fin["session_token"] = sess_tok;
                fin["upload_id"] = up_id;
                json::Json fack;
                if (ftc.request(fin, {}, fack, pl) != proto::RecvStatus::OK ||
                    fack["type"].as_string() != proto::UPLOAD_COMPLETE_ACK) {
                    cleanup();
                    return {false, "UPLOAD_COMPLETE failed"};
                }
                fid = fack["file_id"].as_string();
                if (fack["status_str"].as_string() != "AVAILABLE") {
                    cleanup();
                    return {false, "expected immediate AVAILABLE status on upload, got " + fack["status_str"].as_string()};
                }
            }

            // 4. Corrupt one shard on disk -> ClientDownload detects hash mismatch, reconstructs via RS,
            //    reports CORRUPT to Coordinator -> auto-repair restores shard to HEALTHY
            FileRecord rec;
            coord.catalog().get_file(fid, rec);
            const std::string target_cid = rec.shards[0].chunk_id;
            const std::string target_pid = rec.shards[0].peers[0];
            {
                const std::string sh_path = "storage/peer_" + target_pid + "/chunks/" + target_cid + ".chunk";
                std::fstream cf(sh_path, std::ios::in | std::ios::out | std::ios::binary);
                if (!cf) { cleanup(); return {false, "failed to open shard for corruption test: " + sh_path}; }
                cf.write("CORRUPTED_SHARD_PAYLOAD", 23);
            }

            {
                ClientDownload dl_cli("127.0.0.1", 5650, "output/t11_dl1", sess_tok, secret);
                DownloadResult dl1 = dl_cli.download(fid, DownloadSource::PEERS, nullptr);
                if (!dl1.success || dl1.actual_sha256 != expected_sha) {
                    cleanup();
                    return {false, "download failed when 1 shard was corrupt on disk: " + dl1.message};
                }
                if (dl1.corrupted_shards_detected == 0) {
                    cleanup();
                    return {false, "expected ClientDownload to detect & report corrupted shard"};
                }
            }

            // Wait for Coordinator background auto-repair after CORRUPT report
            {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                bool repaired = false;
                while (std::chrono::steady_clock::now() < deadline) {
                    FileRecord cur;
                    if (coord.catalog().get_file(fid, cur) &&
                        cur.status == FileDistributionStatus::AVAILABLE &&
                        cur.shards[0].status == "HEALTHY") {
                        repaired = true;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                if (!repaired) {
                    cleanup();
                    return {false, "auto-repair did not restore corrupted shard back to HEALTHY"};
                }
                if (!events_contain("SHARD_CORRUPT")) {
                    cleanup();
                    return {false, "SHARD_CORRUPT audit event missing"};
                }
            }

            // 5. Step 6 NO branch -> RECOVERY_PENDING when < K valid shards survive, then backoff resume!
            coord.catalog().get_file(fid, rec);
            std::vector<std::string> stripe0_peers;
            for (const auto& sh : rec.shards) {
                if (sh.stripe_id == "S000" && !sh.peers.empty()) {
                    stripe0_peers.push_back(sh.peers[0]);
                }
            }
            if (stripe0_peers.size() < 3) {
                cleanup();
                return {false, "stripe S000 does not span 3 distinct peers"};
            }
            const std::string sole_survivor = stripe0_peers[0];
            std::vector<size_t> killed_indices;
            for (size_t i = 0; i < pspecs.size(); ++i) {
                if (pspecs[i].id != sole_survivor) {
                    std::string e;
                    proc::terminate_peer(pspecs[i].pid, e);
                    pspecs[i].pid = 0;
                    killed_indices.push_back(i);
                }
            }

            // Wait for RECOVERY_PENDING when only 1 peer (< K=2) survives
            {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
                bool got_pending = false;
                while (std::chrono::steady_clock::now() < deadline) {
                    FileRecord cur;
                    if (coord.catalog().get_file(fid, cur) &&
                        cur.status == FileDistributionStatus::RECOVERY_PENDING) {
                        got_pending = true;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                if (!got_pending) {
                    cleanup();
                    return {false, "expected RECOVERY_PENDING when < K=2 shards remain online"};
                }
            }

            // Confirm ClientDownload refuses cleanly during < K outage
            {
                ClientDownload dl_fail("127.0.0.1", 5650, "output/t11_fail", sess_tok, secret);
                DownloadResult df = dl_fail.download(fid, DownloadSource::PEERS, nullptr);
                if (df.success) {
                    cleanup();
                    return {false, "expected download to fail when fewer than K shards are online"};
                }
            }

            // Restart 2 killed peers -> brings cluster back to 3 peers (>=2 valid shards + target),
            // backoff worker automatically wakes & completes repair to AVAILABLE!
            for (size_t k = 0; k < 2 && k < killed_indices.size(); ++k) {
                auto& ps = pspecs[killed_indices[k]];
                std::string err;
                if (!proc::start_peer(exe, ps.id, "127.0.0.1", ps.port, ps.pid, err, "127.0.0.1", 5650)) {
                    cleanup();
                    return {false, "failed to restart peer " + ps.id + ": " + err};
                }
            }

            {
                auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
                bool back_available = false;
                while (std::chrono::steady_clock::now() < deadline) {
                    FileRecord cur;
                    if (coord.catalog().get_file(fid, cur) &&
                        cur.status == FileDistributionStatus::AVAILABLE) {
                        back_available = true;
                        break;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(150));
                }
                if (!back_available) {
                    cleanup();
                    return {false, "RECOVERY_PENDING did not auto-recover back to AVAILABLE after peers restarted"};
                }
            }

            // Verify full download succeeds byte-identically after auto-recovery
            {
                ClientDownload dl_ok("127.0.0.1", 5650, "output/t11_final", sess_tok, secret);
                DownloadResult dr = dl_ok.download(fid, DownloadSource::PEERS, nullptr);
                if (!dr.success || dr.actual_sha256 != expected_sha) {
                    cleanup();
                    return {false, "final post-recovery download failed: " + dr.message};
                }
            }

            cleanup();
            return {true, "Auth, immediate RS upload, corrupt-shard auto-repair & RECOVERY_PENDING backoff verified"};
        };

        auto t12_auth_http = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            const std::string secret = "http-secret-token";
            const uint16_t tcp_port = 5660;
            const uint16_t http_port = 5680;
            CoordinatorService coord("127.0.0.1", tcp_port, 200, 500, 2, 2, 1, http_port, secret, true, true);
            if (!coord.start()) return {false, "failed to start HTTP/TCP Coordinator"};

            struct PeerSpec { std::string id; uint16_t port; long pid = 0; };
            std::vector<PeerSpec> pspecs = {
                {"H12_A", 5661, 0}, {"H12_B", 5662, 0}, {"H12_C", 5663, 0}
            };
            auto cleanup = [&]() {
                for (const auto& ps : pspecs) {
                    if (ps.pid > 0) { std::string e; proc::terminate_peer(ps.pid, e); }
                }
                coord.stop();
            };

            for (auto& ps : pspecs) {
                std::string err;
                if (!proc::start_peer(exe, ps.id, "127.0.0.1", ps.port, ps.pid, err, "127.0.0.1", tcp_port)) {
                    cleanup();
                    return {false, "failed to start peer " + ps.id + ": " + err};
                }
            }

            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
            while (coord.registry().get_online_peers().size() < 3 &&
                   std::chrono::steady_clock::now() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(80));
            }
            if (coord.registry().get_online_peers().size() != 3) {
                cleanup();
                return {false, "expected 3 online peers"};
            }

            struct HttpResp { int code = 0; std::string body; };
            auto http_call = [&](const std::string& method,
                                 const std::string& path,
                                 const std::string& extra_hdrs,
                                 const std::vector<uint8_t>& body) -> HttpResp {
                HttpResp res;
                sockaddr_in addr{};
                addr.sin_family = AF_INET;
                addr.sin_port = htons(http_port);
                ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
                socket_t fd = ::socket(AF_INET, SOCK_STREAM, 0);
                if (fd == INVALID_SOCK) return res;
                proto::set_timeouts(fd, 10000, 10000);
                if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
                    sock_close(fd);
                    return res;
                }
                std::ostringstream req;
                req << method << " " << path << " HTTP/1.1\r\n"
                    << "Host: 127.0.0.1:" << http_port << "\r\n"
                    << "Content-Length: " << body.size() << "\r\n"
                    << "Connection: close\r\n"
                    << extra_hdrs
                    << "\r\n";
                const std::string h = req.str();
                proto::send_all(fd, h.data(), h.size());
                if (!body.empty()) proto::send_all(fd, body.data(), body.size());

                std::string raw;
                char buf[4096];
                for (;;) {
                    int n = ::recv(fd, buf, sizeof(buf), 0);
                    if (n <= 0) break;
                    raw.append(buf, static_cast<size_t>(n));
                }
                sock_close(fd);
                size_t sp = raw.find("\r\n\r\n");
                if (sp == std::string::npos) return res;
                std::istringstream sl(raw.substr(0, raw.find("\r\n")));
                std::string ver;
                sl >> ver >> res.code;
                res.body = raw.substr(sp + 4);
                return res;
            };

            // 1. Web Console GET /
            auto home = http_call("GET", "/", "", {});
            if (home.code != 200 || home.body.find("Resilient Multi-Peer File Storage") == std::string::npos) {
                cleanup();
                return {false, "GET / web console check failed (HTTP " + std::to_string(home.code) + ")"};
            }

            // 2. Unauthenticated upload rejected 401
            auto unauth = http_call("POST", "/api/upload", "X-Filename: unauth.bin\r\n", {'a', 'b', 'c'});
            if (unauth.code != 401) {
                cleanup();
                return {false, "expected 401 on unauthenticated POST /api/upload, got " + std::to_string(unauth.code)};
            }

            // 3. Wrong auth token rejected 401
            {
                const std::string bad = "{\"token\":\"wrong\",\"client_id\":\"t12\"}";
                std::vector<uint8_t> b(bad.begin(), bad.end());
                auto ar = http_call("POST", "/api/auth", "Content-Type: application/json\r\n", b);
                if (ar.code != 401) {
                    cleanup();
                    return {false, "expected 401 for wrong auth token"};
                }
            }

            // 4. Valid HTTP auth -> session_token
            std::string sess_tok;
            {
                const std::string good = "{\"token\":\"" + secret + "\",\"client_id\":\"t12-http\"}";
                std::vector<uint8_t> b(good.begin(), good.end());
                auto ar = http_call("POST", "/api/auth", "Content-Type: application/json\r\n", b);
                if (ar.code != 200) {
                    cleanup();
                    return {false, "POST /api/auth failed with HTTP " + std::to_string(ar.code)};
                }
                auto aj = json::Json::parse(ar.body);
                sess_tok = aj["session_token"].as_string();
            }

            // 5. Authenticated HTTP binary upload -> immediate AVAILABLE
            std::vector<uint8_t> bin(120000);
            for (size_t i = 0; i < bin.size(); ++i) bin[i] = static_cast<uint8_t>((i * 41 + 13) & 0xFF);
            const std::string bin_sha = ChunkManager::sha256_of_bytes(bin);

            auto up = http_call("POST", "/api/upload",
                                "Authorization: Bearer " + sess_tok + "\r\nX-Filename: http_demo.bin\r\n",
                                bin);
            if (up.code != 200) {
                cleanup();
                return {false, "HTTP POST /api/upload failed with code " + std::to_string(up.code)};
            }
            auto uj = json::Json::parse(up.body);
            const std::string fid = uj["file_id"].as_string();
            if (uj["status"].as_string() != "AVAILABLE") {
                cleanup();
                return {false, "expected AVAILABLE after HTTP upload, got " + uj["status"].as_string()};
            }

            // 6. Download via ClientDownload (PEERS mode) AND verify remote TCP CENTRAL mode
            ClientDownload cdl("127.0.0.1", tcp_port, "output/t12_dl", sess_tok, secret);
            auto dr_peers = cdl.download(fid, DownloadSource::PEERS, nullptr);
            if (!dr_peers.success || dr_peers.actual_sha256 != bin_sha) {
                cleanup();
                return {false, "peer download of HTTP-uploaded file failed: " + dr_peers.message};
            }

            // 7. Download via HTTP GET /api/download/<fid>
            auto hdl = http_call("GET", "/api/download/" + fid, "Authorization: Bearer " + sess_tok + "\r\n", {});
            if (hdl.code != 200 || hdl.body.size() != bin.size() ||
                std::string(bin.begin(), bin.end()) != hdl.body) {
                cleanup();
                return {false, "HTTP GET /api/download verification failed"};
            }

            // 8. Visual Inspector API: probe=1 requires auth, verifies GF(256) parity on live peers, and proves live recovery when peer is down
            {
                auto unauth_insp = http_call("GET", "/api/inspect/" + fid + "?stripe=0&probe=1", "", {});
                if (unauth_insp.code != 401) {
                    cleanup();
                    return {false, "expected 401 on unauthenticated /api/inspect?probe=1"};
                }
                auto insp1 = http_call("GET", "/api/inspect/" + fid + "?stripe=0&probe=1",
                                       "Authorization: Bearer " + sess_tok + "\r\n", {});
                if (insp1.code != 200) {
                    cleanup();
                    return {false, "GET /api/inspect?probe=1 failed"};
                }
                auto ij1 = json::Json::parse(insp1.body);
                if (!ij1["parity_check"]["checked"].as_bool() || !ij1["parity_check"]["verified"].as_bool()) {
                    cleanup();
                    return {false, "/api/inspect did not verify live GF(256) parity across cluster peers"};
                }

                // Kill one peer and confirm /api/inspect performs live C++ RS reconstruction from surviving shards
                {
                    std::string err;
                    proc::terminate_peer(pspecs[1].pid, err);
                    pspecs[1].pid = 0;
                }
                auto dl2 = std::chrono::steady_clock::now() + std::chrono::seconds(3);
                while (coord.registry().get_online_peers().size() > 2 &&
                       std::chrono::steady_clock::now() < dl2) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(80));
                }
                auto insp2 = http_call("GET", "/api/inspect/" + fid + "?stripe=0&probe=1",
                                       "Authorization: Bearer " + sess_tok + "\r\n", {});
                auto ij2 = json::Json::parse(insp2.body);
                if (!ij2["inspected_stripe"].contains("live_recovery_proof") ||
                    !ij2["inspected_stripe"]["live_recovery_proof"]["performed"].as_bool() ||
                    !ij2["inspected_stripe"]["live_recovery_proof"]["recovered"][0]["sha256_match"].as_bool()) {
                    cleanup();
                    return {false, "/api/inspect did not produce verified live_recovery_proof when peer went offline"};
                }
            }

            cleanup();
            return {true, "HTTP upload API, PowerShell Monitor, /api/inspect RS proof & cross-protocol download verified"};
        };

        auto t13_hybrid_algorithms = [&]() -> Result {
            quiet([&] { return cmd_sys_stop(); });
            quiet([&] { return cmd_reset(); });

            // 1. Verify DACA file-size + resource-constraint adaptive sizing
            {
                AdaptiveChunker daca(AdaptivePolicy(), 4, 2);
                if (daca.calculate_shard_size(512ULL * 1024ULL) != 65536ULL)
                    return {false, "DACA <=1MB tier expected 64KB"};
                if (daca.calculate_shard_size(2ULL * 1024ULL * 1024ULL) != 262144ULL)
                    return {false, "DACA 1-10MB tier expected 256KB"};
                if (daca.calculate_shard_size(2ULL * 1024ULL * 1024ULL, 64ULL * 1024ULL) != 16384ULL)
                    return {false, "DACA resource-constrained adaptation failed to step down to 16KB"};
            }

            // 2. Verify Systematic GF(256) RS(6,4) double-loss recovery + MGFRA >= 1000-stripe ordering
            {
                std::error_code ec;
                fs::create_directories("output/t13_rs64_shards", ec);
                std::vector<uint8_t> buf(25000);
                for (size_t i = 0; i < buf.size(); ++i) buf[i] = static_cast<uint8_t>((i * 97 + 31) & 0xFF);
                const std::string rs64_in = "output/t13_rs64_in.bin";
                {
                    std::ofstream o(rs64_in, std::ios::binary | std::ios::trunc);
                    o.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
                }
                AdaptivePolicy p64;
                p64.size_under_1mb = 1024;
                AdaptiveChunker ch64(p64, 4, 2);
                uint64_t sh_sz = 0;
                auto sh64 = ch64.encode_file("F64", rs64_in, "output/t13_rs64_shards", sh_sz);

                // Drop 2 shards per stripe (D1 and D3), reconstruct using D0 + D2 + P0 + P1
                std::string st64 = "output/t13_rs64_surv";
                fs::create_directories(st64, ec);
                for (const auto& s : sh64) {
                    if (s.chunk_id.find("-D1") == std::string::npos &&
                        s.chunk_id.find("-D3") == std::string::npos) {
                        fs::copy_file("output/t13_rs64_shards/" + s.chunk_id + ".chunk",
                                      st64 + "/" + s.chunk_id + ".chunk",
                                      fs::copy_options::overwrite_existing, ec);
                    }
                }
                std::string rs64_out = "output/t13_rs64_out.bin";
                std::string err;
                if (!ch64.reconstruct_file("F64", buf.size(), sh64, st64, rs64_out, err) ||
                    slurp(rs64_in) != slurp(rs64_out)) {
                    return {false, "RS(6,4) double-shard recovery failed: " + err};
                }

                // MGFRA numeric sequence ordering check across >1000 stripes (S1000..S1004 vs S200)
                // Synthesize 1005 tiny 2-shard stripes safely by encoding a 16080-byte file with 8B shards
                AdaptivePolicy p_many;
                p_many.min_size_bytes = 8;
                p_many.size_under_1mb = 8;
                AdaptiveChunker ch_many(p_many, 2, 1);
                std::vector<uint8_t> many_buf(16080); // 1005 stripes * 2 * 8B = 16080B -> S000 .. S1004
                for (size_t i = 0; i < many_buf.size(); ++i) many_buf[i] = static_cast<uint8_t>((i * 19 + 5) & 0xFF);
                const std::string many_in = "output/t13_many_in.bin";
                {
                    std::ofstream o(many_in, std::ios::binary | std::ios::trunc);
                    o.write(reinterpret_cast<const char*>(many_buf.data()), static_cast<std::streamsize>(many_buf.size()));
                }
                fs::create_directories("output/t13_many_shards", ec);
                uint64_t many_sh_sz = 0;
                auto sh_many = ch_many.encode_file("FM", many_in, "output/t13_many_shards", many_sh_sz);
                // Reverse manifest order to verify MGFRA sorts strictly by numeric sequence (not stripe_id string)
                std::reverse(sh_many.begin(), sh_many.end());
                std::string many_out = "output/t13_many_out.bin";
                if (!ch_many.reconstruct_file("FM", many_buf.size(), sh_many, "output/t13_many_shards", many_out, err) ||
                    slurp(many_in) != slurp(many_out)) {
                    return {false, "MGFRA >1000-stripe numeric sequence ordering check failed: " + err};
                }
                fs::remove_all("output/t13_rs64_shards", ec);
                fs::remove_all("output/t13_rs64_surv", ec);
                fs::remove_all("output/t13_many_shards", ec);
            }

            // 3. Full live cluster PASPA + ARMA + HPFD + HRERA (both Replica Copy & RS Decode)
            const std::string exe = proc::find_peer_exe();
            if (exe.empty()) return {false, "peer binary not found"};

            const std::string secret = "t13-hybrid-secret";
            const uint16_t tcp_port = 5670;
            CoordinatorService coord("127.0.0.1", tcp_port, 200, 500, 2, 2, 1, 0, secret, true, true, true, 1);
            if (!coord.start()) return {false, "failed to start Coordinator on 5670"};

            struct PeerSpec { std::string id; uint16_t port; long pid = 0; };
            std::vector<PeerSpec> pspecs = {
                {"H13_A", 5671, 0}, {"H13_B", 5672, 0}, {"H13_C", 5673, 0}, {"H13_D", 5674, 0}, {"H13_E", 5675, 0}
            };
            auto cleanup = [&]() {
                for (const auto& ps : pspecs) {
                    if (ps.pid > 0) { std::string e; proc::terminate_peer(ps.pid, e); }
                }
                coord.stop();
            };

            for (auto& ps : pspecs) {
                std::string err;
                if (!proc::start_peer(exe, ps.id, "127.0.0.1", ps.port, ps.pid, err, "127.0.0.1", tcp_port)) {
                    cleanup();
                    return {false, "failed to start peer " + ps.id + ": " + err};
                }
            }

            auto wait_online = [&](size_t target, int ms) -> bool {
                auto dl = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
                while (std::chrono::steady_clock::now() < dl) {
                    if (coord.registry().get_online_peers().size() == target) return true;
                    std::this_thread::sleep_for(std::chrono::milliseconds(80));
                }
                return false;
            };
            if (!wait_online(5, 5000)) { cleanup(); return {false, "expected 5 online peers"}; }

            // Single 120 KB upload fits in 1 stripe (D0=64KB, D1=56KB, P0=64KB -> 3 primary + 1 ARMA hot replica on D0)
            std::vector<uint8_t> payload(120000);
            for (size_t i = 0; i < payload.size(); ++i) payload[i] = static_cast<uint8_t>((i * 53 + 11) & 0xFF);
            const std::string expected_sha = ChunkManager::sha256_of_bytes(payload);

            std::string sess_tok;
            {
                TCPClient tc(2000, 2000);
                tc.connect_to("127.0.0.1", tcp_port);
                json::Json req = json::Json::object();
                req["type"] = proto::SESSION_AUTH;
                req["client_id"] = "t13-client";
                req["auth_token"] = secret;
                json::Json resp;
                std::vector<uint8_t> pl;
                if (tc.request(req, {}, resp, pl) != proto::RecvStatus::OK ||
                    resp["type"].as_string() != proto::SESSION_AUTH_ACK) {
                    cleanup(); return {false, "SESSION_AUTH failed"};
                }
                sess_tok = resp["session_token"].as_string();
            }

            std::string fid;
            {
                TCPClient tc(2000, 15000);
                tc.connect_to("127.0.0.1", tcp_port);
                json::Json up = json::Json::object();
                up["type"] = "UPLOAD_FILE";
                up["session_token"] = sess_tok;
                up["filename"] = "t13_hybrid.bin";
                json::Json uresp;
                std::vector<uint8_t> pl;
                if (tc.request(up, payload, uresp, pl) != proto::RecvStatus::OK ||
                    uresp["status_str"].as_string() != "AVAILABLE") {
                    cleanup(); return {false, "hybrid upload failed"};
                }
                fid = uresp["file_id"].as_string();
            }

            // Check ARMA placed selective hot replica for S000-D0 across 2 distinct peers
            FileRecord rec;
            coord.catalog().get_file(fid, rec);
            if (rec.shards.size() != 3 || rec.shards[0].peers.size() != 2 ||
                rec.shards[1].peers.size() != 1 || rec.shards[2].peers.size() != 1) {
                cleanup();
                return {false, "expected ARMA placement S000-D0=2 peers, D1=1, P0=1"};
            }
            if (!events_contain("DACA_CHUNKED")) {
                cleanup(); return {false, "missing DACA_CHUNKED audit event"};
            }

            const std::string d0_primary = rec.shards[0].peers[0];
            const std::string d0_replica = rec.shards[0].peers[1];
            const std::string d1_sole    = rec.shards[1].peers[0];

            // HRERA Path A: Kill primary holder of S000-D0 (has hot replica on d0_replica, spare 5th peer exists)
            size_t killed_p0_idx = 0;
            for (size_t i = 0; i < pspecs.size(); ++i) {
                if (pspecs[i].id == d0_primary) {
                    std::string e;
                    proc::terminate_peer(pspecs[i].pid, e);
                    pspecs[i].pid = 0;
                    killed_p0_idx = i;
                    break;
                }
            }

            // Wait for HPFD -> HRERA Replica Copy Recovery (`REPAIR_REPLICA_OK`)
            {
                auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(6);
                while (std::chrono::steady_clock::now() < dl && !events_contain("REPAIR_REPLICA_OK")) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                if (!events_contain("REPAIR_REPLICA_OK")) {
                    cleanup();
                    return {false, "HRERA did not trigger REPAIR_REPLICA_OK on replicated shard peer loss"};
                }
                if (events_contain("REPAIR_RS_OK")) {
                    cleanup();
                    return {false, "HRERA needlessly used RS decode when alternate replica existed"};
                }
                coord.catalog().get_file(fid, rec);
                if (rec.shards[0].peers.size() != 2 ||
                    std::find(rec.shards[0].peers.begin(), rec.shards[0].peers.end(), d0_replica) == rec.shards[0].peers.end()) {
                    cleanup();
                    return {false, "S000-D0 replica holder list mismatch after COPY_SHARD"};
                }
            }

            // Restart the terminated peer (now free healthy 5th peer), then kill sole holder of S000-D1!
            {
                std::string err;
                if (!proc::start_peer(exe, pspecs[killed_p0_idx].id, "127.0.0.1",
                                      pspecs[killed_p0_idx].port, pspecs[killed_p0_idx].pid,
                                      err, "127.0.0.1", tcp_port)) {
                    cleanup();
                    return {false, "failed to restart spare peer: " + err};
                }
                if (!wait_online(5, 4000)) { cleanup(); return {false, "spare peer did not come back ONLINE"}; }
            }

            for (size_t i = 0; i < pspecs.size(); ++i) {
                if (pspecs[i].id == d1_sole) {
                    std::string e;
                    proc::terminate_peer(pspecs[i].pid, e);
                    pspecs[i].pid = 0;
                    break;
                }
            }

            // HRERA Path B: No replica exists for S000-D1 -> escalates to P2P RS Decode (`REPAIR_RS_OK`)!
            {
                auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(6);
                while (std::chrono::steady_clock::now() < dl && !events_contain("REPAIR_RS_OK")) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                if (!events_contain("REPAIR_RS_OK")) {
                    cleanup();
                    return {false, "HRERA did not escalate to REPAIR_RS_OK when unreplicated shard peer failed"};
                }
            }

            // Final peer-to-client MGFRA download verification
            {
                ClientDownload cdl("127.0.0.1", tcp_port, "output/t13_dl", sess_tok, secret);
                DownloadResult dr = cdl.download(fid, DownloadSource::PEERS, nullptr);
                if (!dr.success || dr.actual_sha256 != expected_sha) {
                    cleanup();
                    return {false, "T13 final MGFRA peer download failed: " + dr.message};
                }
            }

            cleanup();
            return {true, "DACA, RS(6,4), MGFRA 1005-stripe order, PASPA, ARMA & HRERA (Replica+RS) verified"};
        };

        struct Test { const char* name; std::function<Result()> run; };
        const std::vector<Test> tests = {
            {"T01", t01}, {"T02", t02}, {"T03", t03}, {"T04", t04},
            {"T05", t05}, {"T06", t06}, {"T07", t07}, {"T08_M2", t08},
            {"T09_RS", t09_rs}, {"T10_GATE2", t10_gate2_e2e},
            {"T11_RECOVERY_FLOW", t11_recovery_flow}, {"T12_AUTH_HTTP", t12_auth_http},
            {"T13_HYBRID_ALGORITHMS", t13_hybrid_algorithms}};

        std::cout << "================ Resilient File Engine Test Suite ================\n";
        int pass = 0, failn = 0;
        for (const auto& t : tests) {
            if (!only.empty() &&
                std::find(only.begin(), only.end(), std::string(t.name)) == only.end())
                continue;
            Result r;
            try {
                r = t.run();
            } catch (const std::exception& e) {
                r = {false, std::string("exception: ") + e.what()};
            }
            std::cout << (r.first ? "PASS " : "FAIL ") << t.name << " - " << r.second << std::endl;
            (r.first ? pass : failn)++;
        }
        quiet([&] { return cmd_sys_stop(); });
        quiet([&] { return cmd_reset(); });
        std::cout << "===================================================\n";
        std::cout << "PASS=" << pass << " FAIL=" << failn << "\n";
        return failn == 0 ? 0 : 1;
    }

private:
    json::Json config_;
    std::vector<PeerInfo> peers_;
    MetadataManager meta_{"metadata"};
    uint64_t chunk_size_ = 65536;
    int hb_interval_ = 500, hb_timeout_ = 1000, hb_threshold_ = 2;
    std::string coord_bind_address_ = "0.0.0.0";
    uint16_t coord_port_ = 5000;
    uint32_t rs_k_ = 2, rs_m_ = 1;
    uint16_t http_port_ = 8080;
    std::string auth_token_ = "rfe-demo-token";
    bool auto_recover_ = true;
    bool require_auth_ = true;
    bool adaptive_replicas_ = true;
    size_t hot_replica_count_ = 1;

    std::string staging_dir() const { return "output/staging"; }
    std::string output_dir() const { return "output"; }
};

static void usage() {
    std::cout <<
        "usage: controller <command> [args]\n"
        "  coordinator [port] [bind_ip] [http_port]\n"
        "                               start Control Plane + HTTP Upload API/Web Console\n"
        "  peers [coord_ip] [port]      query Coordinator and display live peer registry\n"
        "  sys start|stop|status        start / stop / list the 3 peer processes\n"
        "  fail <A|B|C|random>          simulate a peer process failure\n"
        "  demo [file]                  full Gate-1 demonstration (default: input/sample_file.bin)\n"
        "  test [T01 .. T13_HYBRID_ALGORITHMS]\n"
        "                               run automated test suite\n"
        "  chunk <file>                 E1: chunk file + SHA-256 + metadata\n"
        "  distribute                   E3: round-robin + replication to peers\n"
        "  ping                         E2: REGISTER handshake with all peers\n"
        "  detect [--all] [--duration-ms N]\n"
        "                               E4: heartbeat monitor (stops at first failure)\n"
        "  recover [--failed <id>] [--simulate-interrupt]\n"
        "                               E5: recover chunks from surviving replicas\n"
        "  reconstruct [original]       E5: order, concatenate, byte-compare\n"
        "  status                       probe and show peer states\n"
        "  reset                        clear metadata, peer storage, output\n";
}

int main(int argc, char** argv) {
    const std::vector<std::string> args = proc::command_line_args(argc, argv);
    if (args.size() < 2) {
        usage();
        return 1;
    }
    const std::string cmd = args[1];
    if (cmd == "help" || cmd == "--help" || cmd == "-h") {
        usage();
        return 0;
    }
    if (!sock_init()) {
        std::cerr << "controller error: socket subsystem initialisation failed\n";
        return 1;
    }
    try {
        std::string root, err;
        if (!proc::enter_project_root(root, err)) {
            std::cerr << "controller error: " << err << "\n";
            return 1;
        }
        Controller ctl;
        const size_t n = args.size();
        if (cmd == "coordinator") {
            uint16_t port = (n >= 3) ? static_cast<uint16_t>(std::stoi(args[2])) : 0;
            std::string bind_ip = (n >= 4) ? args[3] : "";
            int http_p = (n >= 5) ? std::stoi(args[4]) : -1;
            return ctl.cmd_coordinator(port, bind_ip, http_p);
        }
        if (cmd == "peers") {
            std::string coord_ip = (n >= 3) ? args[2] : "";
            uint16_t port = (n >= 4) ? static_cast<uint16_t>(std::stoi(args[3])) : 0;
            return ctl.cmd_peers(coord_ip, port);
        }
        if (cmd == "chunk" && n == 3) return ctl.cmd_chunk(args[2]);
        if (cmd == "distribute") return ctl.cmd_distribute();
        if (cmd == "ping") return ctl.cmd_ping();
        if (cmd == "detect") {
            bool all = false;
            long long duration_ms = 0;
            for (size_t i = 2; i < n; ++i) {
                if (args[i] == "--all") all = true;
                else if (args[i] == "--duration-ms" && i + 1 < n) duration_ms = std::atoll(args[++i].c_str());
            }
            return ctl.cmd_detect(!all, duration_ms);
        }
        if (cmd == "fail" && n == 3) return ctl.cmd_fail(args[2]);
        if (cmd == "sys" && n >= 3) {
            const std::string sub = args[2];
            if (sub == "start")  return ctl.cmd_sys_start();
            if (sub == "stop")   return ctl.cmd_sys_stop();
            if (sub == "status") return ctl.cmd_sys_status();
        }
        if (cmd == "recover") {
            std::string failed;
            bool interrupt = false;
            for (size_t i = 2; i < n; ++i) {
                if (args[i] == "--failed" && i + 1 < n) failed = args[++i];
                else if (args[i] == "--simulate-interrupt") interrupt = true;
            }
            return ctl.cmd_recover(failed, interrupt);
        }
        if (cmd == "reconstruct")
            return ctl.cmd_reconstruct(n > 2 ? args[2] : "");
        if (cmd == "demo" && n <= 3) return ctl.cmd_demo(n == 3 ? args[2] : "");
        if (cmd == "test")
            return ctl.cmd_test(std::vector<std::string>(args.begin() + 2, args.end()));
        if (cmd == "status") return ctl.cmd_status();
        if (cmd == "reset") return ctl.cmd_reset();
        usage();
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "controller error: " << e.what() << "\n";
        return 1;
    }
}
