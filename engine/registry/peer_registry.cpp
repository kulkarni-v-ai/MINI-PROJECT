// ============================================================================
// peer_registry.cpp - Coordinator Dynamic Peer Registry implementation
// ============================================================================
#include "peer_registry.hpp"

#include <iomanip>
#include <sstream>

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
}  // namespace

bool PeerRegistry::register_peer(const PeerInfo& info, std::string& message_out, PeerHealth* prev_state_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    auto it = peers_.find(info.id);
    if (it != peers_.end()) {
        const PeerHealth prev_health = it->second.state;
        if (prev_state_out) *prev_state_out = prev_health;
        it->second.address = info.address;
        it->second.port = info.port;
        it->second.workload = info.workload;
        it->second.state = PeerHealth::ONLINE;
        it->second.missed_heartbeats = 0;
        it->second.last_heartbeat = now;
        it->second.last_heartbeat_ms_ago = -1;
        message_out = "Peer " + info.id + " re-registered at " + info.address + ":" +
                      std::to_string(info.port) + " (was " + peer_health_str(prev_health) + ")";
        return true;
    }

    if (prev_state_out) *prev_state_out = PeerHealth::UNKNOWN;
    PeerInfo p = info;
    p.state = PeerHealth::ONLINE;
    p.missed_heartbeats = 0;
    p.last_heartbeat = now;
    p.last_heartbeat_ms_ago = -1;
    peers_[info.id] = p;
    message_out = "Peer " + info.id + " registered at " + info.address + ":" +
                  std::to_string(info.port) + " (storage=" + format_bytes(p.workload.available_storage_bytes) + ")";
    return true;
}

void PeerRegistry::set_peer_snapshot(const PeerInfo& info) {
    std::lock_guard<std::mutex> lock(mutex_);
    peers_[info.id] = info;
}

bool PeerRegistry::record_heartbeat(const std::string& peer_id, const PeerWorkload& workload,
                                    PeerHealth* prev_state_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = peers_.find(peer_id);
    if (it == peers_.end()) return false;

    if (prev_state_out) *prev_state_out = it->second.state;
    it->second.last_heartbeat = std::chrono::steady_clock::now();
    it->second.last_heartbeat_ms_ago = -1;
    it->second.missed_heartbeats = 0;
    it->second.workload = workload;
    if (it->second.state == PeerHealth::SUSPECT || it->second.state == PeerHealth::OFFLINE) {
        it->second.state = PeerHealth::ONLINE;
    }
    return true;
}

std::vector<PeerTransition> PeerRegistry::evaluate_health_detailed(int timeout_ms, int failure_threshold) {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PeerTransition> transitions;
    const auto now = std::chrono::steady_clock::now();

    for (auto& kv : peers_) {
        PeerInfo& p = kv.second;
        if (p.state == PeerHealth::OFFLINE) {
            continue; // Already confirmed offline; remains offline until heartbeat arrives
        }

        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - p.last_heartbeat).count();
        if (elapsed >= static_cast<long long>(timeout_ms * failure_threshold)) {
            PeerHealth prev = p.state;
            p.missed_heartbeats = failure_threshold;
            p.state = PeerHealth::OFFLINE;
            std::string msg = "Peer " + p.id + " missed " + std::to_string(failure_threshold) +
                              " heartbeats (" + std::to_string(elapsed) + " ms) -> OFFLINE";
            transitions.push_back({p.id, prev, PeerHealth::OFFLINE, msg});
        } else if (elapsed >= static_cast<long long>(timeout_ms)) {
            p.missed_heartbeats = 1;
            if (p.state != PeerHealth::SUSPECT) {
                PeerHealth prev = p.state;
                p.state = PeerHealth::SUSPECT;
                std::string msg = "Peer " + p.id + " missed 1 heartbeat (" +
                                  std::to_string(elapsed) + " ms) -> SUSPECT";
                transitions.push_back({p.id, prev, PeerHealth::SUSPECT, msg});
            }
        }
    }
    return transitions;
}

std::vector<std::string> PeerRegistry::evaluate_health(int timeout_ms, int failure_threshold) {
    auto detailed = evaluate_health_detailed(timeout_ms, failure_threshold);
    std::vector<std::string> out;
    out.reserve(detailed.size());
    for (const auto& t : detailed) out.push_back(t.message);
    return out;
}

bool PeerRegistry::get_peer(const std::string& peer_id, PeerInfo& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = peers_.find(peer_id);
    if (it == peers_.end()) return false;
    out = it->second;
    return true;
}

std::vector<PeerInfo> PeerRegistry::get_all_peers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PeerInfo> list;
    list.reserve(peers_.size());
    for (const auto& kv : peers_) list.push_back(kv.second);
    return list;
}

std::vector<PeerInfo> PeerRegistry::get_online_peers() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<PeerInfo> list;
    for (const auto& kv : peers_) {
        if (kv.second.state == PeerHealth::ONLINE) list.push_back(kv.second);
    }
    return list;
}

size_t PeerRegistry::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return peers_.size();
}

std::string PeerRegistry::format_table() const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    std::ostringstream ss;
    ss << "=================================== LIVE PEER REGISTRY ===================================\n";
    ss << std::left
       << std::setw(8)  << "PEER ID"
       << std::setw(22) << "ADDRESS:PORT"
       << std::setw(12) << "HEALTH"
       << std::setw(18) << "AVAIL STORAGE"
       << std::setw(14) << "TRANSFERS"
       << std::setw(8)  << "BUSY?"
       << "LAST HEARTBEAT\n";
    ss << "------------------------------------------------------------------------------------------\n";

    if (peers_.empty()) {
        ss << "  (no peers registered yet)\n";
    } else {
        for (const auto& kv : peers_) {
            const PeerInfo& p = kv.second;
            const long long elapsed_ms = (p.last_heartbeat_ms_ago >= 0)
                ? p.last_heartbeat_ms_ago
                : std::chrono::duration_cast<std::chrono::milliseconds>(now - p.last_heartbeat).count();
            std::string ep = p.address + ":" + std::to_string(p.port);
            std::string transfers = std::to_string(p.workload.active_transfers) + " / " +
                                    std::to_string(p.workload.transfer_capacity);
            std::string health_col = peer_health_str(p.state);
            if (p.state == PeerHealth::ONLINE && p.workload.is_busy()) {
                health_col = "BUSY";
            }
            std::string last_hb = (p.state == PeerHealth::OFFLINE)
                                      ? "OFFLINE (" + std::to_string(elapsed_ms) + " ms ago)"
                                      : std::to_string(elapsed_ms) + " ms ago";

            ss << std::left
               << std::setw(8)  << p.id
               << std::setw(22) << ep
               << std::setw(12) << health_col
               << std::setw(18) << format_bytes(p.workload.available_storage_bytes)
               << std::setw(14) << transfers
               << std::setw(8)  << (p.workload.is_busy() ? "YES" : "NO")
               << last_hb << "\n";
        }
    }
    ss << "==========================================================================================\n";
    return ss.str();
}

json::Json PeerRegistry::to_json() const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = std::chrono::steady_clock::now();
    json::Json arr = json::Json::array();
    for (const auto& kv : peers_) {
        PeerInfo copy = kv.second;
        if (copy.last_heartbeat_ms_ago < 0) {
            copy.last_heartbeat_ms_ago = std::chrono::duration_cast<std::chrono::milliseconds>(
                now - copy.last_heartbeat).count();
        }
        arr.push_back(copy.to_json());
    }
    return arr;
}
