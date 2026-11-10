// ============================================================================
// peer.hpp - Core data model: Peer & Health / Workload (E2/E4)
//
// In Gate-2:
//   - Peer health states: ONLINE, SUSPECT, OFFLINE, RECOVERING
//   - Workload metrics separated from health: available storage, active transfers,
//     and transfer capacity (busy state).
// ============================================================================
#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include "../third_party/mini_json.hpp"

enum class PeerHealth {
    ONLINE,
    SUSPECT,
    OFFLINE,
    RECOVERING,
    UNKNOWN
};

// Gate-1 compatibility alias
using PeerState = PeerHealth;

inline const char* peer_health_str(PeerHealth s) {
    switch (s) {
        case PeerHealth::ONLINE:     return "ONLINE";
        case PeerHealth::SUSPECT:    return "SUSPECT";
        case PeerHealth::OFFLINE:    return "OFFLINE";
        case PeerHealth::RECOVERING: return "RECOVERING";
        default:                     return "UNKNOWN";
    }
}

inline const char* peer_state_str(PeerHealth s) {
    return peer_health_str(s);
}

inline PeerHealth peer_health_from_str(const std::string& str) {
    if (str == "ONLINE")     return PeerHealth::ONLINE;
    if (str == "SUSPECT")    return PeerHealth::SUSPECT;
    if (str == "OFFLINE")    return PeerHealth::OFFLINE;
    if (str == "RECOVERING") return PeerHealth::RECOVERING;
    return PeerHealth::UNKNOWN;
}

// Workload metrics separated from health state
struct PeerWorkload {
    uint64_t available_storage_bytes = 0;
    uint32_t active_transfers = 0;
    uint32_t transfer_capacity = 10;

    bool is_busy() const {
        return active_transfers >= transfer_capacity;
    }

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["available_storage_bytes"] = static_cast<long long>(available_storage_bytes);
        j["active_transfers"] = static_cast<long long>(active_transfers);
        j["transfer_capacity"] = static_cast<long long>(transfer_capacity);
        j["is_busy"] = is_busy();
        return j;
    }

    static PeerWorkload from_json(const json::Json& j) {
        PeerWorkload w;
        if (j.contains("available_storage_bytes"))
            w.available_storage_bytes = static_cast<uint64_t>(j["available_storage_bytes"].as_int());
        if (j.contains("active_transfers"))
            w.active_transfers = static_cast<uint32_t>(j["active_transfers"].as_int());
        if (j.contains("transfer_capacity"))
            w.transfer_capacity = static_cast<uint32_t>(j["transfer_capacity"].as_int());
        return w;
    }
};

struct PeerInfo {
    std::string id;        // "A", "B", "Peer1", etc.
    std::string address;   // "127.0.0.1" or LAN IPv4 e.g. "192.168.1.101"
    uint16_t    port = 0;  // 5001, 5002, 5003
    PeerHealth  state = PeerHealth::UNKNOWN; // Gate-1 compatibility

    PeerWorkload workload;
    uint32_t missed_heartbeats = 0;
    long long last_heartbeat_ms_ago = -1;
    std::chrono::steady_clock::time_point last_heartbeat = std::chrono::steady_clock::now();

    std::string storage_dir() const { return "storage/peer_" + id + "/chunks"; }

    json::Json to_json() const {
        json::Json j = json::Json::object();
        j["id"] = id;
        j["address"] = address;
        j["port"] = static_cast<long long>(port);
        j["health"] = peer_health_str(state);
        j["workload"] = workload.to_json();
        j["missed_heartbeats"] = static_cast<long long>(missed_heartbeats);
        if (last_heartbeat_ms_ago >= 0) {
            j["last_heartbeat_ms_ago"] = last_heartbeat_ms_ago;
        }
        return j;
    }

    static PeerInfo from_json(const json::Json& j) {
        PeerInfo p;
        if (j.contains("id")) p.id = j["id"].as_string();
        if (j.contains("address")) p.address = j["address"].as_string();
        if (j.contains("port")) p.port = static_cast<uint16_t>(j["port"].as_int());
        if (j.contains("health")) p.state = peer_health_from_str(j["health"].as_string());
        if (j.contains("workload")) p.workload = PeerWorkload::from_json(j["workload"]);
        if (j.contains("missed_heartbeats"))
            p.missed_heartbeats = static_cast<uint32_t>(j["missed_heartbeats"].as_int());
        if (j.contains("last_heartbeat_ms_ago"))
            p.last_heartbeat_ms_ago = j["last_heartbeat_ms_ago"].as_int();
        return p;
    }
};
