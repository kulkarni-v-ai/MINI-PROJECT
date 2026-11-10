// ============================================================================
// distributor.cpp - E3: Distribution & Replication Engine (implementation)
// ============================================================================
#include "distributor.hpp"

#include <fstream>

#include "../chunking/chunk_manager.hpp"
#include "../communication/tcp_client.hpp"

Distributor::Distributor(std::vector<PeerInfo> peers)
    : peers_(std::move(peers)) {}

std::pair<std::string, std::string> Distributor::placement_for(size_t index) const {
    // Round-robin: C1->A+B, C2->B+C, C3->C+A, ... (spec §6).
    const size_t n = peers_.size();
    const size_t primary = index % n;
    const size_t replica = (primary + 1) % n;
    return {peers_[primary].id, peers_[replica].id};
}

bool Distributor::store_chunk(const PeerInfo& peer, const ChunkMeta& chunk,
                              const std::string& staged_path,
                              std::string& error_out) {
    std::ifstream in(staged_path, std::ios::binary);
    if (!in) {
        error_out = "missing staged chunk: " + staged_path;
        return false;
    }
    std::vector<uint8_t> payload{std::istreambuf_iterator<char>(in),
                                 std::istreambuf_iterator<char>()};

    TCPClient client;
    if (!client.connect_to(peer.address, peer.port)) {
        error_out = "cannot connect to peer " + peer.id;
        return false;
    }
    json::Json req = json::Json::object();
    req["type"] = proto::STORE_CHUNK;
    req["chunk_id"] = chunk.chunk_id;
    req["sha256"] = chunk.sha256;  // peer verifies integrity on store

    json::Json resp;
    std::vector<uint8_t> resp_payload;
    if (client.request(req, payload, resp, resp_payload) != proto::RecvStatus::OK ||
        resp["type"].as_string() != proto::STORE_ACK) {
        error_out = "peer " + peer.id + " rejected chunk " + chunk.chunk_id;
        return false;
    }
    return true;
}

bool Distributor::distribute(const std::string& staging_dir,
                             std::vector<ChunkMeta>& chunks,
                             std::string& error_out) {
    for (size_t i = 0; i < chunks.size(); ++i) {
        auto [primary, replica] = placement_for(i);
        chunks[i].peers = {primary, replica};

        // Metadata records BOTH locations (spec §6).
        for (const std::string& pid : chunks[i].peers) {
            const PeerInfo* target = nullptr;
            for (const auto& p : peers_)
                if (p.id == pid) target = &p;
            if (!target) {
                error_out = "unknown peer " + pid;
                return false;
            }
            const std::string staged =
                staging_dir + "/" + chunks[i].chunk_id + ".chunk";
            if (!store_chunk(*target, chunks[i], staged, error_out)) return false;
        }
    }
    return true;
}
