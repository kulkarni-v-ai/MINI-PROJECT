// ============================================================================
// failure_detector.cpp - E4: Failure Detection Engine (implementation)
// ============================================================================
#include "failure_detector.hpp"

#include "../communication/tcp_client.hpp"

FailureDetector::FailureDetector(std::vector<PeerInfo> peers,
                                 int interval_ms, int timeout_ms, int threshold)
    : peers_(std::move(peers)),
      interval_ms_(interval_ms),
      timeout_ms_(timeout_ms),
      threshold_(threshold) {}

bool FailureDetector::is_alive(const PeerInfo& peer) {
    TCPClient client(timeout_ms_, timeout_ms_);
    if (!client.connect_to(peer.address, peer.port)) return false;

    json::Json req = json::Json::object();
    req["type"] = proto::HEARTBEAT;
    json::Json resp;
    std::vector<uint8_t> payload;
    proto::RecvStatus st = client.request(req, {}, resp, payload);
    if (st != proto::RecvStatus::OK) return false;
    return resp.contains("type") &&
           resp["type"].as_string() == proto::HEARTBEAT_ACK;
}

std::vector<PeerInfo> FailureDetector::probe_once() {
    std::vector<PeerInfo> out = peers_;
    for (auto& p : out)
        p.state = is_alive(p) ? PeerState::ONLINE : PeerState::OFFLINE;
    return out;
}

void FailureDetector::watch(PeerInfo peer, FailureCallback on_failure,
                            bool stop_on_first_failure) {
    int missed = 0;
    auto first_miss_at = std::chrono::steady_clock::time_point{};

    while (running_) {
        if (is_alive(peer)) {
            missed = 0;
        } else {
            ++missed;
            if (missed == 1) first_miss_at = std::chrono::steady_clock::now();
            if (missed >= threshold_) {
                // Threshold reached -> peer marked OFFLINE -> notify recovery.
                const auto now = std::chrono::steady_clock::now();
                const long long detection_ms =
                    std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - first_miss_at)
                        .count();
                peer.state = PeerState::OFFLINE;
                if (on_failure) on_failure(peer, detection_ms);
                if (stop_on_first_failure) { running_ = false; return; }
                missed = 0;  // keep monitoring; report repeated outages anew
            }
        }
        // Heartbeat interval pacing.
        auto until = std::chrono::steady_clock::now() +
                     std::chrono::milliseconds(interval_ms_);
        while (running_ && std::chrono::steady_clock::now() < until)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

void FailureDetector::run(FailureCallback on_failure,
                          bool stop_on_first_failure) {
    running_ = true;
    watchers_.clear();
    for (const auto& p : peers_)
        watchers_.emplace_back([this, p, on_failure, stop_on_first_failure]() {
            watch(p, on_failure, stop_on_first_failure);
        });
    for (auto& t : watchers_)
        if (t.joinable()) t.join();
    watchers_.clear();
}

void FailureDetector::stop() {
    // Only signals the watchers; run() (which owns the threads) joins them.
    // Joining here as well would race with run()'s own join when stop() is
    // called from another thread.
    running_ = false;
}
