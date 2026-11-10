// ============================================================================
// failure_detector.hpp - E4: Failure Detection Engine
//
// Heartbeat + timeout (spec §7):
//   heartbeat interval  = 500 ms
//   heartbeat timeout   = 1000 ms
//   failure threshold   = 2 missed heartbeats
//
// Flow: HEARTBEAT -> wait for ACK -> timeout -> missed count ->
//       threshold reached -> peer marked OFFLINE -> notify recovery system.
//
// Heartbeats are the failure-detection mechanism — TCP itself is NOT.
// Target: detection normally <= 2 seconds. The actual detection time is
// MEASURED, never assumed (spec §7).
// ============================================================================
#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "../../models/peer.hpp"

class FailureDetector {
public:
    // Callback fired once when a peer transitions to OFFLINE.
    using FailureCallback = std::function<void(const PeerInfo& peer,
                                               long long detection_ms)>;

    FailureDetector(std::vector<PeerInfo> peers,
                    int interval_ms = 500,
                    int timeout_ms = 1000,
                    int threshold = 2);

    // Probe every peer once; returns current states. Used for one-shot checks.
    std::vector<PeerInfo> probe_once();

    // Continuous monitoring: one watcher thread per peer. `on_failure` is
    // called the moment a peer crosses the missed-heartbeat threshold.
    // Stops after the first failure if `stop_on_first_failure` is set,
    // otherwise keeps monitoring until stop() is called.
    // Blocks until all watchers finish. stop() may be called from any other
    // thread to end the monitoring.
    void run(FailureCallback on_failure, bool stop_on_first_failure = true);
    void stop();

    // Is `peer_id` currently reachable (single heartbeat probe)?
    bool is_alive(const PeerInfo& peer);

private:
    std::vector<PeerInfo> peers_;
    int interval_ms_;
    int timeout_ms_;
    int threshold_;
    std::atomic<bool> running_{false};
    std::vector<std::thread> watchers_;

    void watch(PeerInfo peer, FailureCallback on_failure,
               bool stop_on_first_failure);
};
