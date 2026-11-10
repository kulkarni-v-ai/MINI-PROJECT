#!/usr/bin/env bash
# ============================================================================
# run_demo.sh - One-Command Live Cluster & Web Visualizer Launcher (Linux/macOS)
# Usage: ./run_demo.sh [peer_count=4] [tcp_port=5000] [http_port=8080]
# ============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

PEERS="${1:-4}"
TCP_PORT="${2:-5000}"
HTTP_PORT="${3:-8080}"
BIN_DIR="$ROOT/build"

if [[ ! -x "$BIN_DIR/controller" || ! -x "$BIN_DIR/peer" ]]; then
  echo "Compiled binaries not found in ./build. Build first:"
  echo "  cmake -S . -B build && cmake --build build -j"
  exit 1
fi

PIDS=()
cleanup() {
  echo ""
  echo "Stopping Coordinator and Peer processes..."
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  echo "Stopped."
}
trap cleanup EXIT INT TERM

"$BIN_DIR/controller" coordinator "$TCP_PORT" 0.0.0.0 "$HTTP_PORT" &
PIDS+=($!)
sleep 0.6

NAMES=(PEER_A PEER_B PEER_C PEER_D PEER_E PEER_F PEER_G PEER_H)
for ((i=0; i<PEERS; i++)); do
  NAME="${NAMES[$i]}"
  PORT=$((TCP_PORT + 1 + i))
  "$BIN_DIR/peer" "$NAME" 127.0.0.1 "$PORT" 127.0.0.1 "$TCP_PORT" &
  PIDS+=($!)
done

sleep 1.2
echo "=================================================================="
echo "PowerShell Web Monitor : http://127.0.0.1:$HTTP_PORT/ (Token: rfe-demo-token)"
echo "CLI Connect            : ./build/rfe connect 127.0.0.1:$TCP_PORT --token rfe-demo-token"
echo "Press Ctrl+C to stop the cluster cleanly."
echo "=================================================================="
wait
